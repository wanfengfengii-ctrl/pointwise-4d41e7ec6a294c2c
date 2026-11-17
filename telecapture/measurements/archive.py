import json
import sqlite3
from hashlib import sha256
from telecapture.measurements.schema import ArchiveError, initialize_archive, validate_schema
from telecapture.measurements.codec import decode_batch, storage_row
from telecapture.measurements.query import read, verified_messages
from telecapture.association.context import make_context
from telecapture.association.registry import assert_same_registration
from telecapture.recording.header import parse_header

def registration_key(context):
    return json.dumps(list(context.registration_key), separators=(',', ':'))

def provenance_object(message):
    return [dict(tsn=p.tsn, fsn=p.fsn, path=p.evidence.location.path,
                 line=p.evidence.location.line, digest=p.evidence.digest) for p in message.fragments]

class Archive:
    def __init__(self, path=':memory:', fault=None):
        self.path = str(path)
        self.fault = fault
        self.closed = False
        self.connection = None
        try:
            self.connection = sqlite3.connect(self.path, isolation_level=None)
            self.connection.execute('PRAGMA foreign_keys=ON')
            initialize_archive(self.connection)
            verified_messages(self.connection)
        except (sqlite3.Error, ArchiveError) as exc:
            if self.connection is not None:
                self.connection.close()
            if isinstance(exc, ArchiveError):
                raise
            raise ArchiveError('storage', str(exc)) from exc

    def _check(self):
        if self.closed:
            raise ArchiveError('storage', 'archive is closed')
        validate_schema(self.connection)

    def register_recording(self, header):
        context = make_context(header)
        key = registration_key(context)
        try:
            self._check()
            self.connection.execute('BEGIN IMMEDIATE')
            row = self.connection.execute('SELECT canonical_header FROM recordings WHERE registration_key=?', (key,)).fetchone()
            if row is None:
                self.connection.execute('INSERT INTO recordings VALUES (?,?)', (key, context.canonical))
            else:
                assert_same_registration(row[0], context.canonical)
            self.connection.execute('COMMIT')
        except BaseException as exc:
            if self.connection.in_transaction:
                self.connection.execute('ROLLBACK')
            if isinstance(exc, sqlite3.Error):
                raise ArchiveError('storage', str(exc)) from exc
            raise
        return context

    def _fault(self, stage):
        if self.fault is not None:
            self.fault(stage)

    def commit(self, messages):
        decoded = decode_batch(tuple(messages))
        count = 0
        try:
            self._check()
            self.connection.execute('BEGIN IMMEDIATE')
            verified_messages(self.connection)
            for item in decoded:
                message = item.message
                context_key = json.dumps([message.recording_id, message.gateway_id, message.session_id],
                                         separators=(',', ':'))
                header_row = self.connection.execute(
                    'SELECT canonical_header FROM recordings WHERE registration_key=?', (context_key,)).fetchone()
                if header_row is None:
                    raise ArchiveError('identity_conflict', 'recording must be registered first')
                header = parse_header(json.loads(header_row[0]))
                unordered_initials = dict(header.unordered_streams)
                initials = {}
                for stream, mid in header.streams:
                    initials[(stream, False)] = mid
                    initials[(stream, True)] = unordered_initials.get(stream, 0)
                initial = initials.get((message.stream_id, message.unordered))
                if (initial is None or
                    message.logical_mid < initial or
                    message.raw_mid != message.logical_mid % 2**32):
                    raise ArchiveError('identity_conflict', 'delivery is outside registered stream')
                previous = self.connection.execute(
                    'SELECT body FROM messages WHERE delivery_key=?', (message.key,)).fetchone()
                if previous is not None:
                    if previous[0] != message.body:
                        raise ArchiveError('identity_conflict', 'same delivery identity has different body')
                    continue
                self.connection.execute('INSERT INTO messages VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                    (message.key, context_key, message.recording_id, message.gateway_id,
                     message.session_id, message.stream_id, str(message.logical_mid), message.raw_mid,
                     message.body, sha256(message.body).hexdigest(),
                     json.dumps(provenance_object(message), sort_keys=True, separators=(',', ':'))))
                self._fault('after_evidence')
                for row in item.measurements:
                    self.connection.execute('INSERT INTO measurements VALUES (?,?,?,?,?,?,?,?)',
                                            (message.key,) + storage_row(row))
                    self._fault('after_measurement')
                count += 1
            self._fault('before_commit')
            self.connection.execute('COMMIT')
            return count
        except BaseException as exc:
            if self.connection.in_transaction:
                self.connection.execute('ROLLBACK')
            if isinstance(exc, (ArchiveError, KeyboardInterrupt, SystemExit)):
                raise
            raise ArchiveError('storage', str(exc)) from exc

    def read(self, **filters):
        try:
            self._check()
            return read(self.connection, **filters)
        except sqlite3.Error as exc:
            raise ArchiveError('storage', str(exc)) from exc

    def evidence(self):
        try:
            self._check()
            return [dict(delivery_key=x[0], recording_id=x[1], gateway_id=x[2], session_id=x[3],
                         stream_id=x[4], unordered=x[5], logical_mid=x[6], raw_mid=x[7],
                         body=x[8], provenance=x[9])
                    for x in sorted(verified_messages(self.connection), key=lambda x: x[0])]
        except sqlite3.Error as exc:
            raise ArchiveError('storage', str(exc)) from exc

    def close(self):
        if not self.closed:
            self.closed = True
            self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
