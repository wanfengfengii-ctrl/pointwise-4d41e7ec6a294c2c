import json
from dataclasses import dataclass, asdict
from hashlib import sha256
from types import SimpleNamespace
from telecapture.measurements.schema import ArchiveError, validate_schema
from telecapture.delivery.queues import make_delivery_key

@dataclass(frozen=True)
class MeasurementView:
    delivery_key: str
    recording_id: str
    gateway_id: str
    session_id: str
    stream_id: int
    logical_mid: int
    raw_mid: int
    row_number: int
    sensor_id: str
    time_us: int
    quantity: str
    unit: str
    value_micro: object
    quality: str
    body: bytes
    unordered: bool = False

    def as_dict(self):
        result = asdict(self)
        result['body'] = self.body.decode('utf-8')
        return result

def validate_filters(gateway_id=None, stream_id=None, start_us=None, end_us=None):
    if gateway_id is not None and (not isinstance(gateway_id, str) or not gateway_id.strip()):
        raise ArchiveError('query', 'invalid gateway filter')
    if stream_id is not None and (type(stream_id) is not int or not 0 <= stream_id <= 65535):
        raise ArchiveError('query', 'invalid stream filter')
    for value in (start_us, end_us):
        if value is not None and type(value) is not int:
            raise ArchiveError('query', 'time bounds must be integers')
    if start_us is not None and end_us is not None and start_us > end_us:
        raise ArchiveError('query', 'reversed time range')

def key_unordered(key):
    parts = json.loads(key)
    if isinstance(parts, list) and len(parts) == 5:
        return False
    if isinstance(parts, list) and len(parts) == 6 and parts[4] == 'unordered':
        return True
    raise ValueError('delivery key outside supported namespaces')

def verified_messages(connection):
    from telecapture.measurements.codec import decode_message, storage_row
    from telecapture.recording.header import parse_header, canonical_header
    validate_schema(connection)
    headers = {}
    for key, canonical in connection.execute('SELECT registration_key, canonical_header FROM recordings'):
        try:
            header = parse_header(json.loads(canonical))
            identity = json.dumps([header.recording_id, header.gateway_id, header.session_id],
                                  separators=(',', ':'))
            if identity != key or canonical_header(header) != canonical:
                raise ValueError('header identity mismatch')
            headers[key] = header
        except (ValueError, TypeError, KeyError) as exc:
            raise ArchiveError('schema', 'corrupt registered header') from exc
    result = []
    for record in connection.execute('SELECT * FROM messages'):
        key, registration, recording, gateway, session, stream, logical, raw, body, digest, provenance = record
        try:
            logical = int(logical)
            unordered = key_unordered(key)
            header = headers[registration]
            if (recording, gateway, session) != (header.recording_id, header.gateway_id, header.session_id):
                raise ValueError('message/header identity mismatch')
            streams = dict(header.streams)
            if stream not in streams or raw != logical % 2**32:
                raise ValueError('message serial identity mismatch')
            if unordered:
                initial = dict(header.unordered_streams).get(stream, 0)
            else:
                initial = streams[stream]
            if logical < initial:
                raise ValueError('message identity below mode initial')
            if key != make_delivery_key(recording, gateway, session, stream, logical, unordered):
                raise ValueError('delivery identity mismatch')
            if type(body) is not bytes or sha256(body).hexdigest() != digest:
                raise ValueError('message digest mismatch')
            source = json.loads(provenance)
            if not isinstance(source, list):
                raise ValueError('invalid provenance')
            decoded = decode_message(SimpleNamespace(body=body))
            expected = [storage_row(row) for row in decoded.measurements]
            actual = connection.execute(
                'SELECT row_number,sensor_id,time_us,quantity,unit,value_micro,quality FROM measurements WHERE delivery_key=? ORDER BY row_number',
                (key,)).fetchall()
            if actual != expected:
                raise ValueError('measurement/evidence mismatch')
        except (ValueError, TypeError, KeyError) as exc:
            raise ArchiveError('schema', f'corrupt archive evidence: {exc}') from exc
        result.append((key, recording, gateway, session, stream, unordered, logical, raw,
                       body, source, decoded.measurements))
    return result

def read(connection, gateway_id=None, stream_id=None, start_us=None, end_us=None):
    validate_filters(gateway_id, stream_id, start_us, end_us)
    rows = []
    for key, recording, gateway, session, stream, unordered, logical, raw, body, source, measurements in verified_messages(connection):
        if gateway_id is not None and gateway_id != gateway:
            continue
        if stream_id is not None and stream_id != stream:
            continue
        for item in measurements:
            if start_us is not None and item.time_us < start_us:
                continue
            if end_us is not None and item.time_us >= end_us:
                continue
            rows.append(MeasurementView(key, recording, gateway, session, stream, logical, raw,
                        item.row_number, item.sensor_id, item.time_us, item.quantity, item.unit,
                        item.value_micro, item.quality, body, unordered))
    return sorted(rows, key=lambda r: (r.time_us, r.gateway_id, r.session_id, r.stream_id,
                                       r.unordered, r.logical_mid, r.row_number, r.recording_id))
