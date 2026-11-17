import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from tests.test_capture import header, data, forward, payload, write_recording, delivered
from telecapture.recording.provenance import make_evidence
from telecapture.association.lifecycle import open_session
from telecapture.association.context import AssociationError
from telecapture.transport.receipts import CaptureConflict
from telecapture.measurements.archive import Archive
from telecapture.measurements.ingest import import_recording

class TestReplay(unittest.TestCase):
    def test_repeat_and_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'archive.sqlite'
            path = Path(directory) / 'recording.jsonl'
            identities = [header(), header(recording='r2'), header(gateway='g2'),
                          header(session='s2'), header(recording='r3', streams=((9, 0),))]
            expected = []
            for index, h in enumerate(identities):
                stream = h.streams[0][0]
                body = payload(index + 1)
                event = data(100, 0, body, stream=stream, session=h.session_id)
                write_recording(path, h, [event])
                with Archive(database) as archive:
                    first = import_recording(path, archive)
                    second = import_recording(path, archive)
                    self.assertEqual((first.inserted, second.inserted), (1, 0))
                    self.assertEqual(second.delivered, 1)
                expected.append((h.recording_id, h.gateway_id, h.session_id, stream, body))
            with Archive(database) as archive:
                self.assertEqual(len(archive.read()), 5)
                actual = {(r.recording_id, r.gateway_id, r.session_id, r.stream_id, r.body)
                          for r in archive.read()}
                self.assertEqual(actual, set(expected))
                before = archive.evidence()
                empty = header(recording='empty')
                write_recording(path, empty, [])
                report = import_recording(path, archive)
                self.assertEqual((report.events, report.delivered, report.inserted), (0, 0, 0))
                self.assertEqual(archive.evidence(), before)
                archive.register_recording(header())
                with self.assertRaises(AssociationError):
                    archive.register_recording(header(tsn=98))
                self.assertEqual(archive.evidence(), before)
                # Same timestamps, distinct identities: the public stable ordering is reproducible.
                rows = archive.read(start_us=100, end_us=101)
                self.assertEqual([(r.gateway_id, r.session_id, r.stream_id, r.recording_id) for r in rows],
                    [('g','s',7,'r'), ('g','s',7,'r2'), ('g','s',9,'r3'),
                     ('g','s2',7,'r'), ('g2','s',7,'r')])

    def test_conflict_atomicity(self):
        session = open_session(header())
        first = data(100, 0, b'A', end=False)
        session.push(first)
        before = session.receiver.snapshot()
        changes = [dict(payload=b'X'), dict(mid=1), dict(fsn=1), dict(begin=False), dict(end=True)]
        for change in changes:
            with self.assertRaises(CaptureConflict):
                session.push(replace(first, **change))
            self.assertEqual(session.receiver.snapshot(), before)
        for event in (replace(first, tsn=105, payload=b'X'),
                      data(106, 0, b'Q', fsn=2, begin=False, end=True)):
            if event.tsn == 106:
                session.push(event)
                after_tail = session.receiver.snapshot()
                with self.assertRaises(CaptureConflict):
                    session.push(data(107, 0, b'Y', fsn=1, begin=False, end=True))
                self.assertEqual(session.receiver.snapshot(), after_tail)
            else:
                with self.assertRaises(CaptureConflict):
                    session.push(event)
                self.assertEqual(session.receiver.snapshot(), before)
        # Different source position is not part of the duplicate identity.
        current = session.receiver.snapshot()
        session.push(replace(first, evidence=make_evidence('other.jsonl', 90, b'raw')))
        self.assertEqual(session.receiver.snapshot(), current)
        session.push(data(101, 0, b'B', fsn=1, begin=False, end=False))
        self.assertEqual([m.body for m in session.drain()], [b'ABQ'])
        current = session.receiver.snapshot()
        with self.assertRaises(CaptureConflict):
            session.push(replace(first, payload=b'bad'))
        self.assertEqual(session.receiver.snapshot(), current)
        session.push(data(108, 1, b'C'))
        self.assertEqual([m.body for m in session.drain()], [b'C'])
        with Archive() as archive:
            h = header(recording='measurement')
            archive.register_recording(h)
            batch = delivered(h, [data(100, 0, payload(3))])
            archive.commit(batch)
            archive.commit(batch)
            self.assertEqual([r.value_micro for r in archive.read()], [3000])
