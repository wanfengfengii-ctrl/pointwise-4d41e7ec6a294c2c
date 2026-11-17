import base64
import json
import tempfile
import subprocess
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from telecapture.recording.header import parse_header, header_object
from telecapture.recording.events import IDataEvent, ForwardEvent, ForwardEntry, decode_event
from telecapture.recording.provenance import RecordingError, make_evidence
from telecapture.recording.reader import open_recording
from telecapture.association.lifecycle import open_session
from telecapture.association.registry import RecordingRegistry
from telecapture.association.context import AssociationError
from telecapture.transport.receipts import CaptureConflict
from telecapture.measurements.archive import Archive
from telecapture.measurements.ingest import import_recording
from telecapture.measurements.schema import ArchiveError

def header(recording='r', gateway='g', session='s', tsn=99, streams=((7, 0), (8, 0))):
    return parse_header(dict(version=1, recording_id=recording, gateway_id=gateway,
        session_id=session, initial_cumulative_tsn=tsn,
        streams=[dict(stream_id=s, initial_mid=m) for s, m in streams]))

def data(tsn, mid, body, stream=7, fsn=0, begin=True, end=True, session='s'):
    return IDataEvent(session, tsn % 2**32, stream, mid % 2**32, fsn, begin, end, body)

def forward(tsn, mid, stream=7, session='s'):
    return ForwardEvent(session, tsn % 2**32, (ForwardEntry(stream, mid % 2**32),))

def payload(value=21, unit='mC', quantity='temperature', quality='good', time=100, sensor='sensor'):
    row = dict(sensor_id=sensor, time_us=time, quantity=quantity, unit=unit, value=value, quality=quality)
    return json.dumps(dict(version=1, measurements=[row]), sort_keys=True, separators=(',', ':')).encode()

def event_object(event):
    if isinstance(event, IDataEvent):
        return dict(type='I-DATA', session_id=event.session_id, tsn=event.tsn,
            stream_id=event.stream_id, mid=event.mid, fsn=event.fsn,
            begin=event.begin, end=event.end, payload_b64=base64.b64encode(event.payload).decode())
    return dict(type='I-FORWARD-TSN', session_id=event.session_id, cumulative_tsn=event.cumulative_tsn,
                entries=[dict(stream_id=e.stream_id, mid=e.mid) for e in event.entries])

def write_recording(path, h, events):
    lines = [json.dumps(header_object(h))] + [json.dumps(event_object(event)) for event in events]
    Path(path).write_text('\n'.join(lines) + '\n', encoding='utf-8')

def delivered(h, events):
    session = open_session(h)
    result = []
    for event in events:
        session.push(event)
        result.extend(session.drain())
    return result

class TestCapture(unittest.TestCase):
    def test_normal_measurements(self):
        h = header()
        large = 2**60 + 3
        first = payload(-large)
        missing = payload(None, unit='uPa', quantity='pressure', quality='missing', sensor='p')
        suspect = payload(large, unit='Pa', quantity='pressure', quality='suspect', time=101)
        session = open_session(h)
        with Archive() as archive:
            archive.register_recording(h)
            session.push(data(100, 0, first[:19], end=False))
            self.assertEqual(session.drain(), [])
            session.push(data(102, 0, missing, stream=8))
            archive.commit(session.drain())
            session.push(data(101, 0, first[19:], fsn=1, begin=False))
            archive.commit(session.drain())
            session.push(data(103, 1, suspect))
            archive.commit(session.drain())
            rows = archive.read()
            self.assertEqual([(r.stream_id, r.value_micro, r.quality, r.unit) for r in rows],
                [(7, -large * 1000, 'good', 'uC'), (8, None, 'missing', 'uPa'),
                 (7, large * 1000000, 'suspect', 'uPa')])
            self.assertEqual(rows[0].body, first)
            self.assertEqual(rows[1].body, missing)
            self.assertEqual(archive.read(start_us=100, end_us=101), rows[:2])
            self.assertEqual(archive.read(gateway_id='g', stream_id=8), [rows[1]])
            self.assertEqual(archive.read(start_us=101, end_us=101), [])
            with self.assertRaises(ArchiveError):
                archive.read(start_us=102, end_us=101)
            with self.assertRaises(ArchiveError):
                archive.read(stream_id=True)
            empty = b'{"version":1,"measurements":[]}'
            session.push(data(104, 2, empty))
            archive.commit(session.drain())
            self.assertEqual(len(archive.evidence()), 4)
            self.assertEqual(len(archive.read()), 3)
        # Independent exact conversion checks, including equivalent units.
        for quantity, aunit, bunit, factor in (
            ('temperature', 'mC', 'uC', 1000), ('pressure', 'Pa', 'uPa', 1000000)):
            with Archive() as archive:
                archive.register_recording(h)
                messages = delivered(h, [data(100, 0, payload(-large, aunit, quantity)),
                                         data(101, 1, payload(-large * factor, bunit, quantity))])
                archive.commit(messages)
                self.assertEqual([r.value_micro for r in archive.read()], [-large * factor] * 2)

    def test_tail_before_head(self):
        session = open_session(header())
        tail = data(102, 0, b'C', fsn=2, begin=False)
        session.push(tail)
        session.push(data(100, 0, b'A', end=False))
        session.push(tail)
        self.assertEqual(session.drain(), [])
        before = session.receiver.snapshot()
        for bad in (data(104, 0, b'D', fsn=3, begin=False),
                    data(105, 0, b'X', fsn=2, begin=False),
                    data(106, 0, b'B', fsn=1, begin=False, end=True),
                    data(107, 0, b'B', fsn=1, begin=True, end=False)):
            with self.assertRaises(CaptureConflict):
                session.push(bad)
            self.assertEqual(session.receiver.snapshot(), before)
        session.push(data(101, 0, b'B', fsn=1, begin=False, end=False))
        self.assertEqual([m.body for m in session.drain()], [b'ABC'])
        self.assertEqual(session.drain(), [])
        session.push(tail)
        self.assertEqual(session.drain(), [])
        self.assertEqual(session.finish_input(), ())
        with self.assertRaises(AssociationError):
            session.push(tail)
        incomplete = open_session(header())
        incomplete.push(data(100, 0, b'A', end=False))
        self.assertEqual(incomplete.finish_input(), ((7, 0),))
        self.assertEqual(incomplete.drain(), [])

    def test_bad_input_and_association(self):
        h = header()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'bad.jsonl'
            for final in ('{"type":', json.dumps(dict(type='I-DATA', session_id='s',
                    tsn=101, stream_id=7, mid=1, fsn=0, begin=True, end=True, payload_b64='!!')),
                    '{"type":"I-DATA","type":"I-FORWARD-TSN"}'):
                write_recording(path, h, [data(100, 0, payload())])
                with path.open('a', encoding='utf-8') as stream:
                    stream.write(final)
                with open_recording(path) as recording:
                    iterator = recording.events()
                    next(iterator)
                    with self.assertRaises(RecordingError) as raised:
                        next(iterator)
                    self.assertEqual(raised.exception.location.line, 3)
                    self.assertTrue(recording.closed)
                result = subprocess.run([sys.executable, '-B', '-m', 'telecapture.measurements.ingest',
                    '--db', str(Path(directory) / 'cli.sqlite'), 'import', str(path)],
                    cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True)
                self.assertEqual(result.returncode, 2, result.stderr)
                diagnostic = json.loads(result.stderr)
                self.assertEqual(diagnostic['error'], 'RecordingError')
                self.assertIn(':3:', diagnostic['message'])
                with Archive(Path(directory) / 'cli.sqlite') as archive:
                    self.assertEqual([r.value_micro for r in archive.read()], [21000])
            path.write_bytes(json.dumps(header_object(h)).encode() + bytes([10,255]))
            with open_recording(path) as recording:
                with self.assertRaises(RecordingError) as raised:
                    list(recording.events())
                self.assertEqual(raised.exception.location.line, 2)
        for changed in ({'tsn': True}, {'fsn': -1}, {'begin': 1}, {'unordered': True}):
            value = event_object(data(100, 0, b'A'))
            value.update(changed)
            with self.assertRaises(RecordingError):
                decode_event(value)
        registry = RecordingRegistry()
        session = open_session(h, registry)
        session.push(data(100, 0, b'A', end=False))
        before = session.receiver.snapshot()
        bads = [data(101, 0, b'B', fsn=1, begin=False, session='other'),
                data(101, 0, b'B', stream=9),
                ForwardEvent('s', 102, (ForwardEntry(7, 0), ForwardEntry(9, 0)))]
        for event in bads:
            with self.assertRaises(AssociationError):
                session.push(event)
            self.assertEqual(session.receiver.snapshot(), before)
        with self.assertRaises(AssociationError):
            open_session(header(tsn=98), registry)
        self.assertEqual(registry.register(h), session.context)
        session.push(data(101, 0, b'B', fsn=1, begin=False))
        self.assertEqual([m.body for m in session.drain()], [b'AB'])
