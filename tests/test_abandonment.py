import itertools
import json
import unittest
from tests.test_capture import header, data, forward, payload
from telecapture.association.lifecycle import open_session
from telecapture.recording.events import IDataEvent
from telecapture.measurements.archive import Archive
from telecapture.transport.serial import compare32, unwrap32, next32
from telecapture.transport.receipts import CaptureConflict

class TestAbandonment(unittest.TestCase):
    def test_keep_complete_successor(self):
        session = open_session(header())
        session.push(data(100, 0, b'A', end=False))
        self.assertEqual(session.drain(), [])
        session.push(data(102, 1, b'C'))
        self.assertEqual(session.drain(), [])
        session.push(data(103, 0, b'X', stream=8))
        self.assertEqual([(m.stream_id, m.body) for m in session.drain()], [(8, b'X')])
        session.push(data(104, 2, b'D'))
        self.assertEqual(session.drain(), [])
        session.push(forward(102, 0))
        actual = [(m.raw_mid, m.body.decode()) for m in session.drain()]
        self.assertEqual(actual, [(1, 'C'), (2, 'D')])
        for event in (data(101, 0, b'B', fsn=1, begin=False),
                      data(100, 0, b'A', end=False), forward(102, 0)):
            session.push(event)
            self.assertEqual(session.drain(), [])
        # Ready is distinct from delivered: source abandonment can retire an undrained candidate.
        waiting = open_session(header())
        waiting.push(data(100, 0, b'A'))
        waiting.push(forward(100, 0))
        self.assertEqual(waiting.drain(), [])
        body0, body1 = payload(12), payload(25)
        session = open_session(header())
        with Archive() as archive:
            archive.register_recording(header())
            batches = []
            for event in (data(100, 0, body0[:20], end=False),
                          data(102, 1, body1), forward(102, 0),
                          data(101, 0, body0[20:], fsn=1, begin=False)):
                session.push(event)
                batch = session.drain()
                batches.append([m.raw_mid for m in batch])
                archive.commit(batch)
            self.assertEqual(batches, [[], [], [1], []])
            self.assertEqual([(r.logical_mid, r.value_micro, r.body) for r in archive.read()],
                             [(1, 25000, body1)])
            print('POINTWISE_SCENARIO ' + json.dumps(dict(
                case_id='v3.push_visibility', input=['A(head)', 'C(complete)', 'forward(102,7/0)', 'B(late)'],
                expected=[[], [], [1], []], actual=batches,
                downstream_expected=[25000], downstream_actual=[r.value_micro for r in archive.read()],
                code_paths=['telecapture/transport/receiver.py','telecapture/delivery/queues.py',
                            'telecapture/measurements/archive.py'],
                oracle='literal per-event MID ledger; 25 mC times 1000; original C asserted separately')))

    def test_serial_wrap(self):
        for tsn_start, mid_start in itertools.product((99, 2**32 - 2), (0, 2**32 - 1)):
            h = header(tsn=tsn_start, streams=((7, mid_start), (8, 0)))
            session = open_session(h)
            session.push(data(tsn_start + 1, mid_start, b'A', end=False))
            session.push(data(tsn_start + 3, mid_start + 1, b'C'))
            self.assertEqual(session.drain(), [])
            session.push(forward(tsn_start + 3, mid_start))
            messages = session.drain()
            self.assertEqual([(m.logical_mid - mid_start, m.body) for m in messages], [(1, b'C')])
            session.push(data(tsn_start + 2, mid_start, b'B', fsn=1, begin=False))
            session.push(forward(tsn_start + 3, mid_start))
            # An older source notification must not apply its larger MID range.
            session.push(forward(tsn_start + 2, mid_start + 5))
            session.push(data(tsn_start + 4, mid_start + 2, b'D'))
            self.assertEqual([m.body for m in session.drain()], [b'D'])
        self.assertEqual(compare32(0, 2**32 - 1), 1)
        self.assertEqual(unwrap32(0, 2**32 - 1), 2**32)
        self.assertEqual(next32(2**32 - 1), 0)
        with self.assertRaises(ValueError):
            compare32(2**31, 0)
        session = open_session(header())
        before = session.receiver.snapshot()
        with self.assertRaises(CaptureConflict):
            session.push(data(100, 2**31, b'bad'))
        self.assertEqual(session.receiver.snapshot(), before)
        # Large abandoned intervals advance without enumerating their missing MIDs.
        session.push(forward(100, 1000000000))
        session.push(data(101, 1000000001, b'next'))
        self.assertEqual([m.body for m in session.drain()], [b'next'])

    def test_small_event_model(self):
        # This oracle uses explicit small logical message/fragment coordinates.
        # It never calls the production serial, completeness or queue helpers.
        definitions = [('A', 0, 0, False), ('B', 0, 1, True),
                       ('C', 1, 0, True), ('D', 2, 0, True), ('F', 0, 0, False)]
        for tsn_base, mid_base in ((99, 0), (2**32 - 2, 2**32 - 1)):
            for order in itertools.permutations(range(5)):
                session = open_session(header(tsn=tsn_base, streams=((7, mid_base),)))
                pieces = {}
                ends = {}
                cursor = 0
                abandoned = -1
                expected = []
                actual = []
                for index in order:
                    label, mid, fsn, end = definitions[index]
                    if label == 'F':
                        event = forward(tsn_base + 4, mid_base)
                        abandoned = max(abandoned, 0)
                    else:
                        event = data(tsn_base + index + 1, mid_base + mid, label.encode(),
                                     fsn=fsn, begin=(fsn == 0), end=end)
                        pieces[(mid, fsn)] = label.encode()
                        if end:
                            ends[mid] = fsn
                    while True:
                        if cursor <= abandoned:
                            cursor = abandoned + 1
                        last = ends.get(cursor)
                        if last is None or any((cursor, i) not in pieces for i in range(last + 1)):
                            break
                        expected.append((cursor, b''.join(pieces[(cursor, i)] for i in range(last + 1))))
                        cursor += 1
                    session.push(event)
                    # Exercise same-content duplicate delivery at every boundary.
                    session.push(event)
                    actual.extend((m.logical_mid - mid_base, m.body) for m in session.drain())
                    self.assertEqual(actual, expected, (tsn_base, mid_base, order, index))
