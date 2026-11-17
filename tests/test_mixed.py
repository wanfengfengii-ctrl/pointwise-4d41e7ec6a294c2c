import itertools
import sqlite3
import tempfile
import unittest
from pathlib import Path
from tests.test_capture import payload
from telecapture.recording.header import parse_header
from telecapture.recording.events import IDataEvent, ForwardEvent, ForwardEntry
from telecapture.association.lifecycle import open_session
from telecapture.delivery.queues import DeliveredMessage, make_delivery_key
from telecapture.transport.receipts import CaptureConflict
from telecapture.measurements.archive import Archive
from telecapture.measurements.schema import ArchiveError

MODULUS = 2**32

def mheader(recording='mix', tsn=99, streams=((7, 0, 0),)):
    return parse_header(dict(version=1, recording_id=recording, gateway_id='g',
        session_id='s', initial_cumulative_tsn=tsn,
        streams=[dict(stream_id=s, initial_mid=m, initial_unordered_mid=u)
                 for s, m, u in streams]))

def mdata(tsn, mid, body, unordered, stream=7, fsn=0, begin=True, end=True):
    return IDataEvent('s', tsn % MODULUS, stream, mid % MODULUS, fsn, begin, end,
                      body, unordered=unordered)

def mforward(tsn, entries):
    return ForwardEvent('s', tsn % MODULUS,
                        tuple(ForwardEntry(stream, mid % MODULUS, unordered)
                              for stream, mid, unordered in entries))

class MixedOracle:
    """Independent small-domain oracle in explicit logical coordinates.

    Expectations come from plain dictionaries keyed by explicit logical
    identity; it never calls production serial, queue or store helpers.
    """
    def __init__(self, forward_position):
        self.initial = {False: 0, True: 0}
        self.pieces = {False: {}, True: {}}
        self.ends = {False: {}, True: {}}
        self.abandoned = {False: -1, True: -1}
        self.cursor = 0
        self.delivered = set()
        self.forward_position = forward_position

    def fragment(self, unordered, mid, fsn, body, end):
        if mid < self.initial[unordered] or mid <= self.abandoned[unordered]:
            return
        if unordered and mid in self.delivered:
            return
        if not unordered and mid < self.cursor:
            return
        self.pieces[unordered][(mid, fsn)] = body
        if end:
            self.ends[unordered][mid] = fsn

    def forward(self, position, entries):
        if position < self.forward_position:
            return
        self.forward_position = position
        for unordered, through in entries:
            if through < self.initial[unordered]:
                continue
            self.abandoned[unordered] = max(self.abandoned[unordered], through)
            self.pieces[unordered] = {k: v for k, v in self.pieces[unordered].items()
                                      if k[0] > through}
            self.ends[unordered] = {k: v for k, v in self.ends[unordered].items()
                                    if k > through}

    def _complete(self, unordered, mid):
        last = self.ends[unordered].get(mid)
        return (last is not None and (mid, 0) in self.pieces[unordered]
                and all((mid, fsn) in self.pieces[unordered] for fsn in range(last + 1)))

    def _body(self, unordered, mid):
        return b''.join(self.pieces[unordered][(mid, fsn)]
                        for fsn in range(self.ends[unordered][mid] + 1))

    def drain(self):
        result = []
        position = max(self.cursor, self.abandoned[False] + 1)
        while self._complete(False, position):
            result.append((False, position, self._body(False, position)))
            position += 1
        self.cursor = max(position, self.abandoned[False] + 1)
        self.pieces[False] = {k: v for k, v in self.pieces[False].items()
                              if k[0] >= self.cursor}
        self.ends[False] = {k: v for k, v in self.ends[False].items()
                            if k >= self.cursor}
        for mid in sorted(m for m in self.ends[True]
                          if m not in self.delivered and self._complete(True, m)):
            result.append((True, mid, self._body(True, mid)))
            self.delivered.add(mid)
            self.pieces[True] = {k: v for k, v in self.pieces[True].items() if k[0] != mid}
            del self.ends[True][mid]
        return result

    def incomplete(self):
        result = []
        for unordered in (False, True):
            for mid in sorted({m for (m, _) in self.pieces[unordered]}):
                if not self._complete(unordered, mid):
                    result.append((7, mid, True) if unordered else (7, mid))
        return tuple(result)

class TestMixedSmallDomain(unittest.TestCase):
    def test_permutation_model(self):
        definitions = [
            dict(kind='data', unordered=False, mid=0, fsn=0, begin=True, end=False, body=b'A'),
            dict(kind='data', unordered=False, mid=0, fsn=1, begin=False, end=True, body=b'B'),
            dict(kind='data', unordered=False, mid=1, fsn=0, begin=True, end=True, body=b'C'),
            dict(kind='data', unordered=True, mid=0, fsn=0, begin=True, end=True, body=b'u'),
            dict(kind='data', unordered=True, mid=2, fsn=0, begin=True, end=True, body=b'v'),
            dict(kind='forward', entries=((False, 0), (True, 0))),
        ]
        for tsn_base, ordered_initial, unordered_initial in (
                (99, 0, 0), (MODULUS - 3, MODULUS - 2, MODULUS - 1)):
            for order in itertools.permutations(range(len(definitions))):
                session = open_session(mheader(tsn=tsn_base,
                    streams=((7, ordered_initial, unordered_initial),)))
                oracle = MixedOracle(tsn_base)
                for index in order:
                    spec = definitions[index]
                    if spec['kind'] == 'data':
                        initial = unordered_initial if spec['unordered'] else ordered_initial
                        event = mdata(tsn_base + 1 + index, initial + spec['mid'], spec['body'],
                                      spec['unordered'], fsn=spec['fsn'],
                                      begin=spec['begin'], end=spec['end'])
                        oracle.fragment(spec['unordered'], spec['mid'], spec['fsn'],
                                        spec['body'], spec['end'])
                    else:
                        event = mforward(tsn_base + 1 + index, tuple(
                            (7, (unordered_initial if unordered else ordered_initial) + offset,
                             unordered) for unordered, offset in spec['entries']))
                        oracle.forward(tsn_base + 1 + index, spec['entries'])
                    for repeat in range(2):
                        session.push(event)
                        actual = [(m.unordered,
                                   m.logical_mid - (unordered_initial if m.unordered
                                                    else ordered_initial),
                                   m.body) for m in session.drain()]
                        self.assertEqual(actual, oracle.drain(),
                                         (tsn_base, ordered_initial, order, index, repeat))
                self.assertEqual(session.finish_input(), oracle.incomplete(),
                                 (tsn_base, ordered_initial, order))

class TestMixedDelivery(unittest.TestCase):
    def test_unordered_high_first_low_late(self):
        session = open_session(mheader())
        session.push(mdata(100, 0, b'blocked-', False, end=False))
        session.push(mdata(101, 2, b'high', True))
        self.assertEqual([(m.unordered, m.logical_mid, m.body) for m in session.drain()],
                         [(True, 2, b'high')])
        session.push(mdata(102, 0, b'lo', True, end=False))
        session.push(mdata(103, 1, b'mid', True))
        self.assertEqual([(m.unordered, m.logical_mid, m.body) for m in session.drain()],
                         [(True, 1, b'mid')])
        # A delivered high MID must not swallow the late-completing low MID.
        session.push(mdata(104, 0, b'w', True, fsn=1, begin=False, end=True))
        self.assertEqual([(m.unordered, m.logical_mid, m.body) for m in session.drain()],
                         [(True, 0, b'low')])
        # The ordered space is still blocked by its own gap only.
        session.push(mdata(105, 1, b'next', False))
        self.assertEqual(session.drain(), [])
        session.push(mforward(106, ((7, 0, False),)))
        self.assertEqual([(m.unordered, m.logical_mid, m.body) for m in session.drain()],
                         [(False, 1, b'next')])
        # Drained identities never re-deliver, even as exact retransmissions.
        session.push(mdata(107, 2, b'high', True))
        session.push(mdata(108, 0, b'w', True, fsn=1, begin=False, end=True))
        self.assertEqual(session.drain(), [])
        # The abandoned ordered gap was retired by the forward notification.
        self.assertEqual(session.finish_input(), ())

    def test_wrap_both_spaces(self):
        session = open_session(mheader(tsn=MODULUS - 3,
                                       streams=((7, MODULUS - 2, MODULUS - 1),)))
        session.push(mdata(MODULUS - 2, MODULUS - 2, b'o1', False))
        self.assertEqual([(m.unordered, m.logical_mid) for m in session.drain()],
                         [(False, MODULUS - 2)])
        session.push(mdata(MODULUS - 1, MODULUS - 1, b'u1', True))
        self.assertEqual([(m.unordered, m.logical_mid) for m in session.drain()],
                         [(True, MODULUS - 1)])
        session.push(mdata(MODULUS, 0, b'u2', True))
        self.assertEqual([(m.unordered, m.logical_mid) for m in session.drain()],
                         [(True, MODULUS)])
        session.push(mdata(MODULUS + 1, MODULUS - 1, b'o2', False))
        self.assertEqual([(m.unordered, m.logical_mid) for m in session.drain()],
                         [(False, MODULUS - 1)])
        # Unordered abandonment across the wrap boundary retires undelivered data.
        session.push(mdata(MODULUS + 2, 1, b'u3', True, end=False))
        session.push(mforward(MODULUS + 3, ((7, 1, True),)))
        self.assertEqual(session.drain(), [])
        session.push(mdata(MODULUS + 4, 1, b'tail', True, fsn=1, begin=False, end=True))
        self.assertEqual(session.drain(), [])
        session.push(mdata(MODULUS + 5, 2, b'u4', True))
        self.assertEqual([(m.unordered, m.logical_mid) for m in session.drain()],
                         [(True, MODULUS + 2)])
        # Ordered abandonment across the wrap boundary.
        session.push(mdata(MODULUS + 6, 0, b'o3', False, end=False))
        session.push(mforward(MODULUS + 7, ((7, 0, False),)))
        self.assertEqual(session.drain(), [])
        self.assertEqual(session.finish_input(), ())

    def test_old_and_same_position_notifications(self):
        session = open_session(mheader())
        session.push(mdata(100, 0, b'u0', True, end=False))
        session.push(mdata(101, 0, b'o0', False, end=False))
        session.push(mforward(102, ((7, 0, False), (7, 0, True))))
        self.assertEqual(session.drain(), [])
        # Same position merges; a smaller same-position range does not retreat.
        session.push(mforward(102, ((7, 1, True),)))
        session.push(mforward(102, ((7, 0, True),)))
        # A strictly older notification is void as a whole, for both modes.
        session.push(mforward(101, ((7, 5, False), (7, 5, True))))
        session.push(mdata(103, 1, b'u1', True))
        session.push(mdata(104, 2, b'u2', True))
        self.assertEqual([(m.unordered, m.logical_mid, m.body) for m in session.drain()],
                         [(True, 2, b'u2')])
        session.push(mdata(105, 1, b'o1', False))
        self.assertEqual([(m.unordered, m.logical_mid, m.body) for m in session.drain()],
                         [(False, 1, b'o1')])
        session.push(mdata(106, 5, b'u5', True))
        self.assertEqual([(m.unordered, m.logical_mid, m.body) for m in session.drain()],
                         [(True, 5, b'u5')])

    def test_mode_change_conflict_is_atomic(self):
        session = open_session(mheader())
        session.push(mdata(100, 0, b'U', True, end=False))
        session.push(mdata(101, 0, b'O', False, end=False))
        before = session.receiver.snapshot()
        with self.assertRaises(CaptureConflict):
            session.push(mdata(100, 0, b'U', False, end=False))
        self.assertEqual(session.receiver.snapshot(), before)
        with self.assertRaises(CaptureConflict):
            session.push(mdata(102, 0, b'X', True, end=False))
        self.assertEqual(session.receiver.snapshot(), before)
        with self.assertRaises(CaptureConflict):
            session.push(mdata(103, 0, b'Y', False, end=False))
        self.assertEqual(session.receiver.snapshot(), before)
        session.push(mdata(104, 0, b'-o', False, fsn=1, begin=False, end=True))
        session.push(mdata(105, 0, b'-u', True, fsn=1, begin=False, end=True))
        self.assertEqual([(m.unordered, m.body) for m in session.drain()],
                         [(False, b'O-o'), (True, b'U-u')])
        # Failed pushes consumed nothing: TSN 102 still belongs to no receipt.
        with self.assertRaises(CaptureConflict):
            session.push(mdata(102, 0, b'Z', True, end=False))
        self.assertEqual(session.finish_input(), ())

    def test_finish_input_orders_modes(self):
        session = open_session(mheader(streams=((7, 0, 0), (8, 0, 0))))
        session.push(mdata(100, 2, b'x', True, end=False))
        session.push(mdata(101, 1, b'x', False, end=False))
        session.push(mdata(102, 0, b'x', True, end=False))
        session.push(mdata(103, 0, b'x', False, stream=8, end=False))
        session.push(mdata(104, 5, b'y', False, end=False))
        self.assertEqual(session.finish_input(),
                         ((7, 1), (7, 5), (7, 0, True), (7, 2, True), (8, 0)))

    def test_unordered_below_initial_never_delivered(self):
        session = open_session(mheader(streams=((7, 0, 5),)))
        session.push(mdata(100, 3, b'old', True))
        self.assertEqual(session.drain(), [])
        session.push(mdata(101, 5, b'new', True))
        self.assertEqual([(m.logical_mid, m.body) for m in session.drain()], [(5, b'new')])
        self.assertEqual(session.finish_input(), ())

    def test_delivery_key_namespaces(self):
        ordered = DeliveredMessage('r', 'g', 's', 7, 3, 3, b'x')
        unordered = DeliveredMessage('r', 'g', 's', 7, 3, 3, b'x', unordered=True)
        self.assertEqual(ordered.key, '["r","g","s",7,3]')
        self.assertEqual(ordered.key, make_delivery_key('r', 'g', 's', 7, 3))
        self.assertEqual(unordered.key, make_delivery_key('r', 'g', 's', 7, 3, unordered=True))
        self.assertNotEqual(ordered.key, unordered.key)
        # Keys depend only on identity, mode and logical MID: not body or fragments.
        self.assertEqual(DeliveredMessage('r', 'g', 's', 7, 3, 3, b'other',
                                          unordered=True).key, unordered.key)
        self.assertFalse(DeliveredMessage('r', 'g', 's', 7, 3, 3, b'x').unordered)
        with self.assertRaises(ValueError):
            DeliveredMessage('r', 'g', 's', 7, 3, 3, b'x', unordered=1)
        with self.assertRaises(ValueError):
            make_delivery_key('r', 'g', 's', 7, 3, unordered=0)

class TestMixedArchive(unittest.TestCase):
    def test_commit_failure_and_retry(self):
        h = mheader('arch')
        batch = [DeliveredMessage('arch', 'g', 's', 7, 0, 0, payload(10, sensor='o')),
                 DeliveredMessage('arch', 'g', 's', 7, 0, 0, payload(20, sensor='u'),
                                  unordered=True),
                 DeliveredMessage('arch', 'g', 's', 7, 1, 1, payload(30, sensor='u2'),
                                  unordered=True)]
        with tempfile.TemporaryDirectory() as directory:
            for stage in ('after_evidence', 'after_measurement', 'before_commit'):
                database = Path(directory) / f'{stage}.sqlite'
                armed = [True]
                def fail(point, target=stage):
                    if point == target and armed[0]:
                        armed[0] = False
                        raise sqlite3.OperationalError('controlled mixed failure')
                with Archive(database, fault=fail) as archive:
                    archive.register_recording(h)
                    with self.assertRaises(ArchiveError) as raised:
                        archive.commit(batch)
                    self.assertEqual(raised.exception.reason, 'storage')
                    self.assertEqual(archive.evidence(), [])
                    self.assertEqual(archive.commit(batch), 3)
                    self.assertEqual(archive.commit(batch), 0)
                with Archive(database) as reopened:
                    self.assertEqual(
                        [(r.unordered, r.logical_mid, r.value_micro) for r in reopened.read()],
                        [(False, 0, 10000), (True, 0, 20000), (True, 1, 30000)])
                    self.assertEqual(sorted(e['unordered'] for e in reopened.evidence()),
                                     [False, True, True])

    def test_identity_conflict_rolls_back_prefix(self):
        h = mheader('arch2')
        good_ordered = DeliveredMessage('arch2', 'g', 's', 7, 0, 0, payload(1))
        good_unordered = DeliveredMessage('arch2', 'g', 's', 7, 0, 0, payload(2),
                                          unordered=True)
        with Archive() as archive:
            archive.register_recording(h)
            self.assertEqual(archive.commit([good_ordered, good_unordered]), 2)
            baseline = archive.evidence()
            conflict = DeliveredMessage('arch2', 'g', 's', 7, 0, 0, payload(3), unordered=True)
            new_ordered = DeliveredMessage('arch2', 'g', 's', 7, 1, 1, payload(4))
            with self.assertRaises(ArchiveError) as raised:
                archive.commit([new_ordered, conflict])
            self.assertEqual(raised.exception.reason, 'identity_conflict')
            self.assertEqual(archive.evidence(), baseline)
            bad = DeliveredMessage('arch2', 'g', 's', 7, 2, 2, b'not json', unordered=True)
            with self.assertRaises(ArchiveError) as raised:
                archive.commit([DeliveredMessage('arch2', 'g', 's', 7, 3, 3, payload(5),
                                                 unordered=True), bad])
            self.assertEqual(raised.exception.reason, 'payload')
            self.assertEqual(archive.evidence(), baseline)

    def test_archive_checks_mode_initial(self):
        h = mheader('arch3', streams=((7, 2, 9),))
        with Archive() as archive:
            archive.register_recording(h)
            with self.assertRaises(ArchiveError) as raised:
                archive.commit([DeliveredMessage('arch3', 'g', 's', 7, 1, 1, payload(1))])
            self.assertEqual(raised.exception.reason, 'identity_conflict')
            with self.assertRaises(ArchiveError) as raised:
                archive.commit([DeliveredMessage('arch3', 'g', 's', 7, 8, 8, payload(1),
                                                 unordered=True)])
            self.assertEqual(raised.exception.reason, 'identity_conflict')
            ok = [DeliveredMessage('arch3', 'g', 's', 7, 2, 2, payload(1)),
                  DeliveredMessage('arch3', 'g', 's', 7, 9, 9, payload(2), unordered=True)]
            self.assertEqual(archive.commit(ok), 2)
            self.assertEqual([(r.unordered, r.logical_mid) for r in archive.read()],
                             [(False, 2), (True, 9)])
