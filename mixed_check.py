"""Finite public mixed-mode scenarios; no receiver or archive implementation."""
import argparse
import base64
import json
import sqlite3
import tempfile
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SAMPLE = ROOT / 'samples' / 'mixed.jsonl'


def body(value, time, sensor):
    return json.dumps(dict(version=1, measurements=[dict(
        sensor_id=sensor, time_us=time, quantity='temperature',
        unit='mC', value=value, quality='good')]),
        sort_keys=True, separators=(',', ':')).encode('utf-8')


def header(recording='mixed-public', initial=0):
    return dict(version=1, recording_id=recording, gateway_id='g',
        session_id='s', initial_cumulative_tsn=99,
        streams=[dict(stream_id=7, initial_mid=initial,
                      initial_unordered_mid=initial)])


def data(tsn, mid, payload, unordered=False, fsn=0, begin=True, end=True):
    return dict(type='I-DATA', session_id='s', tsn=tsn % 2**32,
        stream_id=7, mid=mid % 2**32, fsn=fsn, begin=begin, end=end,
        payload_b64=base64.b64encode(payload).decode('ascii'),
        unordered=unordered)


def forward(tsn, mid, unordered=False):
    return dict(type='I-FORWARD-TSN', session_id='s',
        cumulative_tsn=tsn % 2**32,
        entries=[dict(stream_id=7, mid=mid % 2**32, unordered=unordered)])


def scenario():
    ordered = body(99, 99, 'ordered-abandoned')
    return header(), [
        data(100, 0, ordered[:17], end=False),
        data(101, 1, body(11, 101, 'u1'), True),
        data(102, 0, body(10, 100, 'u0'), True),
        forward(102, 0),
        data(103, 2, body(12, 102, 'u2'), True),
        data(104, 0, ordered[17:], fsn=1, begin=False),
        data(101, 1, body(11, 101, 'u1'), True)], [
        [], [(True, 1)], [(True, 0)], [], [(True, 2)], [], []]


def sample_text():
    h, events, _ = scenario()
    return ''.join(json.dumps(x, sort_keys=True, separators=(',', ':')) + '\n'
                   for x in [h] + events)


def self_check(text=None):
    if text is None:
        text = SAMPLE.read_text(encoding='utf-8')
    values = [json.loads(line) for line in text.splitlines()]
    h, events, expected = scenario()
    assert values == [h] + events
    assert expected == [[], [(True, 1)], [(True, 0)], [], [(True, 2)], [], []]
    for index in (1, 2, 4):
        value = json.loads(base64.b64decode(events[index]['payload_b64'], validate=True))
        row = value['measurements'][0]
        assert (row['value'], row['time_us']) == {1: (11, 101), 2: (10, 100), 4: (12, 102)}[index]
    assert (events[0]['unordered'], events[3]['entries'][0]['unordered']) == (False, False)
    assert (base64.b64decode(events[0]['payload_b64'], validate=True) +
            base64.b64decode(events[5]['payload_b64'], validate=True)) == body(99, 99, 'ordered-abandoned')
    print('MIXED_FIXTURE_OK')


def collect(h, events):
    from telecapture.recording.header import parse_header
    from telecapture.recording.events import decode_event
    from telecapture.association.lifecycle import open_session
    session = open_session(parse_header(h))
    batches = []
    for value in events:
        session.push(decode_event(value, mixed=True))
        batches.append(session.drain())
    return session, batches


def verify():
    from telecapture.recording.events import decode_event
    from telecapture.recording.header import parse_header
    from telecapture.recording.provenance import RecordingError
    from telecapture.association.lifecycle import open_session
    from telecapture.delivery.queues import DeliveredMessage
    from telecapture.measurements.archive import Archive
    from telecapture.measurements.schema import ArchiveError
    from telecapture.measurements.ingest import import_recording, IngestRun
    from telecapture.recording.reader import open_recording
    from telecapture.transport.receipts import CaptureConflict

    h, events, expected = scenario()
    session, batches = collect(h, events)
    actual = [[(m.unordered, m.logical_mid) for m in batch] for batch in batches]
    assert actual == expected, (expected, actual)
    assert session.finish_input() == ()
    assert [m.body for batch in batches for m in batch] == [
        body(11, 101, 'u1'), body(10, 100, 'u0'), body(12, 102, 'u2')]

    # Existing strict API remains strict; the explicit extended entry accepts it.
    try:
        decode_event(events[1])
    except RecordingError:
        pass
    else:
        raise AssertionError('legacy decode_event accepted a V2-only field')

    # Same stream/MID has two complete, independent bodies. Drain ordering is literal.
    same = header('same-mid')
    a, b = body(20, 100, 'ordered'), body(30, 100, 'unordered')
    s = open_session(parse_header(same))
    s.push(decode_event(data(100, 0, b, True), mixed=True))
    s.push(decode_event(data(101, 0, a), mixed=True))
    pair = s.drain()
    assert [(m.unordered, m.logical_mid, m.body) for m in pair] == [
        (False, 0, a), (True, 0, b)]
    assert pair[0].key == '["same-mid","g","s",7,0]'
    assert pair[0].key != pair[1].key
    before = s.receiver.snapshot()
    changed = dict(data(100, 0, b, True), unordered=False)
    try:
        s.push(decode_event(changed, mixed=True))
    except CaptureConflict:
        pass
    else:
        raise AssertionError('mode changed under one TSN without conflict')
    assert s.receiver.snapshot() == before

    # Mode-specific abandonment covers undrained complete data, not the other mode.
    t = open_session(parse_header(header('abandon')))
    t.push(decode_event(data(100, 0, b, True), mixed=True))
    t.push(decode_event(data(101, 0, a), mixed=True))
    t.push(decode_event(forward(101, 0, True), mixed=True))
    assert [(m.unordered, m.body) for m in t.drain()] == [(False, a)]
    t.push(decode_event(data(102, 0, b, True), mixed=True))
    assert t.drain() == []

    # Literal identities across both serial wrap boundaries; an old notification
    # must not terminate the next unordered message.
    modulus = 2**32
    wrap = header('wrap', modulus - 1)
    wrap['initial_cumulative_tsn'] = modulus - 2
    w = open_session(parse_header(wrap))
    for event, result in [
        (data(modulus - 1, modulus - 1, b'O', end=False), []),
        (data(modulus, 0, b'H', True), [(True, modulus, b'H')]),
        (data(modulus + 1, modulus - 1, b'L', True), [(True, modulus - 1, b'L')]),
        (forward(modulus + 1, modulus - 1), []),
        (data(modulus + 2, 1, b'A', True, end=False), []),
        (forward(modulus, 1, True), []),
        (data(modulus + 3, 1, b'B', True, fsn=1, begin=False),
         [(True, modulus + 1, b'AB')])]:
        w.push(decode_event(event, mixed=True))
        assert [(m.unordered, m.logical_mid, m.body) for m in w.drain()] == result
    before = w.receiver.snapshot()
    try:
        w.push(decode_event(data(modulus + 4, modulus + 1 + 2**31, b'bad', True), mixed=True))
    except CaptureConflict:
        pass
    else:
        raise AssertionError('half-range unordered MID accepted')
    assert w.receiver.snapshot() == before
    unfinished = open_session(parse_header(header('unfinished')))
    unfinished.push(decode_event(data(100, 0, b'O', end=False), mixed=True))
    unfinished.push(decode_event(data(101, 0, b'U', True, end=False), mixed=True))
    assert unfinished.finish_input() == ((7, 0), (7, 0, True))

    # Archive is tested with independent public messages, not only queue output.
    direct = [
        DeliveredMessage('same-mid', 'g', 's', 7, 0, 0, a),
        DeliveredMessage('same-mid', 'g', 's', 7, 0, 0, b, unordered=True)]
    with tempfile.TemporaryDirectory(prefix='mixed-check-') as directory:
        database = Path(directory) / 'archive.sqlite'
        with Archive(database) as archive:
            archive.register_recording(parse_header(same))
            assert archive.commit(direct) == 2
            assert archive.commit(direct) == 0
            rows = archive.read()
            assert [(r.unordered, r.value_micro, r.body) for r in rows] == [
                (False, 20000, a), (True, 30000, b)]
            assert {e['unordered'] for e in archive.evidence()} == {False, True}
            evidence = archive.evidence()
            try:
                archive.commit([replace(direct[1], body=body(31, 100, 'unordered'))])
            except ArchiveError as exc:
                assert exc.reason == 'identity_conflict'
            else:
                raise AssertionError('unordered identity conflict accepted')
            assert archive.evidence() == evidence
            first = import_recording(SAMPLE, archive)
            replay = import_recording(SAMPLE, archive)
            assert (first.inserted, replay.inserted) == (3, 0)
        with Archive(database) as archive:
            assert {(r.recording_id, r.unordered, r.logical_mid, r.value_micro) for r in archive.read()} == {
                ('same-mid', False, 0, 20000), ('same-mid', True, 0, 30000),
                ('mixed-public', True, 0, 10000), ('mixed-public', True, 1, 11000),
                ('mixed-public', True, 2, 12000)}

        # A real rolled-back mixed batch and existing pending retry semantics.
        armed = [True]
        def fail(stage):
            if stage == 'before_commit' and armed[0]:
                armed[0] = False
                raise sqlite3.OperationalError('controlled mixed commit failure')
        retry_db = Path(directory) / 'retry.sqlite'
        with Archive(retry_db, fault=fail) as archive:
            archive.register_recording(parse_header(same))
            try:
                archive.commit(direct)
            except ArchiveError as exc:
                assert exc.reason == 'storage'
            else:
                raise AssertionError('controlled failure did not fire')
            assert archive.evidence() == []
            assert archive.commit(direct) == 2
        armed[0] = True
        pending_db = Path(directory) / 'pending.sqlite'
        with open_recording(SAMPLE) as recording, Archive(pending_db, fault=fail) as archive:
            run = IngestRun(recording, archive)
            try:
                run.run()
            except ArchiveError as exc:
                assert exc.reason == 'storage'
            else:
                raise AssertionError('pending failure did not fire')
            assert [(m.unordered, m.logical_mid) for m in run.pending] == [(True, 1)]
            assert archive.read() == []
            run.retry_pending()
            assert run.run().inserted == 3
        with Archive(pending_db) as archive:
            assert [r.value_micro for r in archive.read()] == [10000, 11000, 12000]
    print(json.dumps(dict(case='mixed-order-and-identity',
        expected=expected, actual=actual,
        archived_micro=[10000, 11000, 12000]), sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('self-check', 'verify', 'emit'))
    action = parser.parse_args().action
    if action == 'self-check':
        self_check()
    elif action == 'emit':
        print(sample_text(), end='')
    else:
        self_check()
        verify()


if __name__ == '__main__':
    main()
