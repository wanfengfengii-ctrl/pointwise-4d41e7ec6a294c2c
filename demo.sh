#!/bin/sh
set -eu
cd "$(dirname "$0")"
python3.11 -B - <<'PY'
import json
import tempfile
from pathlib import Path
from telecapture.measurements.archive import Archive
from telecapture.measurements.ingest import import_recording
with tempfile.TemporaryDirectory(prefix='telecapture-demo-') as directory:
    database = Path(directory) / 'archive.sqlite'
    with Archive(database) as archive:
        normal = import_recording('samples/normal.jsonl', archive)
        loss = import_recording('samples/abandonment.jsonl', archive)
        repeated = import_recording('samples/normal.jsonl', archive)
        rows = archive.read()
        actual = [(row.sensor_id, row.value_micro, row.quality) for row in rows]
        expected = [('thermometer', -1250000, 'good'),
                    ('barometer', 101325000000, 'suspect'),
                    ('spare', None, 'missing'),
                    ('thermometer', 25000000, 'good')]
        assert actual == expected, (expected, actual)
        assert (normal.inserted, loss.inserted, repeated.inserted) == (3, 1, 0)
        evidence = archive.evidence()
        assert len(evidence) == 4
        assert all(not (x['recording_id'] == 'loss-day' and x['logical_mid'] == 0) for x in evidence)
        print(json.dumps(dict(normal=normal.__dict__, loss=loss.__dict__,
                              replay=repeated.__dict__, measurements=[r.as_dict() for r in rows]),
                         sort_keys=True))
        print('POINTWISE_SCENARIO ' + json.dumps(dict(
            case_id='v3.push_visibility',
            input=dict(normal='samples/normal.jsonl', abandonment='samples/abandonment.jsonl'),
            expected=expected, actual=actual,
            downstream_expected=dict(evidence=4, replay_inserted=0, abandoned_measurements=0),
            downstream_actual=dict(evidence=len(evidence), replay_inserted=repeated.inserted,
                                   abandoned_measurements=sum(r.recording_id == 'loss-day' and r.logical_mid == 0 for r in rows)),
            code_paths=['telecapture/recording/reader.py','telecapture/transport/receiver.py',
                        'telecapture/delivery/queues.py','telecapture/measurements/archive.py'],
            oracle='literal integer unit multipliers and independent evidence/MID count')))
    with Archive(database) as reopened:
        assert [(r.sensor_id, r.value_micro, r.quality) for r in reopened.read()] == expected
    # Mixed ordered/unordered increment: one recording carries both modes.
    with Archive(database) as archive:
        mixed = import_recording('samples/mixed.jsonl', archive)
        replay = import_recording('samples/mixed.jsonl', archive)
        assert (mixed.events, mixed.delivered, mixed.inserted) == (7, 3, 3)
        assert (replay.delivered, replay.inserted) == (3, 0)
        mixed_rows = [r for r in archive.read() if r.recording_id == 'mixed-public']
        mixed_actual = [(r.unordered, r.logical_mid, r.value_micro) for r in mixed_rows]
        mixed_expected = [(True, 0, 10000), (True, 1, 11000), (True, 2, 12000)]
        assert mixed_actual == mixed_expected, (mixed_expected, mixed_actual)
        mixed_evidence = [e for e in archive.evidence() if e['recording_id'] == 'mixed-public']
        assert len(mixed_evidence) == 3 and all(e['unordered'] for e in mixed_evidence)
        print(json.dumps(dict(mixed=mixed.__dict__, mixed_replay=replay.__dict__,
                              mixed_measurements=[r.as_dict() for r in mixed_rows]),
                         sort_keys=True))
        print('POINTWISE_SCENARIO ' + json.dumps(dict(
            case_id='mixed.order_unordered_delivery',
            input=dict(mixed='samples/mixed.jsonl'),
            expected=mixed_expected, actual=mixed_actual,
            downstream_expected=dict(evidence=3, replay_inserted=0, ordered_delivered=0),
            downstream_actual=dict(evidence=len(mixed_evidence), replay_inserted=replay.inserted,
                                   ordered_delivered=sum(1 for r in mixed_rows if not r.unordered)),
            code_paths=['telecapture/recording/events.py','telecapture/transport/receiver.py',
                        'telecapture/delivery/queues.py','telecapture/measurements/archive.py'],
            oracle='literal per-event drain expectations from MIXED.md; 10/11/12 mC times 1000')))
    with Archive(database) as reopened:
        assert [(r.unordered, r.logical_mid, r.value_micro)
                for r in reopened.read() if r.recording_id == 'mixed-public'] == mixed_expected
        assert [(r.sensor_id, r.value_micro, r.quality)
                for r in reopened.read() if r.recording_id != 'mixed-public'] == expected
PY
