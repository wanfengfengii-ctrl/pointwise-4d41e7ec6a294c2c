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
PY
