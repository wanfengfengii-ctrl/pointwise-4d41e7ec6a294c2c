import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from tests.test_capture import header, data, payload, delivered, write_recording
from telecapture.measurements.archive import Archive
from telecapture.measurements.schema import ArchiveError
from telecapture.measurements.ingest import IngestRun
from telecapture.recording.reader import open_recording

class TestArchive(unittest.TestCase):
    def test_decode_and_commit_failure(self):
        h = header()
        old = delivered(h, [data(100, 0, payload(10))])
        good = delivered(h, [data(100, 0, payload(10)), data(101, 1, payload(20))])[1:]
        bad_body = json.loads(payload(3))
        bad_body['measurements'].append(dict(sensor_id='bad', time_us=5, quantity='temperature',
                                            unit='Pa', value=4, quality='good'))
        bad = replace(good[0], body=json.dumps(bad_body).encode())
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'archive.sqlite'
            with Archive(database) as archive:
                archive.register_recording(h)
                archive.commit(old)
                baseline = archive.evidence()
                with self.assertRaises(ArchiveError) as raised:
                    archive.commit(good + [replace(bad, logical_mid=2, raw_mid=2)])
                self.assertEqual(raised.exception.reason, 'payload')
                self.assertEqual(archive.evidence(), baseline)
                with self.assertRaises(ArchiveError) as raised:
                    archive.commit([replace(old[0], body=payload(999))])
                self.assertEqual(raised.exception.reason, 'identity_conflict')
                self.assertEqual(archive.evidence(), baseline)
            for stage in ('after_evidence', 'after_measurement', 'before_commit'):
                def fail(point, target=stage):
                    if point == target:
                        raise sqlite3.OperationalError('controlled submission failure')
                with Archive(database, fault=fail) as archive:
                    with self.assertRaises(ArchiveError) as raised:
                        archive.commit(good)
                    self.assertEqual(raised.exception.reason, 'storage')
                with Archive(database) as reopened:
                    self.assertEqual(reopened.evidence(), baseline)
                    self.assertEqual([row.value_micro for row in reopened.read()], [10000])
            with Archive(database) as archive:
                self.assertEqual(archive.commit(good), 1)
                self.assertEqual(archive.commit(good), 0)
                self.assertEqual([row.value_micro for row in archive.read()], [10000, 20000])

    def test_pending_retry(self):
        h = header()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'input.jsonl'
            database = Path(directory) / 'store.sqlite'
            write_recording(path, h, [data(100, 0, payload(4)), data(101, 1, payload(5))])
            failures = ['before_commit']
            def fail(stage):
                if failures and stage == failures[0]:
                    failures.pop()
                    raise sqlite3.OperationalError('one real transaction failure')
            with open_recording(path) as recording, Archive(database, fault=fail) as archive:
                run = IngestRun(recording, archive)
                with self.assertRaises(ArchiveError):
                    run.run()
                self.assertEqual([m.raw_mid for m in run.pending], [0])
                self.assertEqual(run.session.drain(), [])
                self.assertEqual(archive.read(), [])
                run.retry_pending()
                report = run.run()
                self.assertEqual((report.events, report.delivered, report.inserted), (2, 2, 2))
                self.assertEqual(run.pending, [])
                self.assertEqual(run.run(), report)
                self.assertEqual([r.value_micro for r in archive.read()], [4000, 5000])
            # Bad payload does not clear the retained drained batch.
            write_recording(path, header(recording='bad'), [data(100, 0, b'bad JSON')])
            with open_recording(path) as recording, Archive(database) as archive:
                run = IngestRun(recording, archive)
                with self.assertRaises(ArchiveError) as raised:
                    run.run()
                self.assertEqual(raised.exception.reason, 'payload')
                self.assertEqual([m.body for m in run.pending], [b'bad JSON'])
                self.assertEqual([r.value_micro for r in archive.read()], [4000, 5000])

    def test_payload_rules(self):
        h = header()
        with Archive() as archive:
            archive.register_recording(h)
            original = json.loads(payload())
            variants = [dict(value=True), dict(value=None), dict(quality='missing'),
                        dict(quality='unknown'), dict(unit='K'), dict(time_us=True),
                        dict(sensor_id=''), dict(quantity='wind')]
            for change in variants:
                body = json.loads(json.dumps(original))
                body['measurements'][0].update(change)
                batch = delivered(h, [data(100, 0, json.dumps(body).encode())])
                with self.assertRaises(ArchiveError) as raised:
                    archive.commit(batch)
                self.assertEqual(raised.exception.reason, 'payload')
                self.assertEqual(archive.read(), [])
                self.assertEqual(archive.evidence(), [])

    def test_initialization_and_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / 'store.sqlite'
            h = header(recording='user')
            with Archive(database) as archive:
                archive.register_recording(h)
                archive.commit(delivered(h, [data(100, 0, payload(42))]))
                archive.connection.execute('CREATE TABLE user_notes (note TEXT)')
                archive.connection.execute("INSERT INTO user_notes VALUES ('keep')")
                before = archive.evidence()
            with Archive(database) as archive:
                self.assertEqual(archive.evidence(), before)
                self.assertEqual(archive.connection.execute('SELECT note FROM user_notes').fetchall(), [('keep',)])
                archive.connection.execute("UPDATE measurements SET value_micro='9'")
                with self.assertRaises(ArchiveError) as raised:
                    archive.read()
                self.assertEqual(raised.exception.reason, 'schema')
            with self.assertRaises(ArchiveError):
                Archive(database)
            bad = Path(directory) / 'bad.sqlite'
            connection = sqlite3.connect(bad)
            connection.execute('CREATE TABLE messages (wrong TEXT)')
            connection.execute("INSERT INTO messages VALUES ('preserve')")
            connection.commit()
            connection.close()
            with self.assertRaises(ArchiveError) as raised:
                Archive(bad)
            self.assertEqual(raised.exception.reason, 'schema')
            connection = sqlite3.connect(bad)
            self.assertEqual(connection.execute('SELECT * FROM messages').fetchall(), [('preserve',)])
            connection.close()
