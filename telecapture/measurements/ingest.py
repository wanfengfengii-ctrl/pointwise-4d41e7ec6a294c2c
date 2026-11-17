import argparse
import json
import sys
from dataclasses import dataclass
from telecapture.recording.reader import open_recording
from telecapture.recording.provenance import RecordingError
from telecapture.association.lifecycle import open_session
from telecapture.association.context import AssociationError
from telecapture.transport.receipts import CaptureConflict
from telecapture.measurements.archive import Archive
from telecapture.measurements.schema import ArchiveError

@dataclass(frozen=True)
class IngestReport:
    events: int
    delivered: int
    inserted: int
    incomplete: tuple

class IngestRun:
    def __init__(self, recording, archive):
        self.recording = recording
        self.archive = archive
        self.archive.register_recording(recording.header)
        self.session = open_session(recording.header)
        self.iterator = iter(recording.events())
        self.pending = []
        self.events = 0
        self.delivered = 0
        self.inserted = 0
        self.finished = False
        self.incomplete = ()

    def retry_pending(self):
        if self.pending:
            inserted = self.archive.commit(self.pending)
            self.inserted += inserted
            self.pending = []
        return self.inserted

    def run(self):
        self.retry_pending()
        if not self.finished:
            for event in self.iterator:
                self.session.push(event)
                self.events += 1
                self.pending = self.session.drain()
                self.delivered += len(self.pending)
                self.retry_pending()
            self.incomplete = self.session.finish_input()
            self.finished = True
        return IngestReport(self.events, self.delivered, self.inserted, self.incomplete)

def import_recording(path, archive):
    with open_recording(path) as recording:
        return IngestRun(recording, archive).run()

def main(argv=None):
    parser = argparse.ArgumentParser(description='Import offline gateway recordings and query measurements')
    parser.add_argument('--db', required=True)
    commands = parser.add_subparsers(dest='command', required=True)
    importing = commands.add_parser('import')
    importing.add_argument('recording')
    reading = commands.add_parser('read')
    reading.add_argument('--gateway')
    reading.add_argument('--stream', type=int)
    reading.add_argument('--start-us', type=int)
    reading.add_argument('--end-us', type=int)
    args = parser.parse_args(argv)
    try:
        with Archive(args.db) as archive:
            if args.command == 'import':
                report = import_recording(args.recording, archive)
                print(json.dumps(report.__dict__, sort_keys=True))
            else:
                rows = archive.read(gateway_id=args.gateway, stream_id=args.stream,
                                    start_us=args.start_us, end_us=args.end_us)
                print(json.dumps([row.as_dict() for row in rows], sort_keys=True))
        return 0
    except (RecordingError, AssociationError, CaptureConflict, ArchiveError) as exc:
        print(json.dumps(dict(error=type(exc).__name__, message=str(exc)), sort_keys=True), file=sys.stderr)
        return 2

if __name__ == '__main__':
    raise SystemExit(main())
