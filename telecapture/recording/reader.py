from pathlib import Path
from telecapture.recording.provenance import RecordingError, SourceLocation, make_evidence
from telecapture.recording.validation import json_object
from telecapture.recording.header import parse_header
from telecapture.recording.events import decode_event

class RecordingInput:
    def __init__(self, path):
        self.path = str(path)
        self.line = 0
        self.closed = False
        self.started = False
        try:
            self.file = Path(path).open('rb')
        except OSError as exc:
            raise RecordingError(str(exc), SourceLocation(self.path, 1)) from exc
        try:
            raw = self._line()
            if raw is None:
                raise RecordingError('missing recording header', SourceLocation(self.path, 1))
            self.header = parse_header(json_object(raw, self.location), self.location)
        except BaseException:
            self.close()
            raise

    @property
    def location(self):
        return SourceLocation(self.path, self.line)

    def _line(self):
        try:
            raw = self.file.readline()
        except OSError as exc:
            raise RecordingError(str(exc), SourceLocation(self.path, self.line + 1)) from exc
        if not raw:
            return None
        self.line += 1
        try:
            raw.decode('utf-8')
        except UnicodeError as exc:
            raise RecordingError('invalid UTF-8', self.location) from exc
        return raw

    def events(self):
        if self.started or self.closed:
            raise RecordingError('recording iterator is single use', self.location)
        self.started = True
        try:
            while True:
                raw = self._line()
                if raw is None:
                    return
                evidence = make_evidence(self.path, self.line, raw)
                yield decode_event(json_object(raw, self.location), evidence)
        finally:
            self.close()

    def close(self):
        if not self.closed:
            self.closed = True
            self.file.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

def open_recording(path):
    return RecordingInput(path)
