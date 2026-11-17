from dataclasses import dataclass
from hashlib import sha256

@dataclass(frozen=True)
class SourceLocation:
    path: str = '<api>'
    line: int = 0

    def __str__(self):
        return f'{self.path}:{self.line}'

@dataclass(frozen=True)
class EventEvidence:
    location: SourceLocation = SourceLocation()
    digest: str = ''

class RecordingError(ValueError):
    def __init__(self, message, location=None):
        self.location = location or SourceLocation()
        super().__init__(f'{self.location}: {message}')

def make_evidence(path, line, raw):
    data = raw.encode('utf-8') if isinstance(raw, str) else raw
    return EventEvidence(SourceLocation(str(path), line), sha256(data).hexdigest())
