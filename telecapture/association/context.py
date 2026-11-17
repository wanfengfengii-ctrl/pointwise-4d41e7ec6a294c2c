from dataclasses import dataclass
from telecapture.recording.header import canonical_header, parse_header
import json

class AssociationError(ValueError):
    def __init__(self, message, location=None):
        self.location = location
        super().__init__(f'{location}: {message}' if location else message)

@dataclass(frozen=True)
class SessionContext:
    recording_id: str
    gateway_id: str
    session_id: str
    initial_cumulative_tsn: int
    streams: tuple
    canonical: str
    unordered_streams: tuple = ()

    @property
    def registration_key(self):
        return (self.recording_id, self.gateway_id, self.session_id)

    @property
    def unordered_initial_map(self):
        declared = dict(self.unordered_streams)
        return {stream: declared.get(stream, 0) for stream, _ in self.streams}

def make_context(header):
    canonical = canonical_header(header)
    h = parse_header(json.loads(canonical))
    return SessionContext(h.recording_id, h.gateway_id, h.session_id,
                          h.initial_cumulative_tsn, h.streams, canonical,
                          h.unordered_streams)
