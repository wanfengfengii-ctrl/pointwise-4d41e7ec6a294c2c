from dataclasses import dataclass
from telecapture.association.context import AssociationError

@dataclass(frozen=True)
class StreamConfig:
    stream_id: int
    initial_mid: int

class StreamCatalog:
    def __init__(self, context):
        self.configs = {s: StreamConfig(s, m) for s, m in context.streams}

    def require_stream(self, stream_id, location=None):
        if stream_id not in self.configs:
            raise AssociationError(f'undeclared stream {stream_id}', location)
        return self.configs[stream_id]

    def validate_forward_entries(self, entries, location=None):
        for item in entries:
            self.require_stream(item.stream_id, location)

    def ordered_streams(self):
        return tuple(sorted(self.configs))
