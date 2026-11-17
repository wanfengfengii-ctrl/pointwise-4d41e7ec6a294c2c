from dataclasses import dataclass, field
from telecapture.recording.provenance import EventEvidence, RecordingError
from telecapture.recording.validation import (require_fields, require_integer,
    require_identifier, require_boolean, decode_payload, uint32)

@dataclass(frozen=True)
class IDataEvent:
    session_id: str
    tsn: int
    stream_id: int
    mid: int
    fsn: int
    begin: bool
    end: bool
    payload: bytes
    evidence: EventEvidence = field(default_factory=EventEvidence, compare=False)
    unordered: bool = False

    def __post_init__(self):
        loc = self.evidence.location
        require_identifier(self.session_id, 'session_id', loc)
        uint32(self.tsn, 'tsn', loc)
        uint32(self.mid, 'mid', loc)
        require_integer(self.stream_id, 'stream_id', loc, 0, 65535)
        require_integer(self.fsn, 'fsn', loc, 0)
        require_boolean(self.begin, 'begin', loc)
        require_boolean(self.end, 'end', loc)
        require_boolean(self.unordered, 'unordered', loc)
        if type(self.payload) is not bytes:
            raise RecordingError('payload must be immutable bytes', loc)

@dataclass(frozen=True)
class ForwardEntry:
    stream_id: int
    mid: int
    unordered: bool = False

    def __post_init__(self):
        require_integer(self.stream_id, 'stream_id', minimum=0, maximum=65535)
        uint32(self.mid, 'mid')
        require_boolean(self.unordered, 'unordered')

@dataclass(frozen=True)
class ForwardEvent:
    session_id: str
    cumulative_tsn: int
    entries: tuple
    evidence: EventEvidence = field(default_factory=EventEvidence, compare=False)

    def __post_init__(self):
        require_identifier(self.session_id, 'session_id', self.evidence.location)
        uint32(self.cumulative_tsn, 'cumulative_tsn', self.evidence.location)
        if type(self.entries) is not tuple or any(not isinstance(x, ForwardEntry) for x in self.entries):
            raise RecordingError('entries must be a tuple of ForwardEntry', self.evidence.location)

def decode_event(value, evidence=None, mixed=False):
    evidence = evidence or EventEvidence()
    loc = evidence.location
    if not isinstance(value, dict):
        raise RecordingError('event must be an object', loc)
    kind = value.get('type')
    if kind == 'I-DATA':
        require_fields(value, ('type', 'session_id', 'tsn', 'stream_id', 'mid',
                               'fsn', 'begin', 'end', 'payload_b64'), loc,
                       optional=('unordered',) if mixed else ())
        unordered = False
        if mixed:
            unordered = require_boolean(value.get('unordered', False), 'unordered', loc)
        return IDataEvent(value['session_id'], value['tsn'], value['stream_id'],
                          value['mid'], value['fsn'], value['begin'], value['end'],
                          decode_payload(value['payload_b64'], loc), evidence,
                          unordered=unordered)
    if kind == 'I-FORWARD-TSN':
        require_fields(value, ('type', 'session_id', 'cumulative_tsn', 'entries'), loc)
        if not isinstance(value['entries'], list):
            raise RecordingError('entries must be an array', loc)
        entries = []
        for item in value['entries']:
            require_fields(item, ('stream_id', 'mid'), loc,
                           optional=('unordered',) if mixed else ())
            stream = require_integer(item['stream_id'], 'stream_id', loc, 0, 65535)
            mid = uint32(item['mid'], 'mid', loc)
            unordered = False
            if mixed:
                unordered = require_boolean(item.get('unordered', False), 'unordered', loc)
            entries.append(ForwardEntry(stream, mid, unordered))
        return ForwardEvent(value['session_id'], value['cumulative_tsn'], tuple(entries), evidence)
    raise RecordingError('unsupported event type (ordered I-DATA only)', loc)
