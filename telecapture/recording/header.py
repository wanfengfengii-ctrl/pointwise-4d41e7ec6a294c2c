import json
from dataclasses import dataclass
from telecapture.recording.validation import require_fields, require_integer, require_identifier, uint32
from telecapture.recording.provenance import RecordingError

@dataclass(frozen=True)
class RecordingHeader:
    recording_id: str
    gateway_id: str
    session_id: str
    initial_cumulative_tsn: int
    streams: tuple
    version: int = 1

def parse_header(value, location=None):
    require_fields(value, ('version', 'recording_id', 'gateway_id', 'session_id',
                           'initial_cumulative_tsn', 'streams'), location)
    if require_integer(value['version'], 'version', location) != 1:
        raise RecordingError('unsupported recording version', location)
    ids = [require_identifier(value[k], k, location)
           for k in ('recording_id', 'gateway_id', 'session_id')]
    initial = uint32(value['initial_cumulative_tsn'], 'initial_cumulative_tsn', location)
    if not isinstance(value['streams'], list):
        raise RecordingError('streams must be an array', location)
    streams = []
    seen = set()
    for item in value['streams']:
        require_fields(item, ('stream_id', 'initial_mid'), location)
        stream = require_integer(item['stream_id'], 'stream_id', location, 0, 65535)
        mid = uint32(item['initial_mid'], 'initial_mid', location)
        if stream in seen:
            raise RecordingError('duplicate stream declaration', location)
        seen.add(stream)
        streams.append((stream, mid))
    return RecordingHeader(*ids, initial, tuple(sorted(streams)))

def header_object(header):
    return dict(version=header.version, recording_id=header.recording_id,
                gateway_id=header.gateway_id, session_id=header.session_id,
                initial_cumulative_tsn=header.initial_cumulative_tsn,
                streams=[dict(stream_id=s, initial_mid=m) for s, m in header.streams])

def canonical_header(header):
    checked = parse_header(header_object(header))
    return json.dumps(header_object(checked), sort_keys=True, separators=(',', ':'))
