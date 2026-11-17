import base64
import binascii
from telecapture.recording.provenance import RecordingError

def require_fields(value, required, location=None):
    if not isinstance(value, dict):
        raise RecordingError('expected JSON object', location)
    missing = set(required) - value.keys()
    extra = value.keys() - set(required)
    if missing or extra:
        raise RecordingError(f'fields missing={sorted(missing)} unknown={sorted(extra)}', location)
    return value

def require_integer(value, name, location=None, minimum=None, maximum=None):
    if type(value) is not int:
        raise RecordingError(f'{name} must be an integer', location)
    if (minimum is not None and value < minimum) or (maximum is not None and value > maximum):
        raise RecordingError(f'{name} outside supported range', location)
    return value

def require_identifier(value, name, location=None):
    if not isinstance(value, str) or not value.strip():
        raise RecordingError(f'{name} must be a nonempty string', location)
    return value

def require_boolean(value, name, location=None):
    if type(value) is not bool:
        raise RecordingError(f'{name} must be a boolean', location)
    return value

def decode_payload(value, location=None):
    if not isinstance(value, str):
        raise RecordingError('payload_b64 must be a string', location)
    try:
        return base64.b64decode(value.encode('ascii'), validate=True)
    except (ValueError, UnicodeError, binascii.Error) as exc:
        raise RecordingError('invalid base64 payload', location) from exc

def uint32(value, name, location=None):
    return require_integer(value, name, location, 0, 2**32 - 1)

def json_object(raw, location):
    import json
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f'duplicate JSON key {key}')
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=unique,
                          parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    except (ValueError, UnicodeError) as exc:
        raise RecordingError(f'invalid JSON: {exc}', location) from exc
