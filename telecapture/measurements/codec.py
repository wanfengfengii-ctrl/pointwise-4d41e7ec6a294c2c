import json
from dataclasses import dataclass
from telecapture.measurements.units import canonical_unit, to_micro_units
from telecapture.measurements.schema import ArchiveError
from telecapture.recording.validation import json_object, require_fields, require_identifier, require_integer
from telecapture.recording.provenance import SourceLocation

@dataclass(frozen=True)
class DecodedMeasurement:
    row_number: int
    sensor_id: str
    time_us: int
    quantity: str
    unit: str
    value_micro: object
    quality: str

@dataclass(frozen=True)
class DecodedMessage:
    message: object
    measurements: tuple

def decode_message(message):
    try:
        value = json_object(message.body.decode('utf-8'), SourceLocation('<payload>', 1))
        require_fields(value, ('version', 'measurements'))
        if require_integer(value['version'], 'version') != 1:
            raise ValueError('unsupported payload version')
        if not isinstance(value['measurements'], list):
            raise ValueError('measurements must be an array')
        rows = []
        for index, row in enumerate(value['measurements']):
            require_fields(row, ('sensor_id', 'time_us', 'quantity', 'unit', 'value', 'quality'))
            sensor = require_identifier(row['sensor_id'], 'sensor_id')
            time_us = require_integer(row['time_us'], 'time_us')
            quality = row['quality']
            if quality not in ('good', 'suspect', 'missing'):
                raise ValueError('unsupported quality')
            if (quality == 'missing') != (row['value'] is None):
                raise ValueError('quality and null value disagree')
            converted = to_micro_units(row['quantity'], row['unit'], row['value'])
            rows.append(DecodedMeasurement(index, sensor, time_us, row['quantity'],
                                            canonical_unit(row['quantity']), converted, quality))
        return DecodedMessage(message, tuple(rows))
    except (ValueError, TypeError, UnicodeError, KeyError, AttributeError) as exc:
        raise ArchiveError('payload', str(exc)) from exc

def decode_batch(messages):
    return tuple(decode_message(message) for message in messages)

def storage_row(row):
    return (row.row_number, row.sensor_id, str(row.time_us), row.quantity, row.unit,
            None if row.value_micro is None else str(row.value_micro), row.quality)
