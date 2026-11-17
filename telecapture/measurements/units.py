UNIT_SCALES = {
    'temperature': {'mC': 1000, 'uC': 1},
    'pressure': {'Pa': 1000000, 'uPa': 1},
}

def canonical_unit(quantity):
    if quantity == 'temperature':
        return 'uC'
    if quantity == 'pressure':
        return 'uPa'
    raise ValueError('unsupported quantity')

def to_micro_units(quantity, unit, value):
    if not isinstance(quantity, str) or not isinstance(unit, str):
        raise ValueError('quantity and unit must be strings')
    if quantity not in UNIT_SCALES or unit not in UNIT_SCALES[quantity]:
        raise ValueError('unit is incompatible with quantity')
    if value is None:
        return None
    if type(value) is not int:
        raise ValueError('measurement value must be an integer or null')
    return value * UNIT_SCALES[quantity][unit]
