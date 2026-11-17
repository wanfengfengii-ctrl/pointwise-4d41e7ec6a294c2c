SERIAL_MODULUS = 2**32
SERIAL_HALF = 2**31

def compare32(left, right):
    for value in (left, right):
        if type(value) is not int or not 0 <= value < SERIAL_MODULUS:
            raise ValueError('serial number must be uint32')
    delta = (left - right) % SERIAL_MODULUS
    if delta == SERIAL_HALF:
        raise ValueError('ambiguous half-range serial comparison')
    return 0 if delta == 0 else (1 if delta < SERIAL_HALF else -1)

def unwrap32(value, anchor):
    compare32(value, anchor % SERIAL_MODULUS)
    delta = (value - anchor) % SERIAL_MODULUS
    if delta >= SERIAL_HALF:
        delta -= SERIAL_MODULUS
    return anchor + delta

def next32(value):
    compare32(value, value)
    return (value + 1) % SERIAL_MODULUS
