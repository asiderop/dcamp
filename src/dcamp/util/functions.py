from time import time


def now_secs():
    '''round down to most recent second -- designed for collection scheduling'''
    return int(time())


def now_msecs():
    '''round up/down to nearest millisecond -- designed for collection records'''
    return int(round(time() * 1000))


def isInstance_orNone(gvalue, gtype):
    '''returns whether given value is an instance of the given type or None'''
    return isinstance(gvalue, (gtype, type(None)))


def seconds_to_str(seconds):
    '''
    Method takes given number of seconds and determines how to best (concisely) represent
    it in a string.

        >>> seconds_to_str(90)
        '90s'

    @see str_to_seconds() does the exact opposite of this
    '''
    return '%ds' % seconds


def str_to_seconds(string):
    '''
    Method determines how time in given string is specified and returns an int value in
    seconds.

        >>> str_to_seconds('90s')
        90

    @see seconds_to_str() does the exact opposite of this

    valid time units:
        s -- seconds

    @todo add this to validation routine / issue #23
    '''
    valid_units = ['s']
    if string.endswith('s'):
        return int(string[:len(string) - 1])
    else:
        raise NotImplementedError('invalid time unit given--valid units: %s' % valid_units)


def plural(count, ending='s', word=None):
    '''return plural form of given word based on given count'''
    return (word or '') + (count != 1.0 and ending or '')


def format_bytes(given, use_short=True, num_or_suffix='both'):
    '''
    Converts given number of bytes (int) into a human-friendly format.

    Use num_or_suffix to dictate the output format:
      + "num"    --> returns given bytes as float (in possibly larger units)
      + "suffix" --> returns units of "num"
      + "both"   --> returns "%.2f%s" % (num, suffix)
    '''
    long_units = ['', 'Kilo', 'Mega', 'Giga', 'Tera', 'Peta']  # with 'byte' suffix
    short_units = ['B', 'Kb', 'Mb', 'Gb', 'Tb', 'Pb']

    assert isinstance(given, (int, float))
    assert num_or_suffix in ['num', 'suffix', 'both']

    num_orders = 0
    num = float(given)
    while round(num, 2) >= 1024.0:
        num_orders += 1
        num /= 1024.0

    suffix = None
    if use_short:
        suffix = short_units[num_orders]
    else:
        suffix = long_units[num_orders] + plural(num, word='byte')

    if 'num' == num_or_suffix:
        return num
    elif 'suffix' == num_or_suffix:
        return suffix
    else:
        return '{:.2f}{}'.format(num, use_short and suffix or ' ' + suffix)


def get_logger_from_caller(default):
    import inspect

    caller = inspect.stack()[2][0]  # we want this method's caller's caller
    if 'self' in caller.f_locals:
        return caller.f_locals['self'].logger

    return default
