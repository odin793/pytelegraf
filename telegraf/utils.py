def format_string(key):
    """Formats either measurement names, tag names or tag values.
    Measurement name and any optional tags separated by commas.
    Measurement names, tag keys, and tag values must escape any spaces,
    commas or equal signs using a backslash (\).
    For example: \ and \,.
    All tag values are stored as strings and should not be surrounded
    in quotes.
    """
    if isinstance(key, str):
        return key.replace(',', r'\,').replace(' ', r'\ ').replace('=', r'\=')
    return key


def format_value(value):
    """Integers are numeric values that do not include a decimal and are
    followed by a trailing i when inserted (e.g. 1i, 345i, 2015i, -10i).
    Note that all values must have a trailing i. If they do not they will
    be written as floats.

    Floats are numeric values that are not followed by a trailing i.(e.g. 1,
    1.0, -3.14, 6.0e5, 10).

    Boolean values indicate true or false. Valid boolean strings for line
    protocol are (t, T, true, True, TRUE, f, F, false, False and FALSE).

    Strings are text values. All string field values must be surrounded by
    double-quotes. If the string contains a double-quote, the
    double-quote must be escaped with a backslash, e.g. \".
    """

    value_type = type(value)
    if value_type is str:
        value = '"{0}"'.format(value.replace('"', r'\"').replace('\n', r'\n'))
    elif value_type is int:
        value = '{0}i'.format(value)
    else:
        value = str(value)

    return value
