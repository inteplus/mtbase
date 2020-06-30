'''Some utilities to manipulate strings.'''


__all__ = ['straighten']


def straighten(s, length, align_left=True, delimiter=' '):
    '''Straighten a Unicode string to have a fixed length.

    :Parameters:
        s : str
            string to be trimmed
        length : int
            number of characters of the output string
        align_left : bool
            whether to align the string to the left (True) or to the right (False)
        delimiter : char
            the whitespace character to fill in if the length input string is shorter than the output length

    :Returns:
        retval : str
            straightened string

    :Examples:

    >>> from mt.base.str import straighten
    >>> straighten('Hello world', 7)
    'Hello …'
    >>> straighten('Hi there', 3, align_left=False)
    '…re'
    >>> straighten("Oh Carol!", 20, align_left=False)
    '           Oh Carol!'
    '''

    if not isinstance(s, str):
        raise ValueError("Input is not a string: {}".format(s))
    if length < 0:
        raise ValueError("Output length must be non-negative, received {}".format(length))
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError("Expecting delimiter to be a single character, but '{}' received.".format(delimiter))

    in_len = len(s)

    if in_len == length: # nothing to do
        return s

    # need to fill in delimiter
    if in_len < length:
        if align_left:
            return s + delimiter*(length-in_len)
        return delimiter*(length-in_len) + s

    # need to truncate with '\u2026' (horizontal lower dots)
    if length == 1:
        return '\u2026' # single-char case
    if align_left:
        return s[:length-1] + '\u2026'
    return '\u2026' + s[in_len-length+1:]
