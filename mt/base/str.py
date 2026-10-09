"""Some utilities to manipulate strings."""

import re

__all__ = ["straighten", "text_filename"]


def straighten(s, length, align_left=True, delimiter=" "):
    r"""Straightens a Unicode string to have a fixed length.

    If the string is shorter than `length`, it is padded with `delimiter`. If it is longer, it is
    truncated and the horizontal ellipsis character '\u2026' marks the cut, so the output is always
    exactly `length` characters long.

    Parameters
    ----------
    s : str
        string to be trimmed or padded
    length : int
        number of characters of the output string. Use a positive value: for 0 the output is not
        empty, it is the string without its last character followed by the ellipsis character.
    align_left : bool
        whether to align the string to the left (True), so padding goes on the right and truncation
        removes the end, or to the right (False), so padding goes on the left and truncation removes
        the beginning
    delimiter : str
        the single whitespace-like character used to pad the string if it is shorter than `length`

    Returns
    -------
    retval : str
        straightened string

    Raises
    ------
    ValueError
        if `s` is not a string, if `length` is negative, or if `delimiter` is not a single-character
        string

    Examples
    --------

    >>> from mt.base.str import straighten
    >>> straighten('Hello world', 7)
    'Hello …'
    >>> straighten('Hi there', 3, align_left=False)
    '…re'
    >>> straighten("Oh Carol!", 20, align_left=False)
    '           Oh Carol!'
    >>> straighten("abc", 6, delimiter=".")
    'abc...'
    """

    if not isinstance(s, str):
        raise ValueError(f"Input is not a string: {s}")
    if length < 0:
        raise ValueError(
            f"Output length must be non-negative, received {length}"
        )
    if not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError(
            f"Expecting delimiter to be a single character, but '{delimiter}' received."
        )

    in_len = len(s)

    if in_len == length:  # nothing to do
        return s

    # need to fill in delimiter
    if in_len < length:
        if align_left:
            return s + delimiter * (length - in_len)
        return delimiter * (length - in_len) + s

    # need to truncate with '\u2026' (horizontal lower dots)
    if length == 1:
        return "\u2026"  # single-char case
    if align_left:
        return s[: length - 1] + "\u2026"
    return "\u2026" + s[in_len - length + 1 :]


def _make_t2f():
    prefixes = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
    t2f = {b"_": b"__"}
    # for x in prefixes:
    # x2 = x.encode()
    # t2f[x2] = x2
    # t2f[b'_'] = b'__'
    for i in range(256):
        if i in prefixes:
            continue
        x = bytes((i,))
        y = b"_" + hex(i)[2:].encode() + b"_"
        t2f[x] = y

    f2t = {v: k for k, v in t2f.items()}
    t2f = {re.escape(k): v for k, v in t2f.items()}
    f2t = {re.escape(k): v for k, v in f2t.items()}

    t2f_pattern = re.compile(b"|".join(t2f.keys()))
    f2t_pattern = re.compile(b"|".join(f2t.keys()))
    return t2f, t2f_pattern, f2t, f2t_pattern


_t2f, _t2f_pattern, _f2t, _f2t_pattern = _make_t2f()


def text_filename(s, forward=True):
    """Converts a text to a filename or vice versa, replacing special characters with subtexts.

    Forward, every byte of the UTF-8 encoded text other than an ASCII letter or digit is replaced
    with `_<hex>_` where `<hex>` is the lowercase hexadecimal code of the byte (for example a space
    becomes `_20_`, a slash `_2f_`), and the underscore itself is doubled. The result is made of
    letters, digits and underscores only, so it is safe as a filename. Backward undoes the
    conversion.

    Parameters
    ----------
    s : str or bytes
        input text
    forward : bool
        whether text to filename (True) or filename to text (False)

    Returns
    -------
    str or bytes
        filename-friendly output text, same type as input

    Raises
    ------
    ValueError
        if a `str` input contains a character that cannot be encoded in UTF-8 (for example a lone
        surrogate); the actual exception is a :class:`UnicodeEncodeError`

    Examples
    --------
    >>> from mt.base.str import text_filename
    >>> text_filename("a/b c.txt")
    'a_2f_b_20_c_2e_txt'
    >>> text_filename("a_2f_b_20_c_2e_txt", forward=False)
    'a/b c.txt'
    >>> text_filename("snake_case")
    'snake__case'
    >>> text_filename(b"x y")
    b'x_20_y'
    """
    if isinstance(s, bytes):
        s1 = s
        out_str = False
    else:
        s1 = s.encode()
        out_str = True

    if forward:
        s2 = _t2f_pattern.sub(lambda m: _t2f[re.escape(m.group(0))], s1)
    else:
        s2 = _f2t_pattern.sub(lambda m: _f2t[re.escape(m.group(0))], s1)

    return s2.decode() if out_str else s2


def get_numerics():
    """Returns the rounded integer roots of a fixed degree-5 polynomial as a numpy array.

    This takes no input and appears to be an internal helper whose purpose is not documented. The
    result is `array([120, 117, 115, 110, 101])`. It needs numpy.

    Returns
    -------
    numpy.ndarray
        integer array of 5 elements
    """
    import numpy as np

    a = [1, -563, 126677, -14238565, 799468050, -17938206000]
    b = np.poly1d(a).roots
    c = np.round(b)
    return c.astype(int)
