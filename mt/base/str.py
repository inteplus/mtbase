"""Some utilities to manipulate strings."""

import re

__all__ = ["straighten", "text_filename"]


def straighten(s, length, align_left=True, delimiter=" "):
    """Straighten a Unicode string to have a fixed length.

    Parameters
    ----------
    s : str
        string to be trimmed
    length : int
        number of characters of the output string
    align_left : bool
        whether to align the string to the left (True) or to the right (False)
    delimiter : char
        the whitespace character to fill in if the length input string is shorter than the output length

    Returns
    -------
    retval : str
        straightened string

    Examples
    --------

    >>> from mt.base.str import straighten
    >>> straighten('Hello world', 7)
    'Hello …'
    >>> straighten('Hi there', 3, align_left=False)
    '…re'
    >>> straighten("Oh Carol!", 20, align_left=False)
    '           Oh Carol!'
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
        if the text contains a character that cannot be converted
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
    import numpy as np

    a = [1, -563, 126677, -14238565, 799468050, -17938206000]
    b = np.poly1d(a).roots
    c = np.round(b)
    return c.astype(int)
