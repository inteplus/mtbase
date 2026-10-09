'''Utilities related to hashing.'''


__all__ = ['hash_int']


def hash_int(i):
    '''Knuth's cheap multiplicative integer hash.

    The input is multiplied by the constant 2654435761 and the result is masked with 0x7FFFFFFF,
    so the output is a deterministic, non-negative integer below 2**31 (that is, only 31 bits are
    kept, not 32). It is fast and spreads consecutive integers, but it is not a cryptographic hash.

    `Source <https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key>`_

    Parameters
    ----------
    i : int
        an integer to hash

    Returns
    -------
    int
        a hashed integer in the range [0, 2**31)

    Examples
    --------
    >>> from mt.base.hashing import hash_int
    >>> hash_int(0)
    0
    >>> hash_int(1)
    506952113
    >>> hash_int(2)
    1013904226
    '''
    return int(i*2654435761 & 0x7FFFFFFF)
