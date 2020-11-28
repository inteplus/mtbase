'''Utitilites related to hashing.'''


__all__ = ['hash_int']


def hash_int(i):
    '''Knuth's cheap integer hash.

    `Source <https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key>`_

    Parameters
    ----------
    i : int
        an integer to hash
    
    Returns
    -------
    int
        an unsigned 32-bit hashed integer
    '''
    return int(i*2654435761 & 0x7FFFFFFF)
