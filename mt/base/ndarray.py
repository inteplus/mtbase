'''Useful functions dealing with numpy array.'''


import numpy as _np


__all__ = ['ndarray_repr']


def ndarray_repr(a):
    '''Gets a one-line representation string for a numpy array.

    Parameters
    ----------
    a : numpy.ndarray
        input numpy array

    Returns
    -------
    str
        a short representation string for the array
    '''
    if not isinstance(a, _np.ndarray):
        raise TypeError("An ndarray expected. Got '{}'.".format(type(a)))

    if a.size > 20:
        return "ndarray(shape={}, dtype={}, min={}, max={}, mean={}, std={})".format(a.shape, a.dtype, a.min(), a.max(), a.mean(), a.std())

    return "ndarray({}, dtype={})".format(repr(a.tolist()), a.dtype)
