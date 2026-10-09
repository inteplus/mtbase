"""Utilities related to composing functions."""

__all__ = ["on_list", "join_funcs", "iterator_as_generator"]


def on_list(func):
    """Turns a function that operates on one element at a time into a function that operates
    on a collection of elements at a time. Can be used as a decorator.

    Parameters
    ----------
    func : function
        the function to build upon. Its expected form is `def func(x, *args, **kwds) -> object`.

    Returns
    -------
    function
        a wrapper function `def func_on_list(list_x, *args, **kwds) -> list_of_objects` that
        invokes `func(x, *args, **kwds)` for each x in list_x and returns the results as a list. The
        wrapper does not preserve the name or docstring of `func`.

    Examples
    --------
    >>> from mt.base.functional import on_list
    >>> @on_list
    ... def scale(x, factor=1):
    ...     return x * factor
    ...
    >>> scale([1, 2, 3], factor=10)
    [10, 20, 30]
    """

    def func_on_list(list_x, *args, **kwds):
        return [func(x, *args, **kwds) for x in list_x]

    return func_on_list


def join_funcs(*funcs, left_to_right=True):
    """Concatenates functions taking a single argument into a single function.

    Parameters
    ----------
    *funcs : list
        list of functions f1(x), f2(x), ..., fn(x)
    left_to_right : bool
        whether to concatenate from left to right or from right to left.

    Returns
    -------
    func : function
        The function `f(x) = fn(...f2(f1(x))...)` if `left_to_right` is True, else
        `f(x) = f1(f2(...fn(x)...))`. With no functions, `f` is the identity.

    Examples
    --------
    >>> from mt.base.functional import join_funcs
    >>> add_one = lambda x: x + 1
    >>> double = lambda x: x * 2
    >>> join_funcs(add_one, double)(3)  # double(add_one(3))
    8
    >>> join_funcs(add_one, double, left_to_right=False)(3)  # add_one(double(3))
    7
    """

    def left_to_right_func(x):
        for f in funcs:
            x = f(x)
        return x

    def right_to_left_func(x):
        for f in reversed(funcs):
            x = f(x)
        return x

    return left_to_right_func if left_to_right else right_to_left_func


def iterator_as_generator(iterator):
    """Turns a Python iterator into a Python generator that generates items forever.

    Probably only useful for keras, whose training loops expect a generator that never ends.

    Parameters
    ----------
    iterator : iterator
        an iterator, that is, an object that supports :func:`next`. A plain iterable such as a list
        is not accepted.

    Returns
    -------
    generator
        a generator that yields `next(iterator)` at every step. Despite the name, the generator is
        only endless if the iterator is. When the iterator is exhausted, the generator raises
        :class:`RuntimeError` (PEP 479) instead of stopping silently.

    Examples
    --------
    >>> from mt.base.functional import iterator_as_generator
    >>> import itertools
    >>> g = iterator_as_generator(itertools.count(5))
    >>> next(g), next(g), next(g)
    (5, 6, 7)
    """

    def asgenerator(iterator):
        while True:
            yield next(iterator)

    return asgenerator(iterator)
