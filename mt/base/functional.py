'''Utitilites related to composing funcions.'''


__all__ = ['on_list', 'join_funcs', 'iterator_as_generator']


def on_list(func):
    '''Turns a function that operates on each element at a time into a function that operates on a colletion of elements at a time. Can be used as a decorator.

    Parameters
    ----------
    func : function
        the function to build upon. Its expected form is `def func(x, *args, **kwargs) -> object`.

    Returns
    -------
    function
        a wrapper function `def func_on_list(list_x, *args, **kwargs) -> list_of_objects` that invokes `func(x, *args, **kwargs)` for each x in list_x.
    '''
    def func_on_list(list_x, *args, **kwargs):
        return [func(x, *args, **kwargs) for x in list_x]
    return func_on_list


def join_funcs(*funcs, left_to_right=True):
    '''Concatenates functions taking a single argument into a single function.

    Parameters
    ----------
    *funcs : list
        list of functions f1(x), f2(x), ..., fn(x)
    left_to_right : boolean
        whether to concatenate from left to right or from right to left.

    Returns
    -------
    func : function
        The function `f(x) = fn(...f2(f1(x))...)` if left_to_right is True, else `f(x) = f1(f2(...fn(x)...))`.
    '''

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
    '''Turns a Python iterator into a Python generator that generates items forever. Probably only useful for keras.'''

    def asgenerator(iterator):
        while True:
            yield next(iterator)

    return asgenerator(iterator)
    
