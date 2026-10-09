'''Registry to store all the cast functions to cast an object from type A to type B.

Cast functions are registered with :func:`register_cast` (and, optionally, cheap
"is this castable?" checks with :func:`register_castable`) and are then looked up by
:func:`cast` and :func:`castable` using the exact type of the object and the destination type.
Subclasses are not matched: the lookup key is `(type(obj), dst_type)`.

Examples
--------
>>> from mt.base.casting import cast, castable, register_cast
>>> class Meters:
...     def __init__(self, value):
...         self.value = value
...
>>> class Feet:
...     def __init__(self, value):
...         self.value = value
...
>>> register_cast(Meters, Feet, lambda m: Feet(m.value * 3.28084))
>>> castable(Meters(10), Feet)
True
>>> castable(Feet(10), Meters)
False
>>> round(cast(Meters(10), Feet).value, 2)
32.81
>>> cast(Feet(1), Meters)
Traceback (most recent call last):
    ...
NotImplementedError: Cast function from type <class 'mt.base.casting.Feet'> to type ...
'''


__all__ = ['cast', 'castable', 'register_cast', 'register_castable']


# map: (src_type, dst_type) -> cast function
_cast_registry = {}

_castable_registry = {}
# map: (src_type, dst_type) -> castable function


def castable(obj, dst_type):
    '''Checks if an object can be cast to a target type.

    If a function has been registered with :func:`register_castable` for
    `(type(obj), dst_type)`, it is called with `obj` and its result is returned. Otherwise, the
    object is castable if and only if a cast function has been registered with
    :func:`register_cast` for that pair of types. Only the exact type of `obj` is considered, not
    its base classes.

    Parameters
    ----------
    obj : object
        object to be cast
    dst_type : type
        type or class to cast to

    Returns
    -------
    bool
        whether or not the object is castable to `dst_type`

    See Also
    --------
    cast : performs the cast
    register_castable : registers a custom castability check
    '''
    key = (type(obj), dst_type)
    if key in _castable_registry:
        return _castable_registry[key](obj)
    return key in _cast_registry


def cast(obj, dst_type):
    '''Casts an object to a target type.

    The cast function registered with :func:`register_cast` for `(type(obj), dst_type)` is called
    with `obj`. Only the exact type of `obj` is considered, not its base classes.

    Parameters
    ----------
    obj : object
        object to be cast
    dst_type : type
        type or class to cast to

    Returns
    -------
    object
        the cast version of `obj`

    Raises
    ------
    NotImplementedError
        if the cast function has not been registered using function :func:`register_cast`

    See Also
    --------
    castable : tells whether a cast is possible
    register_cast : registers a cast function
    '''
    key = (type(obj), dst_type)
    if key in _cast_registry:
        return _cast_registry[key](obj)
    raise NotImplementedError(
        f"Cast function from type {key[0]} to type {key[1]} not found."
    )


def register_cast(src_type, dst_type, func):
    '''Registers a function to cast an object from type A to type B.

    Parameters
    ----------
    src_type : type
        type or class to cast from
    dst_type : type
        type or class to cast to
    func : function
        cast function to register. It takes an instance of `src_type` as its only argument and
        returns an instance of `dst_type`.

    Raises
    ------
    SyntaxError
        if a cast function has already been registered for the same pair of types. Registrations
        cannot be replaced.

    See Also
    --------
    cast : uses the registered function
    '''

    key = (src_type, dst_type)
    if key in _cast_registry:
        raise SyntaxError(
            f"Cast function from type {src_type} to type {dst_type} has been registered before."
        )
    _cast_registry[key] = func


def register_castable(src_type, dst_type, func):
    '''Registers a function to check if we can cast an object from type A to type B.

    This is useful when castability depends on the value of the object and not only on its type.
    When no such function is registered, :func:`castable` falls back on whether a cast function has
    been registered with :func:`register_cast`.

    Parameters
    ----------
    src_type : type
        type or class to cast from
    dst_type : type
        type or class to cast to
    func : function
        castable function to register. It takes an instance of `src_type` as its only argument and
        returns a bool.

    Raises
    ------
    SyntaxError
        if a castable function has already been registered for the same pair of types

    Examples
    --------
    >>> from mt.base.casting import castable, register_cast, register_castable
    >>> class Text:
    ...     def __init__(self, s):
    ...         self.s = s
    ...
    >>> class Number:
    ...     def __init__(self, x):
    ...         self.x = x
    ...
    >>> register_cast(Text, Number, lambda t: Number(float(t.s)))
    >>> register_castable(Text, Number, lambda t: t.s.replace(".", "", 1).isdigit())
    >>> castable(Text("3.5"), Number)
    True
    >>> castable(Text("abc"), Number)
    False
    '''

    key = (src_type, dst_type)
    if key in _castable_registry:
        raise SyntaxError(
            f"Castable function from type {src_type} to type {dst_type} has been registered before."
        )
    _castable_registry[key] = func
