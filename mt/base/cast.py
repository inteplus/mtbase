'''Registry to store all the cast functions to cast an object from type A to type B.'''


__all__ = ['cast', 'castable', 'register_cast', 'register_castable']


# map: (src_type, dst_type) -> cast function
_cast_registry = {}

_castable_registry = {}
# map: (src_type, dst_type) -> castable function


def castable(obj, dst_type):
    '''Checks if an object can be cast to a target type.

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
    '''
    key = (type(obj), dst_type)
    if key in _castable_registry:
        return _castable_registry[key](obj)
    return key in _cast_registry


def cast(obj, dst_type):
    '''Casts an object to a target type.

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
        if the cast function has not been registered using function `register_cast`
    '''
    key = (type(obj), dst_type)
    if key in _cast_registry:
        return _cast_registry[key](obj)
    raise NotImplementedError("Cast function from type {} to type {} not found.".format(key[0], key[1]))


def register_cast(src_type, dst_type, func):
    '''Registers a function to cast an object from type A to type B.

    Parameters
    ----------
    src_type : type
        type or class to cast from
    dst_type : type
        type or class to cast to
    func : function
        cast function to register
    '''

    key = (src_type, dst_type)
    if key in _cast_registry:
        raise SyntaxError("Cast function from type {} to type {} has been registered before.".format(src_type, dst_type))
    _cast_registry[key] = func


def register_castable(src_type, dst_type, func):
    '''registers a function to check if we can cast an object from type A to type B.

    Parameters
    ----------
    src_type : type
        type or class to cast from
    dst_type : type
        type or class to cast to
    func : function
        castable function to register
    '''

    key = (src_type, dst_type)
    if key in _castable_registry:
        raise SyntaxError("Castable function from type {} to type {} has been registered before.".format(src_type, dst_type))
    _castable_registry[key] = func
