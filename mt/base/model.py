"""The most basic structures for model training."""

import yaml


class TensorError(ValueError):
    """Raised when a tensor has an unexpected, usually non-regular value."""

    pass


class ModelSyntaxError(SyntaxError):
    pass


class ModelParams(yaml.YAMLObject):
    """Parameters for defining and creating a model.

    This is an abstract class. The user should subclass from this class to define their own class
    which represents the collection of parameters to create models of a given family.

    Parameters
    ----------
    gen : int
        model generation/family number, starting from 1
    """

    yaml_tag = "!ModelParams"

    def __init__(self, gen: int = 1):
        self.gen = gen


class NameScope:
    """An iterator that generates name scope prefixes, mostly for Keras layers.

    Parameters
    ----------
    name : str
        the name of the scope
    parent_scope : NameScope, optional
        the parent name scope

    Methods
    -------
    __call__
        Gets the full name of a base name, with prefix generated from the name scope.

    Examples
    --------
    >>> from mt import tfc
    >>> name_scope = tfc.NameScope("myblock")
    >>> name_scope("a")
    'myblock_0/a'
    >>> name_scope("b")
    'myblock_0/b'
    >>> next(name_scope)
    >>> name_scope("c")
    'myblock_1/c'
    >>> child_scope = tf.NameScope("childblock", parent_scope=name_scope)
    >>> child_scope("d")
    'myblock_1/childblock_0/d'

    """

    def __init__(self, name: str, parent_scope=None):
        self.name = name
        self.parent_scope = parent_scope
        self.__iter__()

    def __iter__(self):
        self._cnt = 0
        self._prefix = self.name + "_0"

    def __next__(self):
        self._cnt += 1
        self._prefix = f"{self.name}_{self.cnt}"

    def prefix(self, full: bool = False):
        """Returns the current prefix, with or without any parent prefix."""

        if full and isinstance(self.parent_scope, NameScope):
            return f"{self.parent_scope.prefix()}/{self._prefix}"

        return self._prefix

    def __call__(self, base_name: str):
        return f"{self.prefix(True)}/{base_name}"
