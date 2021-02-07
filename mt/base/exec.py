'''Exec utils.'''


from .logging import logger


__all__ = ['debug_exec']


def debug_exec(func, *args, **kwargs):
    '''Executes a function with trials.

    This function executes a function. However, when an exception is raised in the function, it asks the user if they want reload a module and try again, before actually passing the exception up in the call stack.

    Parameters
    ----------
    func : function
        function to be executed
    args : list
        positional arguments of the function
    kwargs : dict
        keyword arguments of the function

    Returns
    -------
    whatever the function returns
    '''
    while True:
        try:
            return func(*args, **kwargs)
            break
        except:
            logger.warn_last_exception()
            while True:
                module_name = input("Enter a module to be reloaded and retry, or press ENTER to continue: ")
                if len(module_name) == 0:
                    raise
                from sys import modules
                if module_name not in modules:
                    logger.warn("Module '{}' not yet loaded. Try again.".format(module_name))
                    continue
                import importlib
                importlib.reload(modules[module_name])
                break
