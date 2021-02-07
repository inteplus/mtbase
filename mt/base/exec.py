'''Exec utils.'''


from .logging import logger


__all__ = ['debug_exec', 'debug_exec2']


def debug_exec(func, *args, **kwargs):
    '''Executes a function with trials.

    This function executes a function. However, when an exception is raised in the function, asks the user to invoke an IPython interactive shell or execute a Python command and retry. If the user declines, it passes the exception up in the call stack.

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
    user_data = {
        'func': func,
        'args': args,
        'kwargs': kwargs,
    }
    while True:
        try:
            return (user_data['func'])(*(user_data['args']), **(user_data['kwargs']))
            break
        except:
            logger.warn_last_exception()
            try:
                from IPython.terminal.embed import InteractiveShellEmbed
            except:
                InteractiveShellEmbed = None

            if InteractiveShellEmbed:
                cmd = input("To debug in IPython shell, enter 'y'. Otherwise, press ENTER to continue: ")
                if len(cmd) == 0:
                    raise
                InteractiveShellEmbed()()
            else:
                cmd = input("Enter a Python command to be executed before we retry, or press ENTER to continue: ")
                if len(cmd) == 0:
                    raise
                exec(cmd)

def debug_exec2(func, *args, **kwargs):
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
