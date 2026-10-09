"""Common tools for ipython."""


import os


def inside_ipython():
    """Checks whether we are inside an IPython environment.

    Returns
    -------
    bool
        True if the name ``__IPYTHON__`` is defined, which IPython (terminal, Jupyter, Colab, ...)
        injects into the builtins, and False in a plain Python interpreter

    Examples
    --------
    >>> from mt.ipython import inside_ipython
    >>> inside_ipython()
    False
    """
    try:
        __IPYTHON__
        return True
    except NameError:
        return False


def inside_sagemaker():
    return ("SAGEMAKER_LOG_FILE" in os.environ) or (
        "SAGEMAKER_LOGGING_DIR" in os.environ
    )


def get_ipython_type():
    """Returns which type of IPython we are in.

    The type is deduced from the class name of the active IPython shell and from the environment
    variables set by Amazon SageMaker.

    Returns
    -------
    {"ipython", "jupyter", "colab", "sagemaker", "sagipython", "ipython-others", None}
        the type of IPython we are in: "ipython" for a terminal IPython, "jupyter" for a Jupyter
        kernel, "colab" for Google Colab, "sagemaker" and "sagipython" for a Jupyter kernel or a
        terminal IPython running inside SageMaker, "ipython-others" for any other IPython shell,
        and None if we are not inside IPython at all

    Examples
    --------
    >>> from mt.ipython import get_ipython_type
    >>> get_ipython_type() is None  # when run in a plain Python interpreter
    True
    """

    if not inside_ipython():
        return None

    from IPython.core import getipython

    s = str(getipython.get_ipython())
    if "TerminalInteractiveShell" in s:
        return "sagipython" if inside_sagemaker() else "ipython"
    if "ZMQInteractiveShell" in s:
        return "sagemaker" if inside_sagemaker() else "jupyter"
    if "google.colab" in s:
        return "colab"
    return "ipython-others"
