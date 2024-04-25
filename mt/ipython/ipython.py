"""Common tools for ipython."""


import os


def inside_ipython():
    """Checks whether we are inside an IPython environment."""
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

    Returns
    -------
    {"ipython", "jupyter", "colab", "sagemaker", "sagipython", "ipython-others", None}
        the type of IPython we are in
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
