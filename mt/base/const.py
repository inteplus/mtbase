'''Some useful constants.

Attributes
----------
ansi_ul : str
    ANSI escape sequence that moves the terminal cursor up one line and to its beginning, handy
    for overwriting the previous line of output. On Windows, colorama needs to be initialised
    for it to work.
'''

__all__ = ['ansi_ul']

# useful ansi sequences. Needs colorama initialised.
ansi_ul = '\033[F' # move up one line and at the beginning
