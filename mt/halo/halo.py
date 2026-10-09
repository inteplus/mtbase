# -*- coding: utf-8 -*-
# pylint: disable=unsubscriptable-object
"""Beautiful terminal spinners in Python.

Contains :class:`Halo`, a version of :class:`halo.Halo` whose constructor is rewritten so that it
registers its clean-up (stopping the spinner) with IPython's ``post_run_cell`` event or, in a
plain terminal, with :mod:`atexit`.
"""
from __future__ import absolute_import, unicode_literals

import atexit
import sys

from halo import Halo as HaloOld

from halo._utils import get_environment


class Halo(HaloOld):
    """Terminal spinner, a thin subclass of :class:`halo.Halo`.

    The spinner is typically used as a context manager (``with Halo(text="Loading"): ...``) or
    with ``start()`` / ``stop()`` / ``succeed()`` / ``fail()``, as in the parent class. On
    construction it registers a clean-up that stops the spinner when the interpreter exits (or
    when the current IPython/Jupyter cell finishes), so that the cursor is restored.

    It animates on a background thread and writes to a stream, so it is not meant for doctests or
    for capturing output. See :class:`HaloNotebook` for the Jupyter version.

    Attributes
    ----------
    CLEAR_LINE : str
        Code to clear the line
    """

    def __init__(
        self,
        text="",
        color="cyan",
        text_color=None,
        spinner=None,
        animation=None,
        placement="left",
        interval=-1,
        enabled=True,
        stream=sys.stdout,
    ):
        """Constructs the Halo object.

        Parameters
        ----------
        text : str, optional
            Text to display.
        color : str, optional
            Color of the spinner. Default is "cyan".
        text_color : str, optional
            Color of the text. Default is None, meaning the terminal's default color.
        spinner : str|dict, optional
            String or dictionary representing spinner. String can be one of 60+ spinners
            supported. If not provided, the default spinner of `halo` is used.
        animation: str, optional
            Animation to apply if text is too large. Can be one of `bounce`, `marquee`.
            Defaults to ellipses.
        placement: str, optional
            Side of the text to place the spinner on. Can be `left` or `right`.
            Defaults to `left`.
        interval : integer, optional
            Interval between each frame of the spinner in milliseconds. If not positive (the
            default, -1), the interval of the chosen spinner is used.
        enabled : boolean, optional
            Spinner enabled or not. A disabled spinner prints nothing.
        stream : io, optional
            Output. Default is `sys.stdout`.
        """
        self._color = color
        self._animation = animation

        self.spinner = spinner
        self.text = text
        self._text_color = text_color

        self._interval = (
            int(interval) if int(interval) > 0 else self._spinner["interval"]
        )
        self._stream = stream

        self.placement = placement
        self._frame_index = 0
        self._text_index = 0
        self._spinner_thread = None
        self._stop_spinner = None
        self._spinner_id = None
        self.enabled = enabled

        environment = get_environment()

        def clean_up(*args):
            """Handle cell execution"""
            self.stop()

        if environment in ("ipython", "jupyter"):
            from IPython import get_ipython

            ip = get_ipython()
            ip.events.register("post_run_cell", clean_up)
        else:  # default terminal
            atexit.register(clean_up)
