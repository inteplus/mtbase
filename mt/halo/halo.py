# -*- coding: utf-8 -*-
# pylint: disable=unsubscriptable-object
"""Beautiful terminal spinners in Python.
"""
from __future__ import absolute_import, unicode_literals

import atexit
import sys

from halo import Halo as HaloOld

from halo._utils import get_environment


class Halo(HaloOld):
    """Halo library.
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
        text_color : str, optional
            Color of the text.
        color : str, optional
            Color of the text to display.
        spinner : str|dict, optional
            String or dictionary representing spinner. String can be one of 60+ spinners
            supported.
        animation: str, optional
            Animation to apply if text is too large. Can be one of `bounce`, `marquee`.
            Defaults to ellipses.
        placement: str, optional
            Side of the text to place the spinner on. Can be `left` or `right`.
            Defaults to `left`.
        interval : integer, optional
            Interval between each frame of the spinner in milliseconds.
        enabled : boolean, optional
            Spinner enabled or not.
        stream : io, optional
            Output.
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
