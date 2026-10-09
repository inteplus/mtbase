from __future__ import absolute_import, print_function, unicode_literals

import sys
import threading

import halo.cursor as cursor

from .halo import Halo
from halo._utils import colored_frame, decode_utf_8_text


class HaloNotebook(Halo):
    def __init__(
        self,
        text="",
        color="cyan",
        text_color=None,
        spinner=None,
        placement="left",
        animation=None,
        interval=-1,
        enabled=True,
        stream=sys.stdout,
    ):
        super(HaloNotebook, self).__init__(
            text=text,
            color=color,
            text_color=text_color,
            spinner=spinner,
            placement=placement,
            animation=animation,
            interval=interval,
            enabled=enabled,
            stream=stream,
        )
        self.output = self._make_output_widget()

    def _make_output_widget(self):
        from ipywidgets.widgets import Output

        return Output()

    # TODO: using property and setter
    def _output(self, text=""):
        return ({"name": "stdout", "output_type": "stream", "text": text},)

    def clear(self):
        if not self.enabled:
            return self

        with self.output:
            self.output.outputs += self._output("\r")
            self.output.outputs += self._output(self.CLEAR_LINE)

        self.output.outputs = self._output()
        self.output.close()
        return self

    def _render_frame(self):
        frame = self.frame()
        output = f"\r{frame}"
        with self.output:
            self.output.outputs += self._output(output)

    def start(self, text=None):
        if text is not None:
            self.text = text

        if not self.enabled or self._spinner_id is not None:
            return self

        if self._stream.isatty():
            cursor.hide()

        self.output = self._make_output_widget()
        from IPython.display import display

        display(self.output)
        self._stop_spinner = threading.Event()
        self._spinner_thread = threading.Thread(target=self.render)
        self._spinner_thread.setDaemon(True)
        self._render_frame()
        self._spinner_id = self._spinner_thread.name
        self._spinner_thread.start()

        return self

    def stop_and_persist(self, symbol=" ", text=None):
        """Stops the spinner and persists the final frame to be shown.

        The final frame is written into the notebook's output widget as ``"<symbol> <text>"``
        (or ``"<text> <symbol>"`` if the spinner is placed on the right). It is the method behind
        ``succeed()``, ``fail()``, ``warn()`` and ``info()``.

        Parameters
        ----------
        symbol : str, optional
            Symbol to be shown in final frame. Default is a single space.
        text: str, optional
            Text to be shown in final frame. If None, the text the spinner was created with is
            used.

        Returns
        -------
        self
            the spinner object, to allow chaining. When the spinner is disabled, nothing is done.
        """
        if not self.enabled:
            return self

        symbol = decode_utf_8_text(symbol)

        if text is not None:
            text = decode_utf_8_text(text)
        else:
            text = self._text["original"]

        text = text.strip()

        if self._text_color:
            text = colored_frame(text, self._text_color)

        self.stop()

        output = "\r{} {}\n".format(
            *[(text, symbol) if self._placement == "right" else (symbol, text)][0]
        )

        with self.output:
            self.output.outputs = self._output(output)
