#!/usr/bin/python3

"""Useful module to debug a process remotely.

The process to be remotely debugged should import this module and invoke :func:`listen` during
startup. Then, from another terminal, invoke :func:`debug_process` with the PID of that process
as the argument, or run this module as a script (`python -m mt.base.debug_process PID`). As of
2020/11/20, as long as you import mt.base, this module is imported and registered automatically.

It works by signalling the process with SIGUSR1 and exchanging pickled messages through a pair of
named pipes in the temporary directory, so it only works on a posix system, and only if the
process's main thread registered the handler. The debugging prompt is a Python console that
executes commands in the frame that was running when the signal arrived.

Reference: https://code.activestate.com/recipes/576515/
"""


try:
    import readline  # For readline input support
except:
    pass

import sys, os, traceback, signal, codeop, tempfile
from io import StringIO
import _pickle as cPickle
import threading as _t


__all__ = ["listen", "debug_process"]


def pipename(pid):
    """Returns the base path of the pair of named pipes used to debug the process of a given PID.

    Parameters
    ----------
    pid : int
        process id

    Returns
    -------
    str
        path 'debug-<pid>' inside the temporary directory
    """
    return os.path.join(tempfile.gettempdir(), "debug-%d" % pid)


class NamedPipe(object):
    """A pair of named pipes (FIFOs) for two-way communication with another process.

    Objects are sent with :meth:`put` and received with :meth:`get`, marshalled with pickle.
    """

    def __init__(self, name, end=0, mode=0o666):
        """Opens a pair of pipes, `name`.in and `name`.out, for communication with another process.

        One process should pass 1 for `end`, and the other 0. Opening blocks until the other end has
        opened the pipes too.

        Parameters
        ----------
        name : str
            base path of the two pipes
        end : int
            0 for the process being debugged, 1 for the debugger
        mode : int
            permission bits used to create the FIFOs
        """
        self.in_name, self.out_name = (
            name + ".in",
            name + ".out",
        )
        try:
            os.mkfifo(self.in_name, mode)
        except OSError:
            pass
        try:
            os.mkfifo(self.out_name, mode)
        except OSError:
            pass

        # NOTE: The order the ends are opened in is important - both ends
        # of pipe 1 must be opened before the second pipe can be opened.
        if end:
            self.inp = open(self.out_name, "r")
            self.out = open(self.in_name, "w")
        else:
            self.out = open(self.out_name, "w")
            self.inp = open(self.in_name, "r")
        self._open = True

    def is_open(self):
        """Returns whether both pipes are still open."""

        return not (self.inp.closed or self.out.closed)

    def put(self, msg):
        """Sends an object to the other end.

        Raises
        ------
        Exception
            if the pipe has been closed
        """

        if self.is_open():
            data = cPickle.dumps(msg, 1)
            self.out.write("%d\n" % len(data))
            self.out.write(data.decode(encoding="iso-8859-1"))
            self.out.flush()
        else:
            raise Exception("Pipe closed")

    def get(self):
        """Receives an object from the other end.

        Returns
        -------
        object or None
            the object received, or None if the other end has closed the pipe
        """

        txt = self.inp.readline()
        if not txt:
            self.inp.close()
        else:
            l = int(txt)
            data = self.inp.read(l)
            if len(data) < l:
                self.inp.close()
            return cPickle.loads(
                data.encode(encoding="iso-8859-1")
            )  # Convert back to python object.

    def close(self):
        """Closes the pipes and removes the FIFO files."""

        self.inp.close()
        self.out.close()
        try:
            os.remove(self.in_name)
        except OSError:
            pass
        try:
            os.remove(self.out_name)
        except OSError:
            pass

    def __del__(self):
        self.close()


def remote_debug(sig, frame):
    """Signal handler that lets the process be remotely debugged.

    It opens the named pipes of the current process and runs a read-eval-print loop that executes
    the commands sent by :func:`debug_process` in the interrupted frame, returning the captured
    output. Exceptions in the loop are reported to the debugger and never reach the program.

    Parameters
    ----------
    sig : int
        the signal number, as in :func:`signal.signal`
    frame : frame
        the frame that was running when the signal arrived
    """

    def _raiseEx(ex):
        """Raise specified exception in the remote process"""
        _raiseEx.ex = ex

    _raiseEx.ex = None

    try:
        # Provide some useful functions.
        locs = {"_raiseEx": _raiseEx}
        locs.update(frame.f_locals)  # Unless shadowed.
        globs = frame.f_globals

        pid = os.getpid()  # Use pipe name based on pid
        pipe = NamedPipe(pipename(pid))

        old_stdout, old_stderr = sys.stdout, sys.stderr
        txt = ""
        pipe.put(
            "Interrupting process at following point:\n"
            + "".join(traceback.format_stack(frame))
            + ">>> "
        )

        try:
            while pipe.is_open() and _raiseEx.ex is None:
                line = pipe.get()
                if line is None:
                    continue  # EOF
                txt += line
                try:
                    code = codeop.compile_command(txt)
                    if code:
                        sys.stdout = StringIO()
                        sys.stderr = sys.stdout
                        exec(code, globs, locs)
                        txt = ""
                        pipe.put(sys.stdout.getvalue() + ">>> ")
                    else:
                        pipe.put("... ")
                except:
                    txt = ""  # May be syntax err.
                    sys.stdout = StringIO()
                    sys.stderr = sys.stdout
                    traceback.print_exc()
                    pipe.put(sys.stdout.getvalue() + ">>> ")
        finally:
            sys.stdout = old_stdout  # Restore redirected output.
            sys.stderr = old_stderr
            pipe.close()

    except Exception:  # Don't allow debug exceptions to propogate to real program.
        traceback.print_exc()

    if _raiseEx.ex is not None:
        raise _raiseEx.ex


def debug_process(pid):
    """Interrupts a running process and debugs it.

    The process must have invoked :func:`listen` (importing mt.base does it). The function sends
    SIGUSR1 to the process and then opens an interactive prompt. Each line typed is executed inside
    the process, in the frame that was running; enter EOF (Ctrl-D) to quit.

    Parameters
    ----------
    pid : int
        id of the process to debug
    """
    os.kill(pid, signal.SIGUSR1)  # Signal process.
    pipe = NamedPipe(pipename(pid), 1)
    try:
        while pipe.is_open():
            txt = input(pipe.get()) + "\n"
            pipe.put(txt)
    except EOFError:
        pass  # Exit.
    pipe.close()


def listen():
    """Registers the handler of SIGUSR1 that enables remote debugging of this process.

    It does nothing outside the main thread. On a non-posix system, a warning is logged instead
    because the feature is unavailable.
    """

    if _t.current_thread().__class__.__name__ != "_MainThread":
        return

    if os.name != "posix":
        from mt.logg import logger

        logger.warning(
            "Unable to register a pipe for remote-debugging the current process. "
            "The functionality only works in a posix system like Unix, Linux or Mac."
        )
    else:
        signal.signal(signal.SIGUSR1, remote_debug)  # Register for remote debugging.


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Error: Must provide process id to debug")
    else:
        pid = int(sys.argv[1])
        debug_process(pid)
