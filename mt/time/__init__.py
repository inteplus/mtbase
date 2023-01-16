"""Additional utitlities dealing with time.

Instead of:

.. code-block:: python

   import time

You do:

.. code-block:: python

   from mt import time

It will import the time package plus the additional stuff implemented here.

Please see Python package `time`_ for more details.

.. _time:
   https://docs.python.org/3/library/time.html
"""

import errno
from time import *


__all__ = ["sleep_until"]


_stages = [
    (10, "napped for 10 seconds"),
    (30, "napped for half a minute"),
    (60, "napped for 1 minute"),
    (300, "napped for 5 minutes"),
    (1800, "napped for 30 minutes"),
    (3600, "slept for 1 hour"),
    (18000, "slept for 5 hours"),
    (3600 * 12, "slept for half a day"),
    (3600 * 24, "slept for a day"),
    (3600 * 24 * 7, "unconsious for a week"),
]


def sleep_until(func, check_interval=1, timeout=60 * 60 * 24, logger=None):
    """Sleeps until function evaluates to true.

    Parameters
    ----------
    check_interval : float
        number of seconds between two checks, in seconds
    timeout : float
        number of seconds before timeout, in which case a TimeoutError is raised
    logger : logging.Logger or equivalent
        logging for debugging purposes
    """
    start_time = time()
    cur_stage = -1
    while True:
        if func():
            return
        sleep(check_interval)
        ellapsed_time = time() - start_time

        if ellapsed_time > timeout:
            raise TimeoutError(
                errno.ETIMEDOUT,
                "Slept for too long",
                "Function {} has stayed non-true for more than {} seconds (timeout at {} seconds).".format(
                    func, ellapsed_time, timeout
                ),
            )

        if logger:
            # find the new stage
            if ellapsed_time < _stages[0][0]:
                stage = -1
            else:
                for i, row in enumerate(_stages):
                    thresh, msg = row
                    if ellapsed_time >= thresh:
                        stage = i

            if cur_stage != stage:
                cur_stage = stage
                logger.debug(_stages[cur_stage][1])
