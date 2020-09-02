'''Additional utitlities dealing with time.'''


from time import *


__all__ = ['sleep_until']


_stages = [
    (10, "napped for 10 seconds"),
    (30, "napped for half a minute"),
    (60, "napped for 1 minute"),
    (300, "napped for 5 minutes"),
    (1800, "napped for 30 minutes"),
    (3600, "slept for 1 hour"),
    (18000, "slept for 5 hours"),
    (3600*12, "slept for half a day"),
    (3600*24, "slept for a day"),
    (3600*24*7, "unconsious for a week"),
    ]


def sleep_until(func, check_interval=1, timeout=60*60*24, logger=None):
    '''Sleeps until function evaluates to true.

    Parameters
    ----------
    check_interval : float
        number of seconds between two checks, in seconds
    timeout : float
        number of seconds before timeout, in which case a RuntimeError is raised
    logger : logging.Logger or equivalent
        logging for debugging purposes
    '''
    start_time = time()
    cur_stage = -1
    while True:
        if func():
            return
        sleep(check_interval)
        ellaped_time = time()-start_time

        if ellaped_time > timeout:
            raise RuntimeError("Slept for too long.")

        if logger:
            # find the new stage
            if ellaped_time < _stages[0][0]:
                stage = -1
            else:
                for i, row in enumerate(_stages):
                    thresh, msg = row
                    if ellaped_time >= thresh:
                        stage = i

            if cur_stage != stage:
                cur_stage = stage
                logger.debug(_stages[cur_stage][1])
        
