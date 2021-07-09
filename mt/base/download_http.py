#!/usr/bin/env python3

'''Downloads from http with multiple connections'''

import sys
from time import sleep

from functools import partial
from itertools import count
from multiprocessing.dummy import Pool # use threads
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from threading import Lock, Semaphore
from .bg_invoke import BgInvoke


__all__ = ['download_http', 'download_http_single', 'download_http_with_file_size', 'download_http_without_file_size', 'request_file_size']


# ---------------------------------------------------------------------------------
# download with single connection
# ---------------------------------------------------------------------------------

def _download_chunk(url, byterange=None):
    '''Downloads a chunk of the remote file. Treat range error as EOF. For any other error, treat as exception.'''
    if byterange is not None:
        print("Chunk byterange {}".format(byterange))
        req = Request(url, headers=dict(Range='bytes=%d-%d' % byterange))
    else:
        req = Request(url)

    try:
        return urlopen(req).read()
    except HTTPError as e:
        if e.code == 416: # treat range error as EOF
            return b''
        return None
    except EnvironmentError:
        return None

def download_http_single(url, local_filepath):
    open(local_filepath, 'wb').write(_download_chunk(url))

# ---------------------------------------------------------------------------------
# without file size
# ---------------------------------------------------------------------------------

def download_http_without_file_size(url, local_filepath, num_conns=4, chunk_size=1 << 16):
    raise NotImplementedError("Crap, this is not working.")
    if num_conns==1:
        download_http_single(url, local_filepath)
        return

    pool = Pool(num_conns) # define number of concurrent connections
    ranges = zip(count(0, chunk_size), count(chunk_size - 1, chunk_size))
    with open(local_filepath, 'wb') as file:
        for s in pool.imap(partial(_download_chunk, url), ranges):
            if not s:
                break # error or EOF
            file.write(s)
            if len(s) != chunk_size:
                break  # EOF (servers with no Range support end up here)

# ---------------------------------------------------------------------------------
# with file size
# ---------------------------------------------------------------------------------

def download_http_with_file_size(url, local_filepath, file_size, max_num_conns=4, min_chunk_size=1 << 16):
    raise NotImplementedError("Crap, this is not working.")
    # check file size
    if file_size < 0:
        raise ValueError("Negative file size ({}) detected.".format(file_size))
    elif file_size == 0:
        open(local_filepath, 'wb')
        return

    # determine chunk size
    chunk_size = file_size / max_num_conns
    if chunk_size < min_chunk_size:
        chunk_size = min_chunk_size
    chunk_size = (chunk_size+32767) & (-32768) # make sure it is divisible by 32768

    # determine number of connections
    num_conns = (file_size+chunk_size-1)//chunk_size

    if num_conns==1:
        download_http_single(url, local_filepath)
        return

    futures = [None]*num_conns
    for id in range(num_conns):
        start = chunk_size*id
        end = min(start+chunk_size, file_size)-1
        futures[id] = BgInvoke(_download_chunk, url, (start, end))

    with open(local_filepath, 'wb') as f:
        for obj in futures:
            while obj.is_running():
                sleep(0.5)
            data = obj.result
            if not data: # error
                raise OSError("Error downloading from {} to {}".format(url, local_filepath))
            f.write(data)

# ---------------------------------------------------------------------------------
# main function
# ---------------------------------------------------------------------------------

def request_file_size(url):
    req = Request(url)
    req.get_method = lambda: 'HEAD'
    meta = urlopen(req).info()
    return meta.getheaders("Content-Length")[0]

def download_http(url, local_filepath, max_num_conns=4, min_chunk_size=1 << 16):
    '''TBC

    Note:
        Errors and exceptions are returned as exceptions.
    '''
    raise NotImplementedError("Crap, this is not working.")
    with download_http.lock: # only one invocation at a time
        # get the file size
        try:
            file_size = request_file_size(url)
        except:
            file_size = -1 # unable to obtain file size

        if file_size >= 0: # there is file size
            download_http_with_file_size(url, local_filepath, file_size, max_num_conns=max_num_conns, min_chunk_size=min_chunk_size)
        else:
            download_http_without_file_size(url, local_filepath, num_conns=max_num_conns, chunk_size=min_chunk_size)
download_http.lock = Lock()

