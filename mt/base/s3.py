'''Useful subroutines dealing with S3 files via botocore and aiobotocore.'''


from typing import Optional, Union
import aiobotocore
import botocore
import botocore.session
from halo import Halo

#from .asyn import sleep, write_binary
#from .path import chmod


__all__ = ['split', 'join', 'get_session', 'list_objects']


def join(bucket: str, prefix: Optional[str] = None):
    '''Joins a bucket and a prefix into an s3cmd url.

    Parameters
    ----------
    bucket : str
        bucket name
    prefix : str, optional
        prefix string

    Returns
    -------
    str
        an s3cmd url in the form 's3://bucket_name[/prefix]'
    '''

    if prefix is None:
        return 's3://'+bucket
    return 's3://'+bucket+'/'+prefix


def split(s3cmd_url: str):
    '''Splits an s3cmd url into bucket and prefix.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd url in the form 's3://bucket_name[/prefix]'

    Returns
    -------
    bucket : str
        bucket name, always exists
    prefix : str
        prefix. Can be None.
    '''

    if not s3cmd_url.startswith('s3://'):
        raise ValueError(
            "Expected an s3cmd url in the form 's3://bucket[/prefix]', but received '{}' instead.".format(s3cmd_url))
    url = s3cmd_url[5:]
    pos = url.find('/')
    if pos < 0: # '/' not found
        bucket = url
        prefix = None
    else:
        bucket = url[:pos]
        prefix = url[pos+1:]
    return bucket, prefix


def get_session(profile = None, asyn: bool = True):
    '''Gets a botocore session, for either asynchronous mode or synchronous mode.

    Parameters
    ----------
    profile : str, optional
        the profile from which the session is created
    asyn : bool
        whether session is to be used asynchronously or synchronously

    Returns
    -------
    object
        In asynchronous mode, an aiobotocore.AioSession instance is returned. In synchronous mode,
        a botocore.session.Session instance is returned.

    Notes
    -----
    Please use the session's member function `create_client('s3')` to create an s3 client.
    '''

    klass = aiobotocore.AioSession if asyn else botocore.session.Session
    return klass(profile=profile)


async def list_objects(s3_client: Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient], s3cmd_url: str, show_progress=False, asyn: bool = True):
    '''An asyn function that lists all objects in a given bucket with optional prefix.

    Parameters
    ----------
    s3_client : aiobotocore.client.AioBaseClient or botocore.client.BaseClient
        the s3 client that matches with the 'asyn' keyword argument below
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    show_progress : bool
        show a progress spinner in the terminal
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    list
        list of records, each of which corresponds to an object matching the prefix. The record
        has multiple attributes.
    '''

    bucket, prefix = split(s3cmd_url)
    paginator = s3_client.get_paginator('list_objects_v2')
    retval = []
    if show_progress:
        spinner = Halo("listing objects at '{}'".format(s3cmd_url), spinner='dots')
        spinner.start()
    if asyn:
        async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            new_list = result.get('Contents', None)
            if new_list is None:
                raise IOError("Unable to get all the records while listing objects at '{}'.".format(s3cmd_url))
            retval.extend(new_list)
            if show_progress:
                spinner.succeed('{} objects found'.format(len(retval)))
    else:
        for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            new_list = result.get('Contents', None)
            if new_list is None:
                raise IOError("Unable to get all the records while listing objects at '{}'.".format(s3cmd_url))
            retval.extend(new_list)
            if show_progress:
                spinner.succeed('{} objects found'.format(len(retval)))
    return retval
