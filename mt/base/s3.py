'''Useful subroutines dealing with S3 files via botocore and aiobotocore.'''


from typing import Optional, Union
import asyncio
import aiobotocore
import botocore
import botocore.session
import botocore.exceptions
from halo import Halo
from tqdm import tqdm

from .asyn import read_binary, srun
from .with_utils import dummy_scope


__all__ = ['split', 'join', 'get_session', 'create_s3_client', 'list_objects', 'get_object', 'get_object_acl', 'put_object', 'delete_object', 'put_files', 'put_files_boto3']


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


def get_session(profile = None, asyn: bool = True) -> Union[aiobotocore.AioSession, botocore.session.Session]:
    '''Gets a botocore session, for either asynchronous mode or synchronous mode.

    Parameters
    ----------
    profile : str, optional
        the profile from which the session is created
    asyn : bool
        whether session is to be used asynchronously or synchronously

    Returns
    -------
    botocore_session: aiobotocore.AioSession or botocore.session.Session
        In asynchronous mode, an aiobotocore.AioSession instance is returned. In synchronous mode,
        a botocore.session.Session instance is returned.

    Notes
    -----
    Please use :func:`create_s3_client` to create an s3 client.
    '''

    klass = aiobotocore.AioSession if asyn else botocore.session.Session
    return klass(profile=profile)


def create_s3_client(botocore_session: Union[aiobotocore.AioSession, botocore.session.Session]) -> Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient]:
    '''Creates an s3 client for the botocore session, maximizing the number of connections.

    Parameters
    ----------
    botocore_session: aiobotocore.AioSession or botocore.session.Session
        In asynchronous mode, an aiobotocore.AioSession instance is returned. In synchronous mode,
        a botocore.session.Session instance is returned.

    Returns
    -------
    s3_client : aiobotocore.client.AioBaseClient or botocore.client.BaseClient
        the s3 client that matches with the 'asyn' keyword argument below
    '''
    config = botocore.config.Config(max_pool_connections=20)
    return botocore_session.create_client('s3', config=config)


async def list_objects(s3_client: Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient], s3cmd_url: str, show_progress=False, asyn: bool = True):
    '''An asyn function that lists all objects prefixed with a given s3cmd url.

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
        list of records, each of which corresponds to an object prefixed with the given s3cmd url.
        The record has multiple attributes.
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
    else:
        for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
            new_list = result.get('Contents', None)
            if new_list is None:
                raise IOError("Unable to get all the records while listing objects at '{}'.".format(s3cmd_url))
            retval.extend(new_list)
    if show_progress:
        spinner.succeed('{} objects found'.format(len(retval)))
    return retval


async def get_object(s3_client: Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient], s3cmd_url: str, show_progress: bool = False, asyn: bool = True):
    '''An asyn function that gets the content of a given s3cmd url.

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
    bytes
        the content of the given s3cmd url
    '''

    bucket, prefix = split(s3cmd_url)
    if show_progress:
        spinner = Halo("getting object '{}'".format(s3cmd_url), spinner='dots')
        spinner.start()
    if asyn:
        response = await s3_client.get_object(Bucket=bucket, Key=prefix)
        # this will ensure the connection is correctly re-used/closed
        async with response['Body'] as stream:
            data = await stream.read()
    else:
        response = s3_client.get_object(Bucket=bucket, Key=prefix)
        data = response['Body'].read()
    if show_progress:
        spinner.succeed('{} bytes downloaded'.format(len(data)))
    return data


async def get_object_acl(s3_client: Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient], s3cmd_url: str, asyn: bool = True):
    '''An asyn function that gets the object properties of a given s3cmd url.

    Parameters
    ----------
    s3_client : aiobotocore.client.AioBaseClient or botocore.client.BaseClient
        the s3 client that matches with the 'asyn' keyword argument below
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    bytes
        the content of the given s3cmd url
    '''

    bucket, prefix = split(s3cmd_url)
    if asyn:
        response = await s3_client.get_object_acl(Bucket=bucket, Key=prefix)
    else:
        response = s3_client.get_object_acl(Bucket=bucket, Key=prefix)
    return response


async def put_object(s3_client: Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient], s3cmd_url: str, data: bytes, show_progress: bool = False, asyn: bool = True):
    '''An asyn function that puts some content to given s3cmd url.

    Parameters
    ----------
    s3_client : aiobotocore.client.AioBaseClient or botocore.client.BaseClient
        the s3 client that matches with the 'asyn' keyword argument below
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    data: bytes
         the content to be uploaded
    show_progress : bool
        show a progress spinner in the terminal
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    bytes
        the content of the given s3cmd url
    '''

    bucket, prefix = split(s3cmd_url)
    if show_progress:
        spinner = Halo("putting object '{}'".format(s3cmd_url), spinner='dots')
        spinner.start()
    if asyn:
        await s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
    else:
        s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
    if show_progress:
        spinner.succeed('{} bytes uploaded'.format(len(data)))
    return data


async def delete_object(s3_client: Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient], s3cmd_url: str, asyn: bool = True):
    '''An asyn function that deletes a given s3cmd url.

    Parameters
    ----------
    s3_client : aiobotocore.client.AioBaseClient or botocore.client.BaseClient
        the s3 client that matches with the 'asyn' keyword argument below
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    list
        the response from S3 of the deletion operation. Lots of attributes expected.
    '''

    bucket, prefix = split(s3cmd_url)
    if asyn:
        response = await s3_client.delete_object(Bucket=bucket, Key=prefix)
    else:
        response = s3_client.delete_object(Bucket=bucket, Key=prefix)
    return response


async def put_files(s3_client: Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient], bucket: str, filepath2key_map: dict, show_progress: bool = False, asyn: bool = True):
    '''An asyn function that uploads many files to the same S3 bucket.

    In asynchronous mode, the files are uploaded concurrently. In synchronous mode, the files are
    uploaded sequentially.

    Despite our best effort, this function may sometimes be slower than calling 'aws s3 sync'. Please
    see the following thread for more details:

    https://stackoverflow.com/questions/56639630/how-can-i-increase-my-aws-s3-upload-speed-when-using-boto3

    It is recommended to use :func:`put_files_boto3` in those cases.

    Parameters
    ----------
    s3_client : aiobotocore.client.AioBaseClient or botocore.client.BaseClient
        the s3 client that matches with the 'asyn' keyword argument below
    bucket : str
        bucket name
    filepath2key_map : dict
        mapping from local filepath to bucket key, defining which file to upload and where to
        upload to in the S3 bucket
    show_progress : bool
        show a progress bar in the terminal
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    '''

    async def process_item(filepath, s3_client, bucket, key, progress_bar, asyn: bool = True):
        s3cmd_url = join(bucket, key)
        data = await read_binary(filepath, asyn=asyn)
        await put_object(s3_client, s3cmd_url, data, show_progress=False, asyn=asyn)
        if isinstance(progress_bar, tqdm):
            progress_bar.update()

    with tqdm(total=len(filepath2key_map), unit='file') if show_progress else dummy_scope as progress_bar:
        if asyn:
            coros = [process_item(filepath, s3_client, bucket, key, progress_bar) for filepath, key in filepath2key_map.items()]
            await asyncio.gather(*coros)
        else:
            for filepath, key in filepath2key_map.items():
                srun(process_item, filepath, s3_client, bucket, key, progress_bar)


def put_files_boto3(s3_client: botocore.client.BaseClient, bucket: str, filepath2key_map: dict, show_progress: bool = False):
    '''Uploads many files to the same S3 bucket using boto3.

    This function implements the code in the url below. It does not use asyncio but it uses
    multi-threading.

    https://stackoverflow.com/questions/56639630/how-can-i-increase-my-aws-s3-upload-speed-when-using-boto3

    Parameters
    ----------
    s3_client : botocore.client.BaseClient
        the (synchronous) s3 client return from :func:`create_s3_client`
    bucket : str
        bucket name
    filepath2key_map : dict
        mapping from local filepath to bucket key, defining which file to upload and where to
        upload to in the S3 bucket
    show_progress : bool
        show a progress bar in the terminal
    '''

    import boto3.s3.transfer as s3transfer
    transfer_config = s3transfer.TransferConfig(use_threads=True, max_concurrency=20)
    s3t = s3transfer.create_transfer_manager(s3_client, transfer_config)

    with tqdm(unit='byte') if show_progress else dummy_scope as progress_bar:
        for filepath, key in filepath2key_map.items():
            s3t.upload(
                filepath, bucket, key,
                subscribers = [s3transfer.ProgressCallbackInvoker(progress_bar.update)],
            )
        s3t.shutdown() # wait for all the upload tasks to finish
