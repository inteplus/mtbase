"""Useful subroutines dealing with S3 files via botocore and aioboto3."""

import errno
import asyncio
import aiobotocore
import aioboto3
import aioboto3.session
import boto3
import boto3.session
import botocore
import botocore.exceptions
from tqdm.auto import tqdm

from mt import ctx, aio, tp, logg
from mt.halo import Halo

from .http import create_http_session


__all__ = [
    "split",
    "join",
    "get_session",
    "create_s3_client",
    "create_context_vars",
    "list_objects",
    "list_object_info",
    "get_object",
    "get_object_acl",
    "put_object",
    "delete_object",
    "put_files",
    "put_files_boto3",
]


def join(bucket: str, prefix: tp.Optional[str] = None):
    """Joins a bucket and a prefix into an s3cmd url.

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
    """

    if prefix is None:
        return "s3://" + bucket
    return "s3://" + bucket + "/" + prefix


def split(s3cmd_url: str):
    """Splits an s3cmd url into bucket and prefix.

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
    """

    if not s3cmd_url.startswith("s3://"):
        raise ValueError(
            "Expected an s3cmd url in the form 's3://bucket[/prefix]', but received '{}' instead.".format(
                s3cmd_url
            )
        )
    url = s3cmd_url[5:]
    pos = url.find("/")
    if pos < 0:  # '/' not found
        bucket = url
        prefix = None
    else:
        bucket = url[:pos]
        prefix = url[pos + 1 :]
    return bucket, prefix


def get_session(
    profile=None, asyn: bool = True
) -> tp.Union[aioboto3.session.Session, boto3.session.Session]:
    """Gets a boto3 session, for either asynchronous mode or synchronous mode.

    Parameters
    ----------
    profile : str, optional
        the profile from which the session is created
    asyn : bool
        whether session is to be used asynchronously or synchronously

    Returns
    -------
    session: aioboto3.session.Session or boto3.session.Session
        In asynchronous mode, an aioboto3.session.Session instance is returned. In synchronous
        mode, a boto3.session.Session instance is returned.

    Notes
    -----
    This function is used as part of :func:`create_s3_client` to create an s3 client.
    """

    if asyn:
        return aioboto3.session.Session(profile_name=profile)
    return boto3.session.Session(profile_name=profile)


@ctx.asynccontextmanager
async def create_s3_client(
    profile=None, asyn: bool = True
) -> tp.Union[aiobotocore.client.AioBaseClient, botocore.client.BaseClient]:
    """An asyn context manager that creates an s3 client for a given profile.

    Parameters
    ----------
    profile : str, optional
        the profile from which the s3 client is created
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    s3_client : aiobotocore.client.AioBaseClient or botocore.client.BaseClient
        the s3 client that matches with the 'asyn' keyword argument below
    """
    session = get_session(profile=profile, asyn=asyn)
    config = botocore.config.Config(max_pool_connections=20)
    if asyn:
        async with session.client("s3", config=config) as s3_client:
            yield s3_client
    else:
        if isinstance(session, boto3.session.Session):
            yield session.client("s3", config=config)
        else:
            yield session.create_client("s3", config=config)


@ctx.asynccontextmanager
async def create_context_vars(
    profile=None,
    asyn: bool = False,
    logger: tp.Optional[logg.IndentedLoggerAdapter] = None,
):
    """Creates a dictionary of context variables for running functions in this package.

    Parameters
    ----------
    profile : str, optional
        one of the profiles specified in the AWS. The default is used if None is given.
    asyn : bool
        whether the functions are to be invoked asynchronously or synchronously
    logger : mt.logg.IndentedLoggerAdapter, optional
        logger for debugging purposes

    Returns
    -------
    context_vars : dict
        dictionary of context variables to run the functions in this package. These include
        's3_client' and 'http_session'.
    """

    try:
        async with create_s3_client(
            profile=profile, asyn=asyn
        ) as s3_client, create_http_session() as http_session:
            context_vars = {
                "async": asyn,
                "s3_client": s3_client,
                "http_session": http_session,
            }
            yield context_vars
    except botocore.exceptions.ConnectionError:
        if logger:
            if profile:
                msg = f"Exception caught while establishing a connection to S3 using AWS profile '{profile}':"
            else:
                msg = "Exception caught while establishing a connection to S3 using the default AWS profile:"
            logger.error(msg)
        raise


async def list_objects(s3cmd_url: str, show_progress=False, context_vars: dict = {}):
    """An asyn function that lists all objects prefixed with a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    show_progress : bool
        show a progress spinner in the terminal
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.

    Returns
    -------
    list
        list of records, each of which corresponds to an object prefixed with the given s3cmd url.
        The record has multiple attributes.
    """

    s3_client = context_vars["s3_client"]
    try:
        bucket, prefix = split(s3cmd_url)
        paginator = s3_client.get_paginator("list_objects_v2")
        retval = []
        if show_progress:
            spinner = Halo("listing objects at '{}'".format(s3cmd_url), spinner="dots")
            spinner.start()
        if context_vars["async"]:
            async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
                new_list = result.get("Contents", None)
                if new_list is None:
                    if not retval:
                        if show_progress:
                            spinner.succeed("no object found")
                        return []
                    raise ConnectionResetError(
                        errno.ECONNRESET,
                        "Unable to get all the records while listing objects.",
                        s3cmd_url,
                    )
                retval.extend(new_list)
        else:
            for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
                new_list = result.get("Contents", None)
                if new_list is None:
                    if not retval:
                        if show_progress:
                            spinner.succeed("no object found")
                        return []
                    raise ConnectionResetError(
                        errno.ECONNRESET,
                        "Unable to get all the records while listing objects.",
                        s3cmd_url,
                    )
                retval.extend(new_list)
        if show_progress:
            spinner.succeed("{} objects found".format(len(retval)))
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            if show_progress:
                spinner.succeed("no object found")
            return []
        raise
    return retval


async def list_object_info(s3cmd_url: str, context_vars: dict = {}):
    """An asyn function that lists basic information of the object at a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.

    Returns
    -------
    dict or None
        A dictionary of attributes related to the object, like 'Key', 'LastModified', 'ETag',
        'Size', 'StorageClass', etc. If the object does not exist, None is returned.
    """

    bucket, prefix = split(s3cmd_url)
    s3_client = context_vars["s3_client"]
    paginator = s3_client.get_paginator("list_objects_v2")
    try:
        if context_vars["async"]:
            async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
                new_list = result.get("Contents", None)
                if new_list is None:
                    break
                for item in new_list:
                    if item["Key"] == prefix:
                        return item
        else:
            for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
                new_list = result.get("Contents", None)
                if new_list is None:
                    break
                for item in new_list:
                    if item["Key"] == prefix:
                        return item
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return None


async def get_object(
    s3cmd_url: str, show_progress: bool = False, context_vars: dict = {}
):
    """An asyn function that gets the content of a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    show_progress : bool
        show a progress spinner in the terminal
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.

    Returns
    -------
    bytes
        the content of the given s3cmd url
    """

    bucket, prefix = split(s3cmd_url)
    s3_client = context_vars["s3_client"]
    if show_progress:
        spinner = Halo("getting object '{}'".format(s3cmd_url), spinner="dots")
        spinner.start()
    if context_vars["async"]:
        response = await s3_client.get_object(Bucket=bucket, Key=prefix)
        # this will ensure the connection is correctly re-used/closed
        async with response["Body"] as stream:
            data = await stream.read()
    else:
        response = s3_client.get_object(Bucket=bucket, Key=prefix)
        data = response["Body"].read()
    if show_progress:
        spinner.succeed("{} bytes downloaded".format(len(data)))
    return data


async def get_object_acl(s3cmd_url: str, context_vars: dict = {}):
    """An asyn function that gets the object properties of a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.

    Returns
    -------
    bytes
        the content of the given s3cmd url
    """

    bucket, prefix = split(s3cmd_url)
    s3_client = context_vars["s3_client"]
    if context_vars["async"]:
        response = await s3_client.get_object_acl(Bucket=bucket, Key=prefix)
    else:
        response = s3_client.get_object_acl(Bucket=bucket, Key=prefix)
    return response


async def put_object(
    s3cmd_url: str, data: bytes, show_progress: bool = False, context_vars: dict = {}
):
    """An asyn function that puts some content to given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    data: bytes
         the content to be uploaded
    show_progress : bool
        show a progress spinner in the terminal
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.

    Returns
    -------
    bytes
        the content of the given s3cmd url
    """

    bucket, prefix = split(s3cmd_url)
    s3_client = context_vars["s3_client"]
    if show_progress:
        spinner = Halo("putting object '{}'".format(s3cmd_url), spinner="dots")
        spinner.start()
    if context_vars["async"]:
        await s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
    else:
        s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
    if show_progress:
        spinner.succeed("{} bytes uploaded".format(len(data)))
    return data


async def delete_object(s3cmd_url: str, context_vars: dict = {}):
    """An asyn function that deletes a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket[/prefix]'
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.

    Returns
    -------
    list
        the response from S3 of the deletion operation. Lots of attributes expected.
    """

    bucket, prefix = split(s3cmd_url)
    s3_client = context_vars["s3_client"]
    if context_vars["async"]:
        response = await s3_client.delete_object(Bucket=bucket, Key=prefix)
    else:
        response = s3_client.delete_object(Bucket=bucket, Key=prefix)
    return response


async def put_files(
    bucket: str,
    filepath2key_map: dict,
    show_progress: bool = False,
    context_vars: dict = {},
):
    """An asyn function that uploads many files to the same S3 bucket.

    In asynchronous mode, the files are uploaded concurrently. In synchronous mode, the files are
    uploaded sequentially.

    Despite our best effort, this function may sometimes be slower than calling 'aws s3 sync'. Please
    see the following thread for more details:

    https://stackoverflow.com/questions/56639630/how-can-i-increase-my-aws-s3-upload-speed-when-using-boto3

    It is recommended to use :func:`put_files_boto3` in those cases.

    Parameters
    ----------
    bucket : str
        bucket name
    filepath2key_map : dict
        mapping from local filepath to bucket key, defining which file to upload and where to
        upload to in the S3 bucket
    show_progress : bool
        show a progress bar in the terminal
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.
    """

    async def process_item(
        filepath, bucket, key, progress_bar, context_vars: dict = {}
    ):
        s3cmd_url = join(bucket, key)
        data = await aio.read_binary(filepath, context_vars=context_vars)
        await put_object(
            s3cmd_url, data, show_progress=False, context_vars=context_vars
        )
        if isinstance(progress_bar, tqdm):
            progress_bar.update()

    with (
        tqdm(total=len(filepath2key_map), unit="file")
        if show_progress
        else ctx.nullcontext()
    ) as progress_bar:
        if context_vars["async"]:
            coros = [
                process_item(
                    filepath, bucket, key, progress_bar, context_vars=context_vars
                )
                for filepath, key in filepath2key_map.items()
            ]
            await asyncio.gather(*coros)
        else:
            for filepath, key in filepath2key_map.items():
                aio.srun(
                    process_item,
                    filepath,
                    bucket,
                    key,
                    progress_bar,
                    extra_context_vars=context_vars,
                )


def put_files_boto3(
    bucket: str,
    filepath2key_map: dict,
    show_progress: bool = False,
    total_filesize: tp.Optional[int] = None,
    set_acl_public_read: bool = False,
    checksum_algorithm: tp.Optional[str] = None,
    context_vars: dict = {},
):
    """Uploads many files to the same S3 bucket using boto3.

    This function implements the code in the url below. It does not use asyncio but it uses
    multi-threading.

    https://stackoverflow.com/questions/56639630/how-can-i-increase-my-aws-s3-upload-speed-when-using-boto3

    Parameters
    ----------
    bucket : str
        bucket name
    filepath2key_map : dict
        mapping from local filepath to bucket key, defining which file to upload and where to
        upload to in the S3 bucket
    show_progress : bool
        show a progress bar in the terminal
    total_filesize : int
        total size of all files in bytes, if you know. Useful for drawing a progress bar.
    set_acl_public_read : bool
        whether or not to set ACL public-read policy on the uploaded object(s)
    checksum_algorithm : str, optional
        if specified, the algorithm to generate a checksum. A good one is 'SHA256'.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.
    """

    from boto3.s3.transfer import (
        TransferConfig,
        create_transfer_manager,
        ProgressCallbackInvoker,
    )

    transfer_config = TransferConfig(use_threads=True, max_concurrency=20)
    s3_client = context_vars["s3_client"]
    s3t = create_transfer_manager(s3_client, transfer_config)

    extra_args = {}

    if set_acl_public_read:
        extra_args["ACL"] = "public-read"

    if checksum_algorithm is not None:
        extra_args["ChecksumAlgorithm"] = checksum_algorithm

    if len(extra_args) == 0:
        extra_args = None

    with (
        tqdm(total=total_filesize, unit="B", unit_scale=True)
        if show_progress
        else ctx.nullcontext()
    ) as progress_bar:
        for filepath, key in filepath2key_map.items():
            s3t.upload(
                filepath,
                bucket,
                key,
                extra_args=extra_args,
                subscribers=(
                    [ProgressCallbackInvoker(progress_bar.update)]
                    if show_progress
                    else None
                ),
            )
        s3t.shutdown()  # wait for all the upload tasks to finish
