"""Useful subroutines dealing with S3 files via botocore and aioboto3.

Most functions are coroutines that work in both asynchronous and synchronous modes, selected by
`context_vars['async']`, where `context_vars` is a dictionary created by
:func:`create_context_vars`. S3 locations are given as s3cmd urls of the form
's3://bucket[/prefix]'. They need valid AWS credentials and network access.

Examples
--------
The functions that talk to S3 cannot be demonstrated offline. Typical usage looks like this.

.. code-block:: python

    from mt.base import s3

    async def main():
        async with s3.create_context_vars(profile="myprofile", asyn=True) as context_vars:
            await s3.put_object("s3://mybucket/hello.txt", b"hi", context_vars=context_vars)
            data = await s3.get_object("s3://mybucket/hello.txt", context_vars=context_vars)
            records = await s3.list_objects("s3://mybucket/", context_vars=context_vars)
"""

import errno
import asyncio
import aiobotocore
import aioboto3
import aioboto3.session
import boto3
import boto3.session
import botocore
import botocore.exceptions
from datetime import datetime
from tqdm.auto import tqdm

from mt import ctx, aio, tp, logg
from mt.halo import HaloAuto

from .http import create_http_session


__all__ = [
    "split",
    "join",
    "get_session",
    "create_s3_client",
    "create_context_vars",
    "list_objects",
    "list_recent_objects",
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

    See Also
    --------
    split : the reverse operation

    Examples
    --------
    >>> from mt.base import s3
    >>> s3.join("mybucket", "folder/file.txt")
    's3://mybucket/folder/file.txt'
    >>> s3.join("mybucket")
    's3://mybucket'
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
    prefix : str or None
        prefix. It is None if the url has no slash after the bucket name, and an empty string if
        the url ends right after the slash.

    Raises
    ------
    ValueError
        if the url does not start with 's3://'

    See Also
    --------
    join : the reverse operation

    Examples
    --------
    >>> from mt.base import s3
    >>> s3.split("s3://mybucket/folder/file.txt")
    ('mybucket', 'folder/file.txt')
    >>> s3.split("s3://mybucket")
    ('mybucket', None)
    >>> s3.split("s3://mybucket/")
    ('mybucket', '')
    """

    if not s3cmd_url.startswith("s3://"):
        raise ValueError(
            "Expected an s3cmd url in the form 's3://bucket[/prefix]', "
            f"but received '{s3cmd_url}' instead."
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
        the profile from which the session is created. The default profile is used if None.
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

    The client allows up to 20 simultaneous connections. In asynchronous mode the client is closed
    when the `async with` statement ends. In synchronous mode a plain boto3 client is yielded.

    Parameters
    ----------
    profile : str, optional
        the profile from which the s3 client is created
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    s3_client : aiobotocore.client.AioBaseClient or botocore.client.BaseClient
        the s3 client that matches with the `asyn` keyword argument

    See Also
    --------
    create_context_vars : wraps this function and also creates an http session
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

    This is an asyn context manager, to be used with `async with`.

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
        'async' (the value of `asyn`), 's3_client' and 'http_session'. The http session is always
        an asynchronous :class:`aiohttp.ClientSession`, whatever the value of `asyn`.

    Raises
    ------
    botocore.exceptions.ConnectionError
        if the connection to S3 cannot be established. An error message is logged first if
        `logger` is provided.

    Examples
    --------
    .. code-block:: python

        async with create_context_vars(profile="myprofile", asyn=True) as context_vars:
            info = await list_object_info("s3://mybucket/key", context_vars=context_vars)
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
                msg = (
                    f"Exception caught while establishing a connection to S3 using AWS profile "
                    f"'{profile}':"
                )
            else:
                msg = (
                    "Exception caught while establishing a connection to S3 using the default "
                    "AWS profile:"
                )
            logger.error(msg)
        raise


async def list_objects(s3cmd_url: str, show_progress=False, context_vars: dict = {}):
    """An asyn function that lists all objects prefixed with a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket/[prefix]'. Note that there must be a slash after the
        bucket.
    show_progress : bool
        show a progress spinner in the terminal
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. The easiest way to get such a
        dictionary is :func:`create_context_vars`.

    Returns
    -------
    list
        list of records, each of which is a dictionary corresponding to an object prefixed with the
        given s3cmd url, with keys like 'Key', 'LastModified', 'ETag', 'Size' and 'StorageClass'.
        The list is empty if no object is found.

    Raises
    ------
    botocore.exceptions.ClientError
        if S3 reports an error other than a missing key
    ConnectionResetError
        if a page of results is unexpectedly empty after earlier pages were received

    See Also
    --------
    list_object_info : looks at a single object
    """

    s3_client = context_vars["s3_client"]
    try:
        bucket, prefix = split(s3cmd_url)
        paginator = s3_client.get_paginator("list_objects_v2")
        retval = []
        if show_progress:
            spinner = HaloAuto(
                f"listing objects at '{s3cmd_url}'", spinner="dots"
            )
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
            spinner.succeed(f"{len(retval)} objects found")
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            if show_progress:
                spinner.succeed("no object found")
            return []
        raise
    return retval


async def list_recent_objects(
    s3cmd_url: str, min_dt: datetime, show_progress=False, context_vars: dict = {}
):
    """An asyn function that lists all recent objects prefixed with a given s3cmd url.

    This is meant to be the same as :func:`list_objects`, but with objects modified at or after a
    certain datetime only.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket/[prefix]'. Note that there must be a slash after the
        bucket.
    min_dt : datetime.datetime
        minimum datetime of the objects to be listed. Must be a zone-aware datetime.
    show_progress : bool
        show a progress spinner in the terminal
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. The easiest way to get such a
        dictionary is :func:`create_context_vars`.

    Returns
    -------
    list
        list of records, each of which is a dictionary corresponding to an object prefixed with
        the given s3cmd url.

    Raises
    ------
    botocore.exceptions.ClientError
        if S3 reports an error other than a missing key

    Notes
    -----
    As currently implemented, the filtered iterator yields the matching object records themselves,
    yet each of them is then looked up for a 'Contents' key, which they do not have. As a result the
    function returns an empty list, both when it matches no object and when it matches some (it
    gives up at the first record). Use :func:`list_objects` and filter on 'LastModified'
    yourself until this is fixed.
    """

    s3_client = context_vars["s3_client"]
    try:
        bucket, prefix = split(s3cmd_url)
        paginator = s3_client.get_paginator("list_objects_v2")
        retval = []
        if show_progress:
            spinner = HaloAuto(
                f"listing objects at '{s3cmd_url}'", spinner="dots"
            )
            spinner.start()
        if context_vars["async"]:
            s3_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
            filtered_iterator = s3_iterator.search(
                f"Contents[?LastModified>=`{min_dt.isoformat()}`]"
            )
            async for result in filtered_iterator:
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
            s3_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
            filtered_iterator = s3_iterator.search(
                f"Contents[?LastModified>=`{min_dt.isoformat()}`]"
            )
            for result in filtered_iterator:
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
            spinner.succeed(f"{len(retval)} objects found")
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
        an s3cmd_url in the form 's3://bucket/prefix', where the prefix is the full key of the
        object
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. The easiest way to get such a
        dictionary is :func:`create_context_vars`.

    Returns
    -------
    dict or None
        A dictionary of attributes related to the object, like 'Key', 'LastModified', 'ETag',
        'Size', 'StorageClass', etc. If the object does not exist, None is returned.

    Raises
    ------
    botocore.exceptions.ClientError
        if S3 reports an error other than a missing key
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
        an s3cmd_url in the form 's3://bucket/key'
    show_progress : bool
        show a progress spinner in the terminal
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. The easiest way to get such a
        dictionary is :func:`create_context_vars`.

    Returns
    -------
    bytes
        the content of the given s3cmd url

    Raises
    ------
    botocore.exceptions.ClientError
        if S3 reports an error, for example when the key does not exist

    See Also
    --------
    put_object : the reverse operation
    """

    bucket, prefix = split(s3cmd_url)
    s3_client = context_vars["s3_client"]
    if show_progress:
        spinner = HaloAuto(f"getting object '{s3cmd_url}'", spinner="dots")
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
        spinner.succeed(f"{len(data)} bytes downloaded")
    return data


async def get_object_acl(s3cmd_url: str, context_vars: dict = {}):
    """An asyn function that gets the access control list (ACL) of the object at a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket/key'
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. The easiest way to get such a
        dictionary is :func:`create_context_vars`.

    Returns
    -------
    dict
        the response of S3 to the 'get_object_acl' request, with keys like 'Owner' and 'Grants'
        (it is not the content of the object)

    Raises
    ------
    botocore.exceptions.ClientError
        if S3 reports an error, for example when the key does not exist
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
    """An asyn function that puts some content to a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket/key'
    data : bytes
        the content to be uploaded
    show_progress : bool
        show a progress spinner in the terminal
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. The easiest way to get such a
        dictionary is :func:`create_context_vars`.

    Returns
    -------
    bytes
        the `data` that has been uploaded

    See Also
    --------
    get_object : the reverse operation
    put_files : uploads many files
    """

    bucket, prefix = split(s3cmd_url)
    s3_client = context_vars["s3_client"]
    if show_progress:
        spinner = HaloAuto(f"putting object '{s3cmd_url}'", spinner="dots")
        spinner.start()
    if context_vars["async"]:
        await s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
    else:
        s3_client.put_object(Bucket=bucket, Key=prefix, Body=data)
    if show_progress:
        spinner.succeed(f"{len(data)} bytes uploaded")
    return data


async def delete_object(s3cmd_url: str, context_vars: dict = {}):
    """An asyn function that deletes a given s3cmd url.

    Parameters
    ----------
    s3cmd_url : str
        an s3cmd_url in the form 's3://bucket/key'
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. The easiest way to get such a
        dictionary is :func:`create_context_vars`.

    Returns
    -------
    dict
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
    uploaded sequentially. Every file is read entirely in memory before it is uploaded.

    Despite our best effort, this function may sometimes be slower than calling 'aws s3 sync'.
    Please see the following thread for more details:

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
        show a progress bar in the terminal, counting files
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. The easiest way to get such a
        dictionary is :func:`create_context_vars`.

    See Also
    --------
    put_files_boto3 : a faster, multi-threaded alternative
    """

    async def process_item(
        filepath, bucket, key, progress_bar, context_vars: dict = {}
    ):
        """Reads one file and uploads it, then advances the progress bar if any."""

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
    multi-threading (up to 20 concurrent transfers) and blocks until all the uploads are done. It
    is a regular function and not a coroutine, and it needs a synchronous client, so create the
    context variables with `asyn=False`.

    https://stackoverflow.com/questions/56639630/how-can-i-increase-my-aws-s3-upload-speed-when-using-boto3

    Parameters
    ----------
    bucket : str
        bucket name
    filepath2key_map : dict
        mapping from local filepath to bucket key, defining which file to upload and where to
        upload to in the S3 bucket
    show_progress : bool
        show a progress bar in the terminal, counting bytes
    total_filesize : int, optional
        total size of all files in bytes, if you know. Useful for drawing a progress bar.
    set_acl_public_read : bool
        whether or not to set ACL public-read policy on the uploaded object(s)
    checksum_algorithm : str, optional
        if specified, the algorithm to generate a checksum. A good one is 'SHA256'.
    context_vars : dict
        a dictionary of context variables. Only variable 's3_client' is used, and it must hold a
        synchronous boto3 client, for example from :func:`create_context_vars` with `asyn=False`.

    See Also
    --------
    put_files : the asyn alternative
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
