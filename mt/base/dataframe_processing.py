"""Batch processing a dataframe."""

import typing as tp

import numpy as np
import pandas as pd

from tqdm import tqdm
import multiprocessing as mp
import queue

from mt import logging

from .hashing import hash_int
from .concurrency import beehive


__all__ = [
    "default_preprocess",
    "default_batchprocess",
    "default_postprocess",
    "sample_rows",
    "process_dataframe",
]


async def default_preprocess(
    s: pd.Series, iter_id: int, rng_seed: int, *args, context_vars: dict = {}, **kwargs
):
    """Preprocesses a row in the dataframe.

    The returning value should be the result of preprocessing the row or, in case some caching
    is used, even the result of postprocessing the row, bypassing the batch-processing step.

    The user should override the function to customise the behaviour. The default is to raise
    a NotImplementedError exception.

    Parameters
    ----------
    s : pandas.Series
        the row that has been selected for preprocessing
    iter_id : int
        iteration id
    rng_seed : int
        a number that can be used to seed an RNG for preprocessing `s`
    args : tuple
        additional positional arguments
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        Variable 's3_client' must exist and hold an enter-result of an async with statement
        invoking :func:`mt.base.s3.create_s3_client`. In asynchronous mode, variable
        'http_session' must exist and hold an enter-result of an async with statement invoking
        :func:`mt.base.http.create_http_session`.
    kwargs : dict
        additional keyword arguments

    Returns
    -------
    dict or pandas.Series
        The output can be a dict signalling the result of the preprocessing step or a pandas.Series
        signalling the result of even postprocessing the row, bypassing the batch-processing step.
        If it is a dict, each key is a tensor name and its value is a numpy array representing the
        tensor value. If it is a pandas.Series, it is the output series corresponding to the input
        series `s`.
    """
    raise NotImplementedError("This function serves only to show the API.")


async def default_batchprocess(
    batch_tensor_dict: dict, *args, context_vars: dict = {}, **kwargs
):
    """Processes batch tensors.

    The returning value should be the result of batchprocessing a collection of batch tensors or,
    in case some caching is used, even the result of postprocessing the batch of rows, bypassing
    the batch-processing step.

    The user should override the function to customise the behaviour. The default is to raise
    a NotImplementedError exception.

    Parameters
    ----------
    batch_tensor_dict : dict
        a dictionary of tensors extracted from `s`. Each key is a tensor name. Each value is a
        numpy array with the first dimension being the item id, or a list of objects,
        representing the content of the tensor. In any case, its size equals the size of
        `iter_id_list`.
    args : tuple
        additional positional arguments
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        Variable 's3_client' must exist and hold an enter-result of an async with statement
        invoking :func:`mt.base.s3.create_s3_client`. In asynchronous mode, variable
        'http_session' must exist and hold an enter-result of an async with statement invoking
        :func:`mt.base.http.create_http_session`.
    kwargs : dict
        additional keyword arguments

    Returns
    -------
    dict or list
        If a dictionary is returned, it is a dictionary of output batch tensors. Each key is a
        tensor name. Each value is a numpy array with the first dimension being the item id, or a
        list of objects, representing the content of the tensor. In any case, it has the same
        number of items as that of `batch_tensor_dict`. If a list is returned, it is a list of
        :class:`pandas.Series` instances representing the output :class:`pandas.Series` of each
        input item/row, after even the postprocessing step.
    """
    raise NotImplementedError("This function serves only to show the API.")


async def default_postprocess(
    tensor_dict: dict, *args, context_vars: dict = {}, **kwargs
):
    """Post-processes output tensors obtained from a row.

    The user should override the function to customise the behaviour. The default is to turn the
    input tensor dict into a :class:`pandas.Series` instance.

    Parameters
    ----------
    tensor_dict : dict
        a dictionary of output tensors from a batch procession. Each key is a tensor name. Each
        value is a numpy array representing the content of the tensor. The first dimension of a
        tensor no longer represents the items of the batch.
    args : tuple
        additional positional arguments
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        Variable 's3_client' must exist and hold an enter-result of an async with statement
        invoking :func:`mt.base.s3.create_s3_client`. In asynchronous mode, variable
        'http_session' must exist and hold an enter-result of an async with statement invoking
        :func:`mt.base.http.create_http_session`.
    kwargs : dict
        additional keyword arguments

    Returns
    -------
    pandas.Series
        A series of a fixed number of fields representing the output fields of the item.
    """
    return pd.Series(data=tensor_dict)


def sample_rows(
    df: pd.DataFrame,
    rng_seed: int = 0,
    num_iters: tp.Optional[int] = None,
    iter_policy: str = "sequential",
    resampling_col: tp.Optional[str] = None,
):
    """Sample the rows of a dataframe, returning `(row_id, rng_seed)` pairs.

    Parameters
    ----------
    df : pandas.DataFrame
        an input unindexed dataframe
    rng_seed : int, optional
        seed for making RNGs
    num_iters : int, optional
        number of iterations or equivalently number of rows selected during the call. If not
        provided, it is set to the number of rows of `df`.
    iter_policy : {'sequential', 'resampling'}
        policy for iterating the rows. If 'sequential' is given, the items are iterated
        sequentially from the first row to the last row and then back to the first row if required.
        If 'resampling' is given, then the dataframe column provided by the `resampling_col`
        argument provides the resampling weights. Rows are resampled randomly using these weights
        as the resampling distribution.
    resampling_col : str
        name of the column/field containing the resampling weights. Only valid if `iter_policy` is
        'resampling'.

    Returns
    -------
    list
        list of `(row_id, rng_seed)` pairs sampled from the dataframe
    """

    rng_seed = hash_int(rng_seed)  # to make it tougher to err
    num_rows = len(df)

    if num_iters is None:
        num_iters = num_rows

    if iter_policy == "sequential":
        index_array = [x % num_rows for x in range(num_iters)]
    elif iter_policy == "resampling":
        weight_list = np.array(df[resampling_col].tolist())
        weight_list /= weight_list.sum() + 1e-8  # to make a pmf
        rng = np.random.RandomState(seed=rng_seed)
        index_array = rng.choice(num_rows, size=num_iters, p=weight_list)
    else:
        raise ValueError("Unknown iteration policy '{}'.".format(iter_policy))

    return [(x, rng_seed ^ hash_int(x)) for x in index_array]


class MyWorkerBee(beehive.WorkerBee):
    """The worker bee class implementation for :func:`process_dataframe`.

    For internal use only.
    """

    def __init__(
        self,
        my_id: int,
        p_m2p: mp.Queue,
        p_p2m: mp.Queue,
        preprocess_func,
        postprocess_func=None,
        max_concurrency: int = 1024,
        context_vars: dict = {},
        logger: tp.Optional[logging.IndentedLoggerAdapter] = None,
    ):
        super().__init__(
            my_id,
            p_m2p,
            p_p2m,
            max_concurrency=max_concurrency,
            context_vars=context_vars,
            logger=logger,
        )
        self.preprocess_func = preprocess_func
        self.postprocess_func = postprocess_func

    async def preprocess(
        self,
        s: pd.Series,
        iter_id: int,
        rng_seed: int,
        *args,
        context_vars: dict = {},
        **kwargs
    ):
        return await self.preprocess_func(
            s, iter_id, rng_seed, *args, context_vars=context_vars, **kwargs
        )

    async def postprocess(
        self, tensor_dict: dict, *args, context_vars: dict = {}, **kwargs
    ):
        return await self.postprocess_func(
            tensor_dict, *args, context_vars=context_vars, **kwargs
        )


class MyQueenBee(beehive.QueenBee):
    """The worker bee class implementation for :func:`process_dataframe`.

    For internal use only.
    """

    def __init__(
        self,
        my_id: int,
        p_m2p: queue.Queue,
        p_p2m: queue.Queue,
        workerbee_class,
        df: pd.DataFrame,
        batchprocess_func=None,
        postprocess_func=None,
        preprocess_args: tuple = (),
        preprocess_kwargs: dict = {},
        batchprocess_args: tuple = (),
        batchprocess_kwargs: dict = {},
        postprocess_args: tuple = (),
        postprocess_kwargs: dict = {},
        skip_null: bool = False,
        batch_size: int = 32,
        worker_init_args: tuple = (),
        worker_init_kwargs: dict = {},
        s3_profile: tp.Optional[str] = None,
        max_concurrency: tp.Optional[int] = None,
        workerbee_max_concurrency: tp.Optional[int] = 1024,
        context_vars: dict = {},
        logger: tp.Optional[logging.IndentedLoggerAdapter] = None,
    ):
        super().__init__(
            my_id,
            p_m2p,
            p_p2m,
            workerbee_class,
            worker_init_args=worker_init_args,
            worker_init_kwargs=worker_init_kwargs,
            s3_profile=s3_profile,
            max_concurrency=max_concurrency,
            workerbee_max_concurrency=workerbee_max_concurrency,
            context_vars=context_vars,
            logger=logger,
        )

        self.df = df
        self.batchprocess_func = batchprocess_func
        self.postprocess_func = postprocess_func
        self.preprocess_args = preprocess_args
        self.preprocess_kwargs = preprocess_kwargs
        self.batchprocess_args = batchprocess_args
        self.batchprocess_kwargs = batchprocess_kwargs
        self.postprocess_args = postprocess_args
        self.postprocess_kwargs = postprocess_kwargs
        self.skip_null = skip_null
        self.batch_size = batch_size

    async def main(self, pair_list: list, context_vars: dict = {}):
        import asyncio

        # tracking variables
        class TVars:
            def __init__(self, logger=None):
                self.offset = 0  # where to get the next pair
                self.pre_pending_dtask_map = {}  # task -> pair_id
                self.pre_done_dtask_map = {}  # pair_id -> input_tensor_dict
                self.batch_done_list = []  # (pair_id_list, output_batch_tensor_dict)
                self.post_pending_dtask_map = {}  # task -> pair_id
                self.post_done_dtask_map = {}  # pair_id -> pd.Series
                self.progress_bar = tqdm(total=len(pair_list)) if logger else None

        tvars = TVars(logger=self.logger)

        # ----- declarations -----

        def delegate_some_preprocessing_tasks():
            max_concurrency = self.num_children * self.workerbee_max_concurrency // 2
            pre_cnt = min(
                len(pair_list) - tvars.offset,
                max_concurrency
                - len(tvars.pre_pending_dtask_map)
                - len(tvars.post_pending_dtask_map),
            )
            if pre_cnt <= 0:
                return False
            for i in range(pre_cnt):
                pair_id = tvars.offset + i
                row_id, rng_seed = pair_list[pair_id]
                s = self.df.iloc[row_id]
                task = asyncio.ensure_future(
                    self.delegate(
                        "preprocess",
                        s,
                        pair_id,
                        rng_seed,
                        *self.preprocess_args,
                        **self.preprocess_kwargs
                    )
                )
                tvars.pre_pending_dtask_map[task] = pair_id
            tvars.offset += pre_cnt
            return True

        def delegate_some_postprocessing_tasks():
            if not tvars.batch_done_list:
                return False
            for pair_id_list, output_batch_tensor_dict in tvars.batch_done_list:
                for i, pair_id in enumerate(pair_id_list):  # unstack the batch
                    row_id = pair_list[pair_id][0]
                    try:
                        output_tensor_dict = {
                            k: v[i] for k, v in output_batch_tensor_dict.items()
                        }
                    except (IndexError, AttributeError):
                        if self.logger:
                            self.logger.warn_last_exception()
                            self.logger.debug("Caught the above exception. Details:")
                            self.logger.debug("  pair_id_list: {}".format(pair_id_list))
                            self.logger.debug("  output_batch_tensor_dict: \{")
                            for k, v in output_batch_tensor_dict.items():
                                self.logger.debug("    {}: {}".format(k, v))
                            self.logger.debug("  \}")
                        raise
                    task = asyncio.ensure_future(
                        self.delegate(
                            "postprocess",
                            output_tensor_dict,
                            *self.postprocess_args,
                            **self.postprocess_kwargs
                        )
                    )
                    tvars.post_pending_dtask_map[task] = pair_id
            tvars.batch_done_list = []
            return True

        async def do_batch_processing():
            cur_batch_size = min(len(tvars.pre_done_dtask_map), self.batch_size)
            if cur_batch_size == 0:
                return False

            # stack them up
            pair_id_list = []
            input_tensor_dict_list = []
            for i in range(cur_batch_size):
                pair_id, input_tensor_dict = tvars.pre_done_dtask_map.popitem()
                pair_id_list.append(pair_id)
                input_tensor_dict_list.append(input_tensor_dict)
            batch_input_tensor_dict = {}
            for k in input_tensor_dict:
                batch_input_tensor_dict[k] = [x[k] for x in input_tensor_dict_list]

            # batch processing
            retval = await self.batchprocess_func(
                batch_input_tensor_dict,
                *self.batchprocess_args,
                context_vars=context_vars,
                **self.batchprocess_kwargs
            )
            if isinstance(retval, dict):
                if self.postprocess_func is None:
                    raise NotImplementedError(
                        "Batchprocessing returns a dictionary but there is no postprocessing function to handle it."
                    )
                tvars.batch_done_list.append((pair_id_list, retval))
            elif isinstance(retval, list):
                for pair_id, out_series in zip(pair_id_list, retval):
                    tvars.post_done_dtask_map[pair_id] = out_series
                tvars.progress_bar.update(cur_batch_size)
            elif (retval is None) and self.skip_null:
                tvars.progress_bar.update()
            else:
                raise NotImplementedError(
                    "Unexpected batchprocessing output type: {}.".format(type(retval))
                )

            return True

        async def handle_done_dtasks():
            import asyncio

            pending_tasks = list(tvars.pre_pending_dtask_map.keys()) + list(
                tvars.post_pending_dtask_map.keys()
            )
            if not pending_tasks:
                return False
            done_tasks, _ = await asyncio.wait(
                pending_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done_tasks:
                if task in tvars.pre_pending_dtask_map:
                    pair_id = tvars.pre_pending_dtask_map.pop(task)
                    is_pre = True
                    proc_str = "preprocessing"
                else:
                    pair_id = tvars.post_pending_dtask_map.pop(task)
                    is_pre = False
                    proc_str = "postprocessing"
                if task.cancelled():
                    text = "Cancelled iteration number {} during {}.".format(
                        pair_id, proc_str
                    )
                    raise asyncio.CancelledError(msg)
                elif task.exception() is not None:
                    if self.logger:
                        text = "Exception raised during {} iteration {}:".format(
                            proc_str, pair_id
                        )
                        self.logger.debug(text)
                    raise task.exception()
                else:
                    msg, child_id = task.result()
                    if msg["status"] == "cancelled":
                        text = "Cancelled delegated {} task to child {} of iteration {}".format(
                            proc_str, child_id, pair_id
                        )
                        with logging.scoped_debug(self.logger, text, curly=False):
                            if (msg["reason"] is not None) and self.logger:
                                self.logger.debug("Reason: {}".format(msg["reason"]))
                        raise asyncio.CancelledError(text + ".")
                    elif msg["status"] == "raised":
                        text = "Exception raised in the delegated {} task to child {} of iteration {}".format(
                            proc_str, child_id, pair_id
                        )
                        with logging.scoped_debug(self.logger, text, curly=False):
                            beehive.logger_debug_msg(msg, logger=self.logger)
                        raise msg["exception"]
                    else:  # msg['status'] == 'succeeded':
                        retval = msg["returning_value"]
                        if is_pre:
                            if isinstance(retval, dict):
                                if self.batchprocess_func is None:
                                    raise NotImplementedError(
                                        "Preprocessing returns a dictionary but there is no batchprocessing function to process it."
                                    )
                                tvars.pre_done_dtask_map[pair_id] = retval
                            elif isinstance(
                                retval, pd.Series
                            ):  # skip batch processing and postprocessing altogether
                                tvars.post_done_dtask_map[pair_id] = retval
                                tvars.progress_bar.update()
                            elif (retval is None) and self.skip_null:
                                tvars.progress_bar.update()
                            else:
                                raise RuntimeError(
                                    "The delegated preprocessing function returns an unexpected type: {}.".format(
                                        type(retval)
                                    )
                                )
                        else:
                            if isinstance(retval, pd.Series):
                                tvars.post_done_dtask_map[pair_id] = retval
                                tvars.progress_bar.update()
                            elif (retval is None) and self.skip_null:
                                tvars.progress_bar.update()
                            else:
                                raise RuntimeError(
                                    "The delegated postprocessing function returns an unexpected type: {}.".format(
                                        type(retval)
                                    )
                                )

            return True

        # ----- implementation -----

        while tvars.offset < len(
            pair_list
        ):  # until all pairs are sent for preprocessing
            await handle_done_dtasks()
            delegate_some_postprocessing_tasks()
            while len(tvars.pre_done_dtask_map) >= self.batch_size:
                await do_batch_processing()
            delegate_some_preprocessing_tasks()
            await asyncio.sleep(0)

        while tvars.pre_pending_dtask_map:  # until all pairs are preprocessed
            await handle_done_dtasks()
            delegate_some_postprocessing_tasks()
            while len(tvars.pre_done_dtask_map) >= self.batch_size:
                await do_batch_processing()
            await asyncio.sleep(0)

        if True:
            await handle_done_dtasks()
            delegate_some_postprocessing_tasks()
            while len(tvars.pre_done_dtask_map) >= self.batch_size:
                await do_batch_processing()
            await do_batch_processing()  # last batch, if any
            await asyncio.sleep(0)

        while tvars.batch_done_list:  # until all pairs are sent for postprocessing
            await handle_done_dtasks()
            delegate_some_postprocessing_tasks()
            await asyncio.sleep(0)

        while tvars.post_pending_dtask_map:  # until all pairs are postprocessed
            await handle_done_dtasks()
            await asyncio.sleep(0)

        if self.logger:
            tvars.progress_bar.close()

        out_rows = [(k, v) for k, v in tvars.post_done_dtask_map.items()]
        out_rows = sorted(out_rows, key=lambda x: x[0])  # sort in ascending pair id
        out_rows = [x[1] for x in out_rows]
        out_df = pd.DataFrame(data=out_rows)
        return out_df


async def process_dataframe(
    df: pd.DataFrame,
    preprocess_func,
    batchprocess_func=None,
    postprocess_func=None,
    rng_seed: int = 0,
    num_iters: tp.Optional[int] = None,
    preprocess_args: tuple = (),
    preprocess_kwargs: dict = {},
    batchprocess_args: tuple = (),
    batchprocess_kwargs: dict = {},
    postprocess_args: tuple = (),
    postprocess_kwargs: dict = {},
    skip_null: bool = False,
    iter_policy: str = "sequential",
    resampling_col: tp.Optional[str] = None,
    batch_size: int = 32,
    s3_profile: tp.Optional[str] = None,
    max_concurrency: int = 16,
    context_vars: dict = {},
    logger=None,
):
    """An asyn function that does batch processing a dataframe.

    The functionality provided here addresses the following situation. The user has a dataframe in
    which each row represents an event or an image. There is a need to take some fields of each row
    and to convert them into tensors to feed one or more models for training, prediction, validation
    or evaluation purposes. Upon applying the tensors to a model and getting some results, there is
    a need to transform output tensors back to some fields. In addition, the model(s) can only
    operate in batches, rather than on individual items.

    To address this situation, the user needs to provide 3 asyn functions: `preprocess`,
    `batchprocess` and `postprocess`. `preprocess` applies to each row for converting some fields of
    the row into a dictionary of tensors. Tensors of the same name are stacked up to form a
    dictionary of batch tensors, and then are fed to `batchprocess` for batch processing using the
    model(s). The output batch tensors are unstacked into individual tensors and then fed to
    `postprocess` to convert them back into pandas.Series representing fields for each row. Finally,
    these new fields are concatenated to form an output dataframe.

    The three above functions have a default implementation, namely :func:`default_preprocess`,
    :func:`default_batchprocess` and :func:`default_postprocess`, respectively. The user must make
    sure the APIs of their functions match with those of the default ones. Note that it is possible
    that during preprocessing or batchprocessing a row the function can skip batch-processing and
    postprocessing altogether and returns an output series corresponding to each input row.

    Internally, we use the BeeHive concurrency model to address the problem. The queen bee is
    responsible for forming batches, batch processing, and forming the output dataframe, the
    worker bees that she spawns are responsible for doing preprocessing and postprocessing works
    of each row.

    Parameters
    ----------
    df : pandas.DataFrame
        an input unindexed dataframe
    preprocess_func : function
        the preprocessing function
    batchprocess_func : function, optional
        the function for batch-processing. If not provided, the preprocess function must returned
        postprocessed pandas.Series instances.
    postprocess_func : function, optional
        the postrocessing function. If not provided, the preprocess and batchprocess functions
        must make sure that every row is processed and a pandas.Series is returned.
    rng_seed : int, optional
        seed for making RNGs
    num_iters : int, optional
        number of iterations or equivalently number of rows selected during the call. If not
        provided, it is set to the number of rows of `df`.
    preprocess_args : tuple, optional
        positional arguments to be passed as-is to :func:`preprocess`
    preprocess_kwargs : dict, optional
        keyword arguments to be passed as-is to :func:`preprocess`
    batchprocess_args : tuple, optional
        positional arguments to be passed as-is to :func:`batchprocess`
    batchprocess_kwargs : dict, optional
        keyword arguments to be passed as-is to :func:`batchprocess`
    postprocess_args : tuple, optional
        positional arguments to be passed as-is to :func:`postprocess`
    postprocess_kwargs : dict, optional
        keyword arguments to be passed as-is to :func:`postprocess`
    skip_null : bool
        If True, any None returned value from the provided functions will be considered as a
        trigger to skip the row. Otherwise, an exception is raised as usual.
    iter_policy : {'sequential', 'resampling'}
        policy for iterating the rows. If 'sequential' is given, the items are iterated
        sequentially from the first row to the last row and then back to the first row if required.
        If 'resampling' is given, then the dataframe column provided by the `resampling_col`
        argument provides the resampling weights. Rows are resampled randomly using these weights
        as the resampling distribution.
    resampling_col : str
        name of the column/field containing the resampling weights. Only valid if `iter_policy` is
        'resampling'.
    batch_size : int
        maximum batch size for each batch that is formed internally
    s3_profile : str, optional
        The AWS S3 profile to be used so that we can spawn an S3 client for each newly created
        subprocess. If not provided, the default profile will be used.
    max_concurrency : int
        the maximum number of concurrent tasks each worker bee handles at a time
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        Variable 's3_client' must exist and hold an enter-result of an async with statement
        invoking :func:`mt.base.s3.create_s3_client`. In asynchronous mode, variable
        'http_session' must exist and hold an enter-result of an async with statement invoking
        :func:`mt.base.http.create_http_session`.
    logger : mt.logging.IndentedLoggerAdapter, optional
        logger for debugging purposes

    Returns
    -------
    df : pandas.DataFrame
        an output unindexed dataframe

    Notes
    -----
    The function only works in asynchronous mode. That means `context_vars['async'] is True` is
    required.
    """

    if len(df) == 0:
        return pd.DataFrame(data=[])

    if not context_vars["async"]:
        raise ValueError("Only asynchronous mode is supported.")

    df = df.reset_index(
        drop=True
    )  # to make sure we have a standard unindexed dataframe. resampling can spoil the indices

    pair_list = sample_rows(
        df,
        rng_seed=rng_seed,
        num_iters=num_iters,
        iter_policy=iter_policy,
        resampling_col=resampling_col,
    )  # (row_id, rng_seed) list

    return await beehive.beehive_run(
        MyQueenBee,
        MyWorkerBee,
        "main",
        task_args=(pair_list,),
        queenbee_init_args=(df,),
        queenbee_init_kwargs={
            "batchprocess_func": batchprocess_func,
            "postprocess_func": postprocess_func,
            "preprocess_args": preprocess_args,
            "preprocess_kwargs": preprocess_kwargs,
            "batchprocess_args": batchprocess_args,
            "batchprocess_kwargs": batchprocess_kwargs,
            "postprocess_args": postprocess_args,
            "postprocess_kwargs": postprocess_kwargs,
            "skip_null": skip_null,
            "batch_size": batch_size,
        },
        workerbee_init_args=(preprocess_func,),
        workerbee_init_kwargs={"postprocess_func": postprocess_func},
        s3_profile=s3_profile,
        context_vars=context_vars,
        max_concurrency=max_concurrency,
        logger=logger,
    )
