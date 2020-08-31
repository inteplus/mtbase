'''streamz utilities, dask component.'''


from streamz import DaskStream, logger
from ..concurrency import get_dd_client


__all__ =  ['map_list']


@DaskStream.register_api()
class map_list(DaskStream):
    """ Apply a function to every non-null element in the stream of lists or iterables, but group the non-null elements into blocks of fixed size for efficiency.

    This operation is equivalent to `stream.flatten().filter(lambda x: x is not None).partition(block_size).map(func, *args, **kwargs)`, but without breaking this chain into 3 different threads. It should be used when `func` is too quick and we want to batch in large groups to avoid speed penalties due to switching context.

    Parameters
    ----------
    block_size : int
        the number of elements per block
    func: callable
    *args : list
        The arguments to pass to the function.
    **kwargs: dict
        Keyword arguments to pass to func

    Examples
    --------
    >>> source = Stream()
    >>> source.partition(2).map_list(5, lambda x: 2*x).sink(print)
    >>> for i in range(10):
    ...     source.emit(i)
    0
    2
    4
    6
    8
    10
    12
    14
    16
    18
    """
    def __init__(self, upstream, block_size, func, *args, **kwargs):
        if not isinstance(block_size, int) or block_size < 1:
            raise ValueError("Block size must be a positive integer, not {}.".format(block_size))
        self.item_buffer = []
        self.block_size = block_size
        self.func = func
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args
        self.client = get_dd_client()

        DaskStream.__init__(self, upstream, stream_name=stream_name)

    def update(self, iterables, who=None, metadata=None):

        # enqueue to self.item_buffer
        num_item_cnt = len(iterables)
        for i, item in enumerate(iterables):
            if item is None:
                continue
            item_metadata = metadata[i] if isinstance(metadata, list) and len(metadata) >= num_item_cnt else None
            if item_metadata is not None: self._retain_refs(item_metadata)
            self.item_buffer.append((item, item_metadata))
            
        # process each block of items from self.item_buffer
        result_list = []
        while len(self.item_buffer) >= self.block_size:
            # extract
            work_list = self.item_buffer[:self.block_size]
            self.item_buffer = self.item_buffer[self.block_size:]

            # map
            func_result_list = [None]*self.block_size
            for i in range(self.block_size):
                try:
                    item = work_list[i][0]
                    func_result_list[i] = self.client.submit(self.func, item, *self.args, **self.kwargs)
                except Exception as e:
                    logger.exception(e)
                    raise

            # block_metadata
            metadata_list = [work[1] for work in work_list]
            if metadata_list[0] is None: # expect everyone else is None
                for item_metadata in metadata_list:
                    if item_metadata is not None:
                        raise ValueError("Expected all items in a block to have no metadata. But at least one of them has metadata.")
                metadata_list = None
            else: # expect everyone else is not None
                for item_metadata in metadata_list:
                    if item_metadata is None:
                        raise ValueError("Expected all items in a block to have metadata. But at least one of them has not.")

            # partition
            ret = self._emit(tuple(func_result_list), metadata=metadata_list)
            if metadata_list is not None: self._release_refs(metadata_list)
            result_list.extend(ret)

        return result_list
