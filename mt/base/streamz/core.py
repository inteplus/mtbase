'''Extra core operators for streamz.'''


from streamz import Stream, logger


__all__ = ['rebatch', 'batch_map', 'Stream']


@Stream.register_api()
class rebatch(Stream):
    '''Regroups/repartitions a stream of batches of objects into another stream of batches of fixed size.

    Parameters
    ----------
    batch_size : int
        target number of items in each output block

    Examples
    --------
    >>> source = Stream()
    >>> source.partition(3).rebatch(4).sink(print)
    >>> for i in range(12):
            source.emit(i)
    [0, 1, 2, 3]
    [4, 5, 6, 7]
    [8, 9, 10, 11]
    '''

    def __init__(self, upstream, batch_size, **kwargs):
        if not isinstance(batch_size, int):
            raise TypeError("Expected batch_size to be an integer. Got {}.".format(type(batch_size)))
        if batch_size <= 0:
            raise ValueError("Expected batch_size to be a positive integer. Got {}.".format(batch_size))
        self.batch_size = batch_size
        self.item_buffer = []

        stream_name = kwargs.pop('stream_name', None)
        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, batch, who=None, metadata=None):
        # enqueue
        item_cnt = len(batch)
        if metadata is None:
            metadata = []
        self._retain_refs(metadata)
        for i in range(item_cnt-1):
            self.item_buffer.append((batch[i], []))
        self.item_buffer.append((batch[item_cnt-1], metadata)) # the last one has all the metadata

        # emit
        result_list = []
        while len(self.item_buffer) >= self.batch_size:
            items = []
            metadata = []
            for i in range(self.batch_size):
                pair = self.item_buffer[i]
                items.append(pair[0])
                metadata.extend(pair[1])
            self.item_buffer = self.item_buffer[self.batch_size:]

            ret = self._emit(items, metadata=metadata)
            self._release_refs(metadata)
            
            result_list.extend(ret)

        return result_list


@Stream.register_api()
class batch_map(Stream):
    """ Apply a function to every element of every batch in the stream

    Parameters
    ----------
    func: callable
    *args :
        The arguments to pass to the function.
    **kwargs:
        Keyword arguments to pass to func

    Examples
    --------
    >>> source = Stream()
    >>> source.partition(3).batch_map(lambda x: 2*x).sink(print)
    >>> for i in range(6):
    ...     source.emit(i)
    [0, 2, 4]
    [6, 8, 10]
    """
    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        # this is one of a few stream specific kwargs
        stream_name = kwargs.pop('stream_name', None)
        self.kwargs = kwargs
        self.args = args

        Stream.__init__(self, upstream, stream_name=stream_name)

    def update(self, batch, who=None, metadata=None):
        try:
            result = [self.func(x, *self.args, **self.kwargs) for x in batch]
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result, metadata=metadata)


