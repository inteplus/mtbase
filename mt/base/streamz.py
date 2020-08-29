'''streamz utilities.'''


from streamz import *


__all__ =  ['map_block']


def map_block(upstream, block_size, func, *args, **kwargs):
    '''Apply a function to every element in the stream, but group elements in to blocks of fixed size for faster processing.

    This function behaves the same as :func:`streamz.map`, except that it groups items into blocks of fixed size so that the time to process each block is large enough to avoid threading penalties. It does not do buffering as a pre-processing step.

    Parameters
    ----------
    upstream : streamz.Stream
        stream to operate on
    block_size : int
        number of items each block. If block_size is 1, it haves exactly like :func:`streamz.map`.
    func : function
        the function to apply to every item
    args : list
        additional positional arguments to pass to the function
    kwargs : dict
        additional keyword arguments to pass to the function

    Returns
    -------
    downstream : same as `upstream`
        the output stream

    See Also
    --------
    streamz.map
        for the original functionality
    '''
    if block_size==1:
        return source.map(func, *args, **kwargs)
    if not isinstance(block_size, int) or block_size < 1:
        raise ValueError("Expected block_size to be a positive integer. Received {}.".format(block_size))
    def process(block, *args, **kwargs):
        return [func(item, *args, **kwargs) for item in block]
    return upstream.partition(block_size).map(process, *args, **kwargs).flatten()
