'''Extra dask operators for streamz.'''


from tornado import gen
from dask.distributed import default_client
from streamz import DaskStream, logger
from . import core


__all__ = ['DaskStream']


@DaskStream.register_api()
class rebatch(DaskStream, core.rebatch):
    pass


@DaskStream.register_api()
class batch_map(DaskStream):
    __doc__ = core.batch_map.__doc__

    def __init__(self, upstream, func, *args, **kwargs):
        self.func = func
        self.kwargs = kwargs
        self.args = args

        DaskStream.__init__(self, upstream)

    def update(self, batch, who=None, metadata=None):
        try:
            client = default_client()
            result = [client.submit(self.func, x, *self.args, **self.kwargs) for x in batch]
        except Exception as e:
            logger.exception(e)
            raise
        else:
            return self._emit(result, metadata=metadata)
