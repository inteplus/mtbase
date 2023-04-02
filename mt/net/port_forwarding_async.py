import asyncio
import socket

from mt import tp, logg

from .host_port import HostPort
from .port_forwarding import set_keepalive_linux


class StreamForwarder:
    def __init__(self, reader, writer, logger=None):
        self.reader = reader
        self.writer = writer
        self.logger = logger

    async def close_writer(self):
        if not self.writer.is_closing():
            self.writer.close()
            await self.writer.wait_closed()

    async def __call__(self):
        if self.reader.at_eof():
            await self.close_writer()
        else:
            try:
                data = await self.reader.read(n=2048)
                if data:
                    self.writer.write(data)
                    await self.writer.drain()
            except:
                if self.logger:
                    self.logger.warn_last_exception()
                    self.logger.warn("Dropping the stream.")
                await self.close_writer()


class PortForwardingService:
    def __init__(self, listen_config, connect_configs, logger=None):
        self.listen_config = listen_config
        self.connect_configs = connect_configs
        self.logger = logger

    async def scan_remotes(self):
        """Scans the remote configs for one that can be connected to."""

        delay_times = [60, 600, 3600]
        idx = 0

        while True:
            task_map = {}
            for connect_config in self.connect_configs:
                try:
                    connect_hostport = HostPort.from_str(connect_config)
                    connect_address = connect_hostport.socket_address()
                except ValueError:
                    msg = "Unable to parse connecting config: '{}'.".format(
                        connect_config
                    )
                    logg.error(msg, logger=self.logger)
                    raise

                family = socket.AF_INET6 if connect_hostport.is_v6() else socket.AF_INET
                coro = asyncio.open_connection(
                    host=connect_address[0], port=connect_address[1], family=family
                )
                task = asyncio.ensure_future(coro)
                task_map[connect_config] = task

            res = None
            while res is None:
                cur_task_map = {k: v for k, v in task_map.items() if not v.done()}
                if len(cur_task_map) == 0:
                    break

                await asyncio.wait(
                    cur_task_map.values(), return_when=asyncio.FIRST_COMPLETED
                )

                # find a good remote
                for connect_config, task in cur_task_map.items():
                    if (
                        not task.done()
                        or task.cancelled()
                        or (task.exception() is not None)
                    ):
                        continue
                    server_reader, server_writer = task.result()
                    res = (connect_config, server_reader, server_writer)
                    break

            # clean up and return
            if res is not None:
                for task in task_map.values():
                    if not task.done() and not task.cancelled():
                        task.cancel()
                return res

            # attempt to retry in 1 minute
            if not self.logger:
                await asyncio.sleep(60)
                continue

            # display error messages before retrying
            msg = "Unable to connect '{}' to any destination.".format(
                self.listen_config
            )
            self.logger.error(msg)
            with self.logger.scoped_error("Reasons"):
                for connect_config, task in task_map.items():
                    if not task.done() or task.cancelled():
                        continue
                    e = task.exception()
                    if e is None:
                        continue
                    msg = "Connect '{}'".format(connect_config)
                    with self.logger.scoped_error(msg):
                        if isinstance(e, socket.gaierror) and e.errno == -3:
                            msg = "Unable to resolve host '{}'.".format(connect_config)
                            self.logger.warn(msg)
                        else:
                            try:
                                raise e
                            except:
                                self.logger.warn_last_exception()

            if idx < len(delay_times):
                timeout = delay_times[idx]
                self.logger.error(f"Will retry in {timeout} seconds.")
                idx += 1
                await asyncio.sleep(60)
            else:
                raise ConnectionAbortedError("Unable to connect to any remote server.")

    async def __call__(
        self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter
    ):
        client_addr = client_reader._transport.get_extra_info("peername")
        sock = client_reader._transport.get_extra_info("socket")
        set_keepalive_linux(sock)
        msg = "Client '{}' connected to '{}'.".format(client_addr, self.listen_config)
        logg.debug(msg, logger=self.logger)

        # establish a connection to a server
        try:
            connect_config, server_reader, server_writer = await self.scan_remotes()
        except OSError:
            return  # MT-TODO: fix me
        sock = server_reader._transport.get_extra_info("socket")
        set_keepalive_linux(sock)
        msg = "Client '{}' forwarded to '{}'.".format(client_addr, connect_config)
        logg.debug(msg, logger=self.logger)

        c2s_task = None
        s2c_task = None

        while True:
            if client_writer.is_closing() and server_writer.is_closing():
                break

            if c2s_task is None and not server_writer.is_closing():
                c2s = StreamForwarder(
                    client_reader, server_writer, logger=self.logger
                )()
                c2s_task = asyncio.ensure_future(c2s)
            if s2c_task is None and not client_writer.is_closing():
                s2c = StreamForwarder(
                    server_reader, client_writer, logger=self.logger
                )()
                s2c_task = asyncio.ensure_future(s2c)

            tasks = []
            if c2s_task is not None:
                tasks.append(c2s_task)
            if s2c_task is not None:
                tasks.append(s2c_task)

            done_set, pending_set = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )

            for item in done_set:
                if c2s_task == item:
                    try:
                        c2s_task.result()
                    except:
                        if self.logger:
                            self.logger.warn_last_exception()
                            self.logger.warn(
                                "Ignored the above exception while forwarding data "
                                "from client '{}' to server '{}'.".format(
                                    client_addr, connect_config
                                )
                            )
                    c2s_task = None
                elif s2c_task == item:
                    try:
                        s2c_task.result()
                    except:
                        if self.logger:
                            self.logger.warn_last_exception()
                            self.logger.warn(
                                "Ignored the above exception while forwarding data "
                                "from server '{}' to client '{}'.".format(
                                    connect_config, client_addr
                                )
                            )
                    s2c_task = None

        msg = "Client '{}' disconnected from '{}'.".format(
            client_addr, self.listen_config
        )
        logg.debug(msg, logger=self.logger)


async def port_forwarder_actx(
    listen_config,
    connect_configs,
    logger: tp.Optional[logg.IndentedLoggerAdapter] = None,
) -> asyncio.base_events.Server:
    """Launches an asynchronous port forwarding server.

    Parameters
    ----------
    listen_config : str
        listening config as an 'addr:port' pair. For example, ':30443', '0.0.0.0:324',
        'localhost:345', etc.
    connect_configs : iterable
        list of connecting configs, each of which is an 'addr:port' pair. For example,
        'home2.sdfamily.co.uk:443', etc. Special case '::1:port' stands for localhost in ipv6 with
        a specific port.
    logger : mt.logg.IndentedLoggerAdapter, optional
        logger for debugging purposes

    Returns
    -------
    server : asyncio.base_events.Server
        the port forwarding server that can be used as an asynchronous context
    """

    try:
        listen_hostport = HostPort.from_str(listen_config)
        listen_address = listen_hostport.socket_address()
        listen_family = socket.AF_INET6 if listen_hostport.is_v6() else socket.AF_INET
    except Exception:
        msg = "Exception caught while parsing listening config: '{}'".format(
            listen_config
        )
        logg.error(msg, logger=logger)
        raise

    client_connected_cb = PortForwardingService(
        listen_config, connect_configs, logger=logger
    )
    server = await asyncio.start_server(
        client_connected_cb,
        host=listen_address[0],
        port=listen_address[1],
        family=listen_family,
    )

    return server
