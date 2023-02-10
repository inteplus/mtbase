import socket
from time import sleep

from mt import tp, logg, threading

from .host_port import HostPort, listen_to_port


def set_keepalive_linux(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    """Set TCP keepalive on an open socket.

    It activates after 1 second (after_idle_sec) of idleness,
    then sends a keepalive ping once every 3 seconds (interval_sec),
    and closes the connection after 5 failed ping (max_fails), or 15 seconds
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, after_idle_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, max_fails)


def set_keepalive_osx(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    """Set TCP keepalive on an open socket.

    sends a keepalive ping once every 3 seconds (interval_sec)
    """
    # scraped from /usr/include, not exported by python's socket module
    TCP_KEEPALIVE = 0x10
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, interval_sec)


def pf_shutdown_socket(socket, mode, config=None, logger=None):
    try:
        socket.shutdown(mode)
        return True
    except:
        if logger:
            msg = "Shuting down socket '{}' with mode {}".format(config, mode)
            with logger.scoped_warning(msg, curly=False):
                logger.warn_last_exception()
        return False


def pf_shutdown_stream(connection, is_c2s):
    """Shuts down the client->server stream or the server->client stream."""
    logger = connection["logger"]
    if is_c2s:
        if connection["c2s_stream"]:
            connection["c2s_stream"] = False
            with logg.scoped_debug(
                "Shutting down stream client {} -> server {}".format(
                    connection["client_config"], connection["server_config"]
                ),
                logger=logger,
                curly=False,
            ):
                pf_shutdown_socket(
                    connection["client_socket"],
                    socket.SHUT_RD,
                    config=connection["client_config"],
                    logger=logger,
                )
                pf_shutdown_socket(
                    connection["server_socket"],
                    socket.SHUT_WR,
                    config=connection["server_config"],
                    logger=logger,
                )
    else:
        if connection["s2c_stream"]:
            connection["s2c_stream"] = False
            with logg.scoped_debug(
                "Shutting down stream server {} -> client {}".format(
                    connection["server_config"], connection["client_config"]
                ),
                logger=logger,
                curly=False,
            ):
                pf_shutdown_socket(
                    connection["server_socket"],
                    socket.SHUT_RD,
                    config=connection["server_config"],
                    logger=logger,
                )
                pf_shutdown_socket(
                    connection["client_socket"],
                    socket.SHUT_WR,
                    config=connection["client_config"],
                    logger=logger,
                )

    if (
        connection["c2s_stream"] is False
        and connection["s2c_stream"] is False
        and not connection["closed"]
    ):
        connection["closed"] = True
        if "closed_callback" in connection:
            connection["closed_callback"]()


def pf_forward(connection, is_c2s):
    if is_c2s:
        src_socket = connection["client_socket"]
        dst_socket = connection["server_socket"]
    else:
        src_socket = connection["server_socket"]
        dst_socket = connection["client_socket"]
    logger = connection["logger"]

    string = " "
    while string:
        try:
            string = src_socket.recv(1024)
        except socket.timeout:
            if logger:
                if is_c2s:
                    msg = "Stream client '{}' -> server '{}' has timed out".format(
                        connection["client_config"], connection["server_config"]
                    )
                else:
                    msg = "Stream server '{}' -> client '{}' has timed out".format(
                        connection["client_config"], connection["server_config"]
                    )
                with logger.scoped_warning(msg, curly=False):
                    logger.warn_last_exception()

                pf_shutdown_stream(connection, is_c2s)
            break
        except OSError:
            if logger:
                msg = "Broken connection client '{}' <-> server '{}'  because".format(
                    connection["client_config"], connection["server_config"]
                )
                with logger.scoped_warning(msg, curly=False):
                    logger.warn_last_exception()
            pf_shutdown_stream(connection, is_c2s)
            pf_shutdown_stream(connection, not is_c2s)
            break

        if string:
            dst_socket.sendall(string)
        else:
            pf_shutdown_stream(connection, is_c2s)


def pf_server(listen_config, connect_configs, timeout=30, logger=None):
    try:
        dock_socket = listen_to_port(listen_config, logger=logger)

        while True:
            client_socket, client_addr = dock_socket.accept()
            client_socket.settimeout(timeout)
            set_keepalive_linux(client_socket)  # keep it alive
            if logger:
                logger.info(
                    "Client '{}' connected to '{}'.".format(client_addr, listen_config)
                )

            for connect_config in connect_configs:
                try:
                    connect_hostport = HostPort.from_str(connect_config)
                    connect_address = connect_hostport.socket_address()
                except ValueError:
                    if logger:
                        logger.warn_last_exception()
                        logger.error(
                            "Unable to parse connecting config: '{}'".format(
                                connect_config
                            )
                        )
                    break

                try:
                    family = (
                        socket.AF_INET6 if connect_hostport.is_v6() else socket.AF_INET
                    )
                    server_socket = socket.socket(family, socket.SOCK_STREAM)
                    # listen for 10 seconds before going to the next
                    server_socket.settimeout(10)
                    result = server_socket.connect_ex(connect_address)
                    if result != 0:
                        if logger:
                            logger.warning(
                                "Forward-connecting '{}' to '{}' returned {} instead of 0.".format(
                                    client_addr, connect_config, result
                                )
                            )
                        continue
                    if logger:
                        logger.info(
                            "Client '{}' forwarded to '{}'.".format(
                                client_addr, connect_config
                            )
                        )
                    server_socket.settimeout(timeout)
                    set_keepalive_linux(server_socket)  # keep it alive
                    connection = {
                        "client_socket": client_socket,
                        "server_socket": server_socket,
                        "client_config": listen_config,
                        "server_config": connect_config,
                        "logger": logger,
                        "c2s_stream": True,
                        "s2c_stream": True,
                        "closed": False,
                    }
                    threading.Thread(target=pf_forward, args=(connection, True)).start()
                    threading.Thread(
                        target=pf_forward, args=(connection, False)
                    ).start()

                    # wait for 1 sec to see if server disconnects disruptedly or not
                    sleep(1)
                    if connection["closed"]:  # already closed after 1 second?
                        if logger:
                            logger.warning(
                                "Connectioned terminated too quickly. Trying the next..."
                            )
                        continue  # bad config, try another one

                    break
                except:
                    if logger:
                        logger.warn_last_exception()
                        logger.warning(
                            "Unable to forward '{}' to '{}'. Skipping to next server.".format(
                                client_addr, connect_config
                            )
                        )
                    continue
            else:
                if logger:
                    logger.error(
                        "Unable to forward to any server for client '{}' connected to '{}'.".format(
                            client_addr, listen_config
                        )
                    )
    finally:
        if logger:
            logger.info("Waiting for 10 seconds before restarting the listener...")
        sleep(10)
        threading.Thread(
            target=pf_server,
            args=(listen_config, connect_configs),
            kwargs={"logger": logger},
        ).start()


def launch_port_forwarder(
    listen_config,
    connect_configs,
    timeout=30,
    logger: tp.Optional[logg.IndentedLoggerAdapter] = None,
):
    """Launchs in other threads a port forwarding service.

    Parameters
    ----------
    listen_config : str
        listening config as an 'addr:port' pair. For example, ':30443', '0.0.0.0:324',
        'localhost:345', etc.
    connect_configs : iterable
        list of connecting configs, each of which is an 'addr:port' pair. For example,
        'home2.sdfamily.co.uk:443', etc. Special case '::1:port' stands for localhost in ipv6 with
        a specific port.
    timeout : int
        number of seconds for connection timeout
    logger : mt.logg.IndentedLoggerAdapter, optional
        logger for debugging purposes
    """
    threading.Thread(
        target=pf_server,
        args=(listen_config, connect_configs),
        kwargs={"timeout": timeout, "logger": logger},
    ).start()
