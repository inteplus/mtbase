import socket as _s
import psutil as _p
import getpass as _getpass
import threading as _t
import netifaces as _n
import ipaddress as _ia
import requests as _r
from getmac import get_mac_address
from time import sleep
from .with_utils import dummy_scope


__all__ = ['get_default_ifaces', 'is_port_open', 'get_hostname', 'get_username', 'get_all_hosts_from_network', 'get_all_inet4_ipaddresses', 'set_keepalive_linux', 'set_keepalive_osx', 'SSHTunnelWatcher', 'launch_port_forwarder', 'launch_ssh_forwarder', 'get_public_ip_address']


def get_default_ifaces():
    '''Returns a list of (host_ip_addr, subnet, broadcast, gateway_ip_address, iface) tuples of default ifaces.'''
    res = []
    for k, v in _n.gateways()['default'].items():
        gw, iface = v
        item = _n.ifaddresses(iface)[k][0]
        ip_addr = _ia.ip_address(item['addr'])
        net_str = "{}/{}".format(item['addr'], item['netmask'])
        ip_network = _ia.ip_network(net_str, strict=False)
        gw_addr = _ia.ip_address(gw)
        bc_addr = _ia.ip_address(item['broadcast'])
        res.append((ip_addr, ip_network, bc_addr, gw_addr, iface))
    return res


def is_port_open(addr, port, timeout=2.0):
    '''Checks if a port is open, with timeout.

    Parameters
    ----------
    addr : str
        ip address, hostname or fqdn
    port : int
        port number
    time_out : float
        timeout in seconds
    
    Returns
    -------
    bool
        whether or not the port at the given address is open
    '''

    try:
        sock = _s.socket(_s.AF_INET, _s.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((addr, port))
        return result == 0
    except:
        return False


def get_hostname():
    '''Returns the machine's hostname.'''
    return _s.gethostname()


def get_username():
    '''Returns the current username.

    Returns
    -------
    str
        the current username
    '''
    return _getpass.getuser()


def get_all_hosts_from_network(ip_network):
    '''Gets all hosts (ip_addr, mac_addr) from a given ip network.

    Parameters
    ----------
    ip_network : ipaddress.Ipv4Network
        IP network

    Returns
    -------
    list
        list of (ip_addr -> ipaddress.Ipv4Address, mac_addr -> str) pairs of detected hosts in the subnet.
    '''
    res = []
    for addr in ip_network.hosts():
        mac = get_mac_address(ip=addr.exploded)
        if mac is not None and mac != '00:00:00:00:00:00':
            res.append((addr, mac))

    return res


def get_all_inet4_ipaddresses():
    '''Returns all network INET4 interfaces' IP addresses+netmasks.

    Returns
    -------
    dict
        A dictionary of interface_name -> (ip_address, netmask)
    '''
    retval1 = _p.net_if_addrs()
    retval2 = {}
    for k, v in retval1.items():
        for e in v:
            if e.family == _s.AF_INET:
                retval2[k] = (e.address, e.netmask)
                break

    return retval2


# ----- port forwarding -----


def set_keepalive_linux(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    """Set TCP keepalive on an open socket.

    It activates after 1 second (after_idle_sec) of idleness,
    then sends a keepalive ping once every 3 seconds (interval_sec),
    and closes the connection after 5 failed ping (max_fails), or 15 seconds
    """
    sock.setsockopt(_s.SOL_SOCKET, _s.SO_KEEPALIVE, 1)
    sock.setsockopt(_s.IPPROTO_TCP, _s.TCP_KEEPIDLE, after_idle_sec)
    sock.setsockopt(_s.IPPROTO_TCP, _s.TCP_KEEPINTVL, interval_sec)
    sock.setsockopt(_s.IPPROTO_TCP, _s.TCP_KEEPCNT, max_fails)


def set_keepalive_osx(sock, after_idle_sec=1, interval_sec=3, max_fails=5):
    """Set TCP keepalive on an open socket.

    sends a keepalive ping once every 3 seconds (interval_sec)
    """
    # scraped from /usr/include, not exported by python's socket module
    TCP_KEEPALIVE = 0x10
    sock.setsockopt(_s.SOL_SOCKET, _s.SO_KEEPALIVE, 1)
    sock.setsockopt(_s.IPPROTO_TCP, TCP_KEEPALIVE, interval_sec)


def _pf_shutdown_socket(socket, mode, config=None, logger=None):
    try:
        socket.shutdown(mode)
        return True
    except:
        if logger:
            msg = "Shuting down socket '{}' with mode {}".format(config, mode)
            with logger.scoped_warning(msg, curly=False):
                logger.warn_last_exception()
        return False


def _pf_shutdown_stream(connection, is_c2s):
    '''Shuts down the client->server stream or the server->client stream.'''
    logger = connection['logger']
    if is_c2s:
        if connection['c2s_stream']:
            connection['c2s_stream'] = False
            with logger.scoped_debug("Shutting down stream client {} -> server {}".format(connection['client_config'], connection['server_config']), curly=False) if logger else dummy_scope:
                _pf_shutdown_socket(
                    connection['client_socket'], _s.SHUT_RD, config=connection['client_config'], logger=logger)
                _pf_shutdown_socket(
                    connection['server_socket'], _s.SHUT_WR, config=connection['server_config'], logger=logger)
    else:
        if connection['s2c_stream']:
            connection['s2c_stream'] = False
            with logger.scoped_debug("Shutting down stream server {} -> client {}".format(connection['server_config'], connection['client_config']), curly=False) if logger else dummy_scope:
                _pf_shutdown_socket(
                    connection['server_socket'], _s.SHUT_RD, config=connection['server_config'], logger=logger)
                _pf_shutdown_socket(
                    connection['client_socket'], _s.SHUT_WR, config=connection['client_config'], logger=logger)

    if connection['c2s_stream'] is False and connection['s2c_stream'] is False and not connection['closed']:
        connection['closed'] = True
        if 'closed_callback' in connection:
            connection['closed_callback']()


def _pf_forward(connection, is_c2s):
    if is_c2s:
        src_socket = connection['client_socket']
        dst_socket = connection['server_socket']
    else:
        src_socket = connection['server_socket']
        dst_socket = connection['client_socket']
    logger = connection['logger']

    string = ' '
    while string:
        try:
            string = src_socket.recv(1024)
        except _s.timeout:
            if logger:
                if is_c2s:
                    msg = "Stream client '{}' -> server '{}' has timed out".format(connection['client_config'], connection['server_config'])
                else:
                    msg = "Stream server '{}' -> client '{}' has timed out".format(connection['client_config'], connection['server_config'])
                with logger.scoped_warning(msg, curly=False):
                    logger.warn_last_exception()

                _pf_shutdown_stream(connection, is_c2s)
            break
        except OSError:
            if logger:
                msg = "Broken connection client '{}' <-> server '{}'  because".format(connection['client_config'], connection['server_config'])
                with logger.scoped_warning(msg, curly=False):
                    logger.warn_last_exception()
            _pf_shutdown_stream(connection, is_c2s)
            _pf_shutdown_stream(connection, not is_c2s)
            break

        if string:
            dst_socket.sendall(string)
        else:
            _pf_shutdown_stream(connection, is_c2s)


def _pf_server(listen_config, connect_configs, timeout=30, logger=None):
    try:
        while True:
            try:
                dock_socket = _s.socket(_s.AF_INET, _s.SOCK_STREAM)
            except OSError:
                if logger:
                    logger.warn_last_exception()
                sleep(5)
                continue

            try:
                listen_params = listen_config.split(':')
                dock_socket.bind((listen_params[0], int(listen_params[1])))
                dock_socket.listen(5)
                break
            except OSError:
                if logger:
                    logger.warn_last_exception()
                dock_socket.close()
                sleep(5)

        if logger:
            logger.info("Listening at '{}'.".format(listen_config))

        while True:
            client_socket, client_addr = dock_socket.accept()
            client_socket.settimeout(timeout)
            set_keepalive_linux(client_socket)  # keep it alive
            if logger:
                logger.info("Client '{}' connected to '{}'.".format(
                    client_addr, listen_config))

            for connect_config in connect_configs:
                try:
                    server_socket = _s.socket(_s.AF_INET, _s.SOCK_STREAM)
                    # listen for 10 seconds before going to the next
                    server_socket.settimeout(10)
                    connect_params = connect_config.split(':')
                    result = server_socket.connect_ex(
                        (connect_params[0], int(connect_params[1])))
                    if result != 0:
                        if logger:
                            logger.warning("Forward-connecting '{}' to '{}' returned {} instead of 0.".format(
                                client_addr, connect_config, result))
                        continue
                    if logger:
                        logger.info("Client '{}' forwarded to '{}'.".format(
                            client_addr, connect_config))
                    server_socket.settimeout(timeout)
                    set_keepalive_linux(server_socket)  # keep it alive
                    connection = {
                        'client_socket': client_socket,
                        'server_socket': server_socket,
                        'client_config': listen_config,
                        'server_config': connect_config,
                        'logger': logger,
                        'c2s_stream': True,
                        's2c_stream': True,
                        'closed': False,
                    }
                    _t.Thread(target=_pf_forward, args=(
                        connection, True)).start()
                    _t.Thread(target=_pf_forward, args=(
                        connection, False)).start()

                    # wait for 1 sec to see if server disconnects disruptedly or not
                    sleep(1)
                    if connection['closed']: # already closed after 1 second?
                        if logger:
                            logger.warning("Connectioned terminated too quickly. Trying the next...")
                        continue # bad config, try another one

                    break
                except:
                    if logger:
                        logger.warn_last_exception()
                        logger.warning("Unable to forward '{}' to '{}'. Skipping to next server.".format(
                            client_addr, connect_config))
                    continue
            else:
                if logger:
                    logger.error("Unable to forward to any server for client '{}' connected to '{}'.".format(
                        client_addr, listen_config))
    finally:
        if logger:
            logger.info(
                "Waiting for 10 seconds before restarting the listener...")
        sleep(10)
        _t.Thread(target=_pf_server, args=(listen_config,
                                           connect_configs), kwargs={'logger': logger}).start()


class SSHTunnelWatcher(object):

    def __init__(self, ssh_tunnel_forwarder, logger=None):
        self.base = ssh_tunnel_forwarder
        self.logger = logger
        self.num_conns = 0
        self.lock = _t.Lock()

    def inc(self):
        with self.lock:
            if self.num_conns == 0:
                if not self.base.is_alive:
                    if self.logger:
                        self.logger.debug("Activating SSH tunnel '{}'.".format(
                            self.base._remote_binds))
                    self.base.start()
            self.num_conns += 1

    def __call__(self):
        with self.lock:
            self.num_conns -= 1
            if self.num_conns == 0:
                if self.logger:
                    self.logger.debug("Deactivating SSH tunnel '{}'.".format(
                        self.base._remote_binds))
                self.base.stop()



def launch_port_forwarder(listen_config, connect_configs, timeout=30, logger=None):
    '''Launchs in other threads a port forwarding service.

    Parameters
    ----------
    listen_config : str
        listening config as an 'addr:port' pair. For example, ':30443', '0.0.0.0:324', 'localhost:345', etc.
    connect_configs : iterable
        list of connecting configs, each of which is an 'addr:port' pair. For example, 'www.foomum.com:443', etc.
    timeout : int
        number of seconds for connection timeout
    logger : logging.Logger or equivalent
        for logging messages
    '''
    _t.Thread(target=_pf_server, args=(listen_config, connect_configs),
              kwargs={'timeout': timeout, 'logger': logger}).start()


def _pf_tunnel_server(listen_config, ssh_tunnel_forwarder, timeout=30, logger=None):
    try:
        while True:
            try:
                dock_socket = _s.socket(_s.AF_INET, _s.SOCK_STREAM)
            except OSError:
                if logger:
                    logger.warn_last_exception()
                sleep(5)
                continue

            try:
                listen_params = listen_config.split(':')
                dock_socket.bind((listen_params[0], int(listen_params[1])))
                dock_socket.listen(5)
                break
            except OSError:
                if logger:
                    logger.warn_last_exception()
                dock_socket.close()
                sleep(5)

        if logger:
            logger.info("Listening at '{}'.".format(listen_config))

        watcher = SSHTunnelWatcher(ssh_tunnel_forwarder, logger=logger)

        while True:
            client_socket, client_addr = dock_socket.accept()
            client_socket.settimeout(timeout)
            set_keepalive_linux(client_socket)  # keep it alive
            if logger:
                logger.info("Client '{}' connected to '{}'.".format(
                    client_addr, listen_config))

            watcher.inc()

            try:
                server_socket = _s.socket(_s.AF_INET, _s.SOCK_STREAM)
                # listen for 10 seconds before going to the next
                server_socket.settimeout(10)
                result = server_socket.connect_ex(
                    ('localhost', ssh_tunnel_forwarder.local_bind_port))
                if result != 0:
                    if logger:
                        logger.warning("Forward-connecting '{}' to '{}' returned {} instead of 0.".format(
                            client_addr, ssh_tunnel_forwarder._remote_binds, result))
                    continue
                if logger:
                    logger.info("Client '{}' forwarded to '{}'.".format(
                        client_addr, ssh_tunnel_forwarder._remote_binds))
                server_socket.settimeout(timeout)
                set_keepalive_linux(server_socket)  # keep it alive
                connection = {
                    'client_socket': client_socket,
                    'server_socket': server_socket,
                    'client_config': listen_config,
                    'server_config': ssh_tunnel_forwarder._remote_binds,
                    'logger': logger,
                    'c2s_stream': True,
                    's2c_stream': True,
                    'closed': False,
                    'closed_callback': watcher,
                }
                _t.Thread(target=_pf_forward, args=(
                    connection, True)).start()
                _t.Thread(target=_pf_forward, args=(
                    connection, False)).start()
            except:
                if logger:
                    msg = "Unable to forward '{}' to '{}'.".format(client_addr, ssh_tunnel_forwarder._remote_binds)
                    with logger.scoped_warning(msg, curly=False):
                        logger.warn_last_exception()
    finally:
        if logger:
            logger.warn_last_exception()
            logger.info(
                "Waiting for 10 seconds before restarting the listener...")
        sleep(10)
        _t.Thread(target=_pf_tunnel_server, args=(listen_config,
                                                  ssh_tunnel_forwarder), kwargs={'timeout': timeout, 'logger': logger}).start()


def launch_ssh_forwarder(listen_config, ssh_tunnel_forwarder, timeout=30, logger=None):
    '''Launchs in other threads a port forwarding service via SSH tunnel.

    Parameters
    ----------
    listen_config : str
        listening config as an 'addr:port' pair. For example, ':30443', '0.0.0.0:324', 'localhost:345', etc.
    ssh_tunnel_forwarder : sshtunnel.SSHTunnelForwarder
        a stopped SSHTunnelForwarder instance
    timeout : int
        number of seconds for connection timeout
    logger : logging.Logger or equivalent
        for logging messages
    '''
    try:
        import sshtunnel
    except ImportError:
        raise RuntimeError(
            "Unable to import sshtunnel. Try installing it like using 'pip install sshtunnel'.")
    if not isinstance(ssh_tunnel_forwarder, sshtunnel.SSHTunnelForwarder):
        raise ValueError(
            "The argument `ssh_tunnel_forwarder` is not an instance of sshtunnel.SSHTunnelForwarder.")
    _t.Thread(target=_pf_tunnel_server, args=(listen_config, ssh_tunnel_forwarder),
              kwargs={'timeout': timeout, 'logger': logger}).start()


def get_public_ip_address():
    '''Obtains the public IP address using AWS.

    Returns
    -------
    ipaddress.Ipv4Address
        public ip address of the current host
    '''
    ip = _r.get('https://checkip.amazonaws.com').text.strip()
    return _ia.ip_address(ip)
