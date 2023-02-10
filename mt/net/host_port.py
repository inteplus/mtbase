import socket
from time import sleep
import re
import ipaddress

from mt import tp, logg


class HostPort:
    """Pair of host and port, where host can be by name or by ip address.

    The port must be given. As for the host, if an ip address is given, it has the priority. If
    not, the host name is taken. If even the host name is not provided, it is assumed that the host
    is localhost.

    Parameters
    ----------
    port : int
        the port number
    host_addr : ipaddress.IPv4Address or ipaddress.IPv6Address or None
        the host address, if known
    host_name : str, optional
        the host name, if known
    """

    def __init__(
        self,
        port: int,
        host_addr: tp.Union[ipaddress.IPv4Address, ipaddress.IPv6Address, None] = None,
        host_name: tp.Optional[str] = None,
    ):
        self.port = port
        self.host_addr = host_addr
        self.host_name = host_name

    def is_v6(self):
        if self.host_addr is None:
            if self.host_name is None:
                return False
            return ":" in self.host_name
        return isinstance(self.host_addr, ipaddress.IPv6Address)

    def socket_address(self) -> tp.Tuple[str, int]:
        """Returns the (addr, port) pair for socket programming."""

        if self.is_v6():
            if self.host_addr is None:
                return (self.host_name, self.port)
            return (self.host_addr.exploded, self.port)

        if self.host_addr is None:
            host_name = "localhost" if self.host_name is None else self.host_name
            return (host_name, self.port)

        return (self.host_addr.exploded, self.port)

    def to_str(self) -> str:
        """Serializes to a string."""
        host, port = self.socket_address()
        if self.host_addr is None:
            return "{}:{}".format(host, port)
        if self.is_v6():
            return "[{}]:{}".format(host, port)
        return "{}:{}".format(host, port)

    @classmethod
    def from_str(cls, s: str):
        """Deserializes from a string."""
        i = s.rfind(":")
        if i < 0:
            raise ValueError("Port not found in input: '{}'".format(s))
        port = int(s[i + 1 :])
        host = s[:i]

        if ":" in host:  # must be ipv6
            if host[0] == "[" and host[-1] == "]":
                host = host[1:-1]
            return HostPort(port, host_addr=ipaddress.IPv6Address(host))

        if len(host) == 0 or host == "*":
            return HostPort(port, host_name="")

        if not "." in host:
            return HostPort(port, host_name=host)

        if re.search("[^0-9\.]", host) is None:  # ipv4
            return HostPort(port, host_addr=ipaddress.IPv4Address(host))

        return HostPort(port, host_name=host)


def listen_to_port(
    listen_config: str,
    blocking: bool = True,
    logger: tp.Optional[logg.IndentedLoggerAdapter] = None,
) -> socket.socket:
    """Listens to a local port, returning the listening socket.

    The function repeats indefinitely until it can open the port.

    Parameters
    ----------
    listen_config : str
        listening config as an 'addr:port' pair. For example, ':30443', '0.0.0.0:324',
        'localhost:345', etc.
    blocking : bool
        whether or not the returning socket is blocking
    logger : mt.logg.IndentedLoggerAdapter, optional
        logger for debugging purposes

    Returns
    -------
    dock_socket : socket.socket, optional
        the output listening socket. None may be returned if something wrong has happened.
    """

    while True:
        try:
            listen_hostport = HostPort.from_str(listen_config)
            listen_address = listen_hostport.socket_address()
        except ValueError:
            if logger:
                logger.warn_last_exception()
                logger.error(
                    "Unable to parse listening config: '{}'".format(listen_config)
                )
            return

        try:
            family = socket.AF_INET6 if listen_hostport.is_v6() else socket.AF_INET
            socket_type = socket.SOCK_STREAM
            if not blocking:
                socket_type |= socket.SOCK_NONBLOCK
            dock_socket = socket.socket(family, socket_type)
        except OSError:
            if logger:
                logger.warn_last_exception()
            sleep(5)
            continue

        try:
            dock_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            dock_socket.bind(listen_address)
            dock_socket.listen(5)
            break
        except OSError as e:
            if logger:
                if e.errno == 98:
                    logger.warn(
                        "Unable to bind to local port {} which is in use. Please wait until it is "
                        "available.".format(listen_address)
                    )
                else:
                    logger.warn_last_exception()
            dock_socket.close()
            sleep(5)

    if logger:
        logger.info("Listening at '{}'.".format(listen_config))

    return dock_socket
