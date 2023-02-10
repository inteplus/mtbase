import re
import ipaddress

from mt import tp


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
