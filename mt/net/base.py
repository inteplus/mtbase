import socket
import psutil
import getpass
import netifaces
import ipaddress
import requests
import getmac


def get_default_ifaces():
    """Returns a list of (host_ip_addr, subnet, broadcast, gateway_ip_address, iface) tuples of default ifaces."""
    res = []
    for k, v in netifaces.gateways()["default"].items():
        try:
            gw, iface = v
            item = netifaces.ifaddresses(iface)[k][0]
            ip_addr = ipaddress.ip_address(item["addr"])
            net_str = "{}/{}".format(item["addr"], item["netmask"])
            ip_network = ipaddress.ip_network(net_str, strict=False)
            gw_addr = ipaddress.ip_address(gw)
            bc_addr = ipaddress.ip_address(item["broadcast"])
            res.append((ip_addr, ip_network, bc_addr, gw_addr, iface))
        except ValueError:
            continue
    return res


def is_port_open(addr, port, timeout=2.0):
    """Checks if a port is open, with timeout.

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
    """

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((addr, port))
        return result == 0
    except:
        return False


def get_hostname():
    """Returns the machine's hostname."""
    return socket.gethostname()


def get_username():
    """Returns the current username.

    Returns
    -------
    str
        the current username
    """
    return getpass.getuser()


def get_all_hosts_from_network(ip_network):
    """Gets all hosts (ip_addr, mac_addr) from a given ip network.

    Parameters
    ----------
    ip_network : ipaddress.Ipv4Network
        IP network

    Returns
    -------
    list
        list of (ip_addr -> ipaddress.Ipv4Address, mac_addr -> str) pairs of detected hosts in the subnet.
    """
    res = []
    for addr in ip_network.hosts():
        mac = getmac.get_mac_address(ip=addr.exploded)
        if mac is not None and mac != "00:00:00:00:00:00":
            res.append((addr, mac))

    return res


def get_all_inet4_ipaddresses():
    """Returns all network INET4 interfaces' IP addresses+netmasks.

    Returns
    -------
    dict
        A dictionary of interface_name -> (ip_address, netmask)
    """
    retval1 = psutil.net_if_addrs()
    retval2 = {}
    for k, v in retval1.items():
        for e in v:
            if e.family == socket.AF_INET:
                retval2[k] = (e.address, e.netmask)
                break

    return retval2


def get_public_ip_address():
    """Obtains the public IP address using AWS.

    Returns
    -------
    ipaddress.Ipv4Address
        public ip address of the current host
    """
    ip = requests.get("https://checkip.amazonaws.com").text.strip()
    return ipaddress.ip_address(ip)
