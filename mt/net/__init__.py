from .base import *
from .host_port import *
from .port_forwarding import *
from .port_forwarding_async import *
from .ssh_forwarding import *


__api__ = [
    "get_default_ifaces",
    "is_port_open",
    "get_hostname",
    "get_username",
    "get_all_hosts_from_network",
    "get_all_inet4_ipaddresses",
    "get_public_ip_address",
    "HostPort",
    "listen_to_port",
    "set_keepalive_linux",
    "set_keepalive_osx",
    "launch_port_forwarder",
    "port_forwarder_actx",
    "SSHTunnelWatcher",
    "launch_ssh_forwarder",
]


# check if mtnet has been installed
def _future_warn_install_mtnet():

    import subprocess as sp

    bash_str = 'pip freeze | grep "mtnet"'
    try:
        sp.check_output(bash_str, shell=True)
    except sp.CalledProcessError:
        from .. import logg

        logg.logger.warn(
            "The 'mt.net' section of package 'mtbase' will be moved to package 'mtnet' from version 5.0."
        )
        logg.logger.warn("Please pip install mtnet in advance to avoid disruptions.")


_future_warn_install_mtnet()
