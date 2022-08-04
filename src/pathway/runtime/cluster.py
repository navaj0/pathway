import json
import socket

from retrying import retry


class Cluster:
    def __init__(self):
        self._resource_config = {}
        self._load()

        self._current_host = self._resource_config.get('current_host', 'local')
        self._hosts = self._resource_config.get('hosts', ['local'])

        self._wait_hostname_resolution()

    def _load(self):
        try:
            with open("/opt/ml/config/resourceconfig.json", "r") as file:
                self._resource_config = json.load(file)
        except FileNotFoundError:
            print("/opt/ml/config/resourceconfig.json not found.  current_host is unknown.")
            pass

    @retry(stop_max_delay=1000 * 60 * 15, wait_exponential_multiplier=100, wait_exponential_max=30000)
    def _dns_lookup(self, host):
        """Retrying DNS lookup on host."""
        return socket.gethostbyname(host)

    def _wait_hostname_resolution(self):
        """Wait for the hostname resolution of the container. This is known behavior as the cluster
        boots up and has been documented here:
         https://docs.aws.amazon.com/sagemaker/latest/dg/your-algorithms-training-algo-running-container.html#your-algorithms-training-algo-running-container-dist-training
        """
        for host in self._hosts:
            self._dns_lookup(host)

    @property
    def master_host(self):
        return self._hosts[0]

    @property
    def master_port(self):
        return 55555

    @property
    def current_host(self):
        return self._current_host

    @property
    def rank(self):
        return self._hosts.index(self._current_host)

    @property
    def size(self):
        return len(self._hosts)



