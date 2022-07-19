import json


class Resources:
    def __init__(self):
        self._resource_config = {}
        self._load()

        self.current_host = self._resource_config.get('current_host', 'local')
        self.hosts = self._resource_config.get('hosts', ['local'])

    def _load(self):
        try:
            with open("/opt/ml/config/resourceconfig.json", "r") as file:
                self._resource_config = json.load(file)
        except FileNotFoundError:
            print("/opt/ml/config/resourceconfig.json not found.  current_host is unknown.")
            pass

