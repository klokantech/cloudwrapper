"""Google Compute Engine Instance."""

import requests

class GoogleComputeEngine(object):

    def __init__(self):
        self.server = "http://metadata/computeMetadata/v1/instance/"
        self.headers = {"Metadata-Flavor": "Google"}
        self._id = None
        self._name = None
        self._zone = None
        try:
            self._id = requests.get(self.server + "id", headers=self.headers).text
        except requests.exceptions.ConnectTimeout:
            self.is_instance = False
        else:
            self.is_instance = True


    def isInstance(self):
        return self.is_instance


    def instanceId(self):
        if not self.is_instance:
            return ''
        return self._id


    def instanceName(self):
        if not self.is_instance:
            return ''
        if self._name is None:
            self._name = requests.get(self.server + "hostname", headers=self.headers).text
        return self._name


    def instanceZone(self):
        if not self.is_instance:
            return ''
        if self._zone is None:
            self._zone = requests.get(self.server + "zone", headers=self.headers).text
        return self._zone

