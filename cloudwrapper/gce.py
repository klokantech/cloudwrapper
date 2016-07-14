"""Google Compute Engine instance metadata.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

try:
    import requests
except ImportError:
    from warnings import warn
    install_modules = [
        'requests==2.9.1',
    ]
    warn('cloudwrapper.gce requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise

class GoogleComputeEngine(object):

    def __init__(self):
        self.server = "http://metadata/computeMetadata/v1/instance/"
        self.headers = {"Metadata-Flavor": "Google"}
        self._id = None
        self._name = None
        self._zone = None
        self._externalIp = None
        self._internalIp = None
        self._projectId = None
        try:
            self._id = requests.get(self.server + "id", headers=self.headers).text
            self._projectId = requests.get('http://metadata/computeMetadata/v1/project/project-id', headers=self.headers).text
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
            resp = requests.get(self.server + "zone", headers=self.headers).text
            # Returns: projects/pid/zones/<zone>
            self._zone = resp.split('/')[-1]
        return self._zone


    def instanceExternalIP(self):
        if not self.is_instance:
            return ''
        if self._externalIp is None:
            self._externalIp = requests.get(self.server + "network-interfaces/0/access-configs/0/external-ip", headers=self.headers).text
        return self._externalIp


    def instanceInternalIP(self):
        if not self.is_instance:
            return ''
        if self._internalIp is None:
            self._internalIp = requests.get(self.server + "network-interfaces/0/ip", headers=self.headers).text
        return self._internalIp


    def projectId(self):
        if not self.is_instance:
            return None
        # Project ID should be requested only once in constructor!
        return self._projectId

