"""Google Compute Engine instance metadata.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

try:
    import requests
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from oauth2client.client import GoogleCredentials
except ImportError:
    from warnings import warn
    install_modules = [
        'requests==2.9.1',
        'google-api-python-client==1.5.1',
        'oauth2client==2.0.2',
    ]
    warn('cloudwrapper.gce requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise

class GoogleComputeEngine(object):

    def __init__(self):
        self.server = "http://metadata/computeMetadata/v1/instance/"
        self.headers = {"Metadata-Flavor": "Google"}
        self._id = None
        self._name = None
        self._hostname = None
        self._zone = None
        self._externalIp = None
        self._internalIp = None
        self._projectId = None
        self._credentials = None
        self._client_ce = None
        try:
            self._id = requests.get(self.server + "id", headers=self.headers).text
            self._reconnect()
            self._projectId = requests.get('http://metadata/computeMetadata/v1/project/project-id', headers=self.headers).text
        except (requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as ex:
            self.is_instance = False
        else:
            self.is_instance = True


    def _reconnect(self):
        self._credentials = GoogleCredentials.get_application_default()
        self._client_ce = build('compute', 'v1', credentials=self._credentials)


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
            try:
                self._name = requests.get(self.server + "name", headers=self.headers).text
            except:
                # Missing name attribute in metadata server
                # parse name from the hostname
                hostname = self.instanceHostname()
                # Pattern of hostname: name.c.project.internal
                parts = hostname.split('.')
                self._name = '.'.join(parts[:-3])
        return self._name


    def instanceHostname(self):
        if not self.is_instance:
            return ''
        if self._hostname is None:
            self._hostname = requests.get(self.server + "hostname", headers=self.headers).text
        return self._hostname


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


    def regionQuotas(self, region_name):
        if not self.is_instance:
            return None
        request = self._client_ce.regions().get(
            project=self.projectId(),
            region=region_name
        )
        region = request.execute(num_retries=6)
        return region['quotas'] if 'quotas' in region else None


    def regionsQuotas(self):
        if not self.is_instance:
            return None
        regions_list = {}
        request = self._client_ce.regions().list(
            project=self.projectId()
        )
        while request is not None:
            response = request.execute(num_retries=6)
            for region in response['items']:
                regions_list[region['name']] = region['quotas']
            request = self._client_ce.regions().list_next(
                previous_request=request,
                previous_response=response
            )
        return regions_list


    def regionsZones(self):
        if not self.is_instance:
            return None
        regions_list = {}
        request = self._client_ce.regions().list(
            project=self.projectId()
        )
        while request is not None:
            response = request.execute(num_retries=6)
            for region in response['items']:
                regions_list[region['name']] = [z.split('/')[-1] for z in region['zones']]
            request = self._client_ce.regions().list_next(
                previous_request=request,
                previous_response=response
            )
        return regions_list

