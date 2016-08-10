"""Google Cloud Logging.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import logging
import json
import errno

from time import sleep
from datetime import datetime

try:
    from googleapiclient.discovery import build
    from oauth2client.client import GoogleCredentials
except ImportError:
    from warnings import warn
    install_modules = [
        'google-api-python-client==1.5.1',
        'oauth2client==2.0.2',
        'requests==2.9.1',
    ]
    warn('cloudwrapper.gcl requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise

from .gce import GoogleComputeEngine


class GclConnection(object):

    def __init__(self):
        credentials = GoogleCredentials.get_application_default()
        self.connection = build('logging', 'v2beta1', credentials=credentials)


    def handler(self, projectId, logId):
        return Handler(self.connection, projectId, logId)



class Handler(logging.Handler):

    def __init__(self, connection, projectId, logId, *args, **kwargs):
        super(Handler, self).__init__(*args, **kwargs)
        self.connection = connection
        self.projectId = projectId
        self.logId = logId
        self.gce = GoogleComputeEngine()
        self.token = None
        self.entries = []
        self.body = {
            'logName': 'projects/{}/logs/{}'.format(projectId, logId),
            'resource': {
                'type': 'gce_instance' if self.gce.isInstance() else 'none',
                'labels': {
                    'instance_id': self.gce.instanceId(),
                    'zone': self.gce.instanceZone(),
                }
            },
            'labels': {
                'internal_ip': self.gce.instanceInternalIP(),
                'external_ip': self.gce.instanceExternalIP(),
                'instance_name': self.gce.instanceName()
            },
            'entries': [],
        }


    def emit(self, record):
        d = datetime.utcnow() # <-- get time in UTC
        self.entries.append({
            'timestamp': d.isoformat("T") + "Z",
            'jsonPayload': json.loads(self.format(record)),
            'severity': record.levelname
        })


    def flush(self):
        if not self.entries:
            return
        for _repeat in range(6):
            try:
                self.body['entries'] = self.entries
                resp = self.connection.entries().write(
                    body=self.body).execute()
                self.entries = []
                break
            except IOError as e:
                sleep(_repeat * 2 + 1)
                if e.errno == errno.EPIPE:
                    credentials = GoogleCredentials.get_application_default()
                    self.connection = build('logging', 'v2beta1', credentials=credentials)
            except Exception:
                sleep(_repeat * 2 + 5)


    def list(self, filter=None, orderAsc=True):
        logFilter = []
        logFilter.append('logName="projects/{}/logs/{}"'.format(self.projectId, self.logId))
        if self.gce.isInstance():
            logFilter.append('resource.type="gce_instance"')
        if filter is not None:
            logFilter.append('({})'.format(filter))
        body = {
            'orderBy': 'timestamp {}'.format('asc' if orderAsc else 'desc'),
            'pageSize': 1000,
            'filter': ' AND '.join(logFilter),
            'projectIds': [
                self.projectId
            ]
        }
        req = self.connection.entries().list(body=body)
        while req:
            resp = req.execute(num_retries=6)
            nextPageToken = str(resp.get('nextPageToken', ''))
            entries = resp.get('entries', [])
            for entry in entries:
                payload = entry.get('jsonPayload', {})
                yield payload
            if len(nextPageToken) == 0:
                break
            body['pageToken'] = nextPageToken
            req = self.connection.entries().list(body=body)

