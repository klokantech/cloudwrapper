"""Google Cloud logging."""

import logging
import time
import json

from datetime import datetime
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

from .gce import GoogleComputeEngine

class GoogleLoggingConnection(object):

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
                    'zone': self.gce.instanceZone()
                }
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
        for _ in range(6):
            try:
                self.body['entries'] = self.entries
                resp = self.connection.entries().write(
                    body=self.body).execute()
                self.entries = []
                break
            except Exception:
                time.sleep(30)
