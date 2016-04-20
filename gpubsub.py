"""Google Pub Sub using GCE Authentication.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import os
import errno
import base64
import json

from time import sleep
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

from .gce import GoogleComputeEngine

class GpubsubConnection(object):

    def __init__(self):
        self.credentials = GoogleCredentials.get_application_default()
        self.client = build('pubsub', 'v1', credentials=self.credentials)
        self.gce = GoogleComputeEngine()


    def topic(self, name):
        return Topic(name, self.client, self.credentials, self.gce.projectId())


    def subscription(self, name):
        return Subscription(name, self.client, self.credentials, self.gce.projectId())



class Topic(object):

    def __init__(self, name, handle, credentials, projectId):
        self.name = name
        self.handle = handle
        self.credentials = credentials
        self.projectId = projectId
        self.topicId = 'projects/{}/topics/{}'.format(self.projectId, name)


    def publish(self, message):
        msg = base64.b64encode( json.dumps(message) )
        body = {
            "messages": [
                {
                    "data": msg
                },
            ]
        }
        self.handle.projects().topics().publish(
            topic=self.topicId,
            body=body).execute(num_retries=6)



class Subscription(object):

    def __init__(self, name, handle, credentials, projectId):
        self.name = name
        self.handle = handle
        self.credentials = credentials
        self.projectId = projectId
        self.subscriptionId = 'projects/{}/subscriptions/{}'.format(self.projectId, name)


    def list(self, maxCount=100):
        body = {
            "returnImmediately": True,
            "maxMessages": maxCount,
        }
        resp = self.handle.projects().subscriptions().pull(
            subscription=self.subscriptionId, body=body).execute(num_retries=6)

        receivedMessages = resp.get('receivedMessages')
        if receivedMessages is not None:
            for message in receivedMessages:
                msg = message.get('message')
                if msg:
                    yield json.loads( base64.b64decode( str(msg.get('data')) ) )

