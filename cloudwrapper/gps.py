"""Google PubSub using GCE Authentication.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import os
import errno
import base64
import json
import sys

if sys.version[0] == '2':
    from Queue import Empty
else:
    from queue import Empty

from time import sleep

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
    warn('cloudwrapper.gps requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise

from .gce import GoogleComputeEngine


class GpsConnection(object):

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
        msg = base64.b64encode( json.dumps(message, separators=(',', ':')) )
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


    def put(self, item, block=True, timeout=None, delay=None):
        """Put item into the queue.

        Note that PubSub doesn't implement non-blocking or timeouts for writes,
        so both 'block' and 'timeout' must have their default values only.
        """
        if not (block and timeout is None):
            raise Exception('GpubsubConnection::Topic::put() - Block and timeout must have default values.')
        self.publish(item)



class Subscription(object):

    def __init__(self, name, handle, credentials, projectId):
        self.name = name
        self.handle = handle
        self.credentials = credentials
        self.projectId = projectId
        self.subscriptionId = 'projects/{}/subscriptions/{}'.format(self.projectId, name)
        self.available_timestamp = None


    def list(self, maxCount=100):
        """Pull list of messages (default 100) from the subscriber.
        """
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


    def _get_message(self, block=True):
        """Internal pull one message from the subscriber.
        """
        body = {
            "returnImmediately": not block,
            "maxMessages": 1,
        }
        resp = self.handle.projects().subscriptions().pull(
            subscription=self.subscriptionId,
            body=body).execute(num_retries=6)
        receivedMessages = resp.get('receivedMessages')
        if receivedMessages is not None:
            for message in receivedMessages:
                msg = message.get('message')
                if msg:
                    return message
        return None


    def pull(self, block=True, timeout=None):
        """Pull one message from the subscriber.
        """
        self.message = self._get_message(block)
        if block:
            while self.message is None:
                sleep()
                # Sleep at least 20 seconds before next message receive
                sleep(timeout if timeout is not None else 20)
                self.message = self._get_message(block)

        if self.message is None:
            raise Empty
        data = self.message.get('message').get('data')
        if not data:
            raise Empty

        return json.loads( base64.b64decode( str(data) ) )


    def get(self, block=True, timeout=None):
        """Get (pull) one message from the subscriber.
        """
        return self.pull(block=block, timeout=timeout)


    def acknowledge(self):
        """Acknowledge that a formerly enqueued message is complete.

        Note that this method MUST be called for each item.
        """
        if self.message is None:
            raise Exception('No message to acknowledge.')
        body = {
            "ackIds": [ self.message.get('ackId') ],
        }
        resp = self.handle.projects().subscriptions().acknowledge(
            subscription=self.subscriptionId,
            body=body).execute(num_retries=6)
        # Response should be empty
        if resp:
            raise Exception(resp)
        self.message = None


    def task_done(self):
        """Acknowledge that a formerly enqueued message is complete.

        Note that this method MUST be called for each item.
        """
        self.acknowledge()


    def update(self, lease_time=600, msg=None):
        """Update lease time for a formerly enqueued message.
        """
        if msg is None:
            msg = self.message
        if msg is None:
            raise Exception('No message to update.')
        body = {
            "ackDeadlineSeconds": int(lease_time),
            "ackIds": [ msg.get('ackId') ],
        }
        resp = self.handle.projects().subscriptions().modifyAckDeadline(
            subscription=self.subscriptionId,
            body=body).execute(num_retries=6)
        # Response should be empty
        if resp:
            raise Exception(resp)


    def has_available(self):
        """Is any message available for lease.

        If there is no message, this state is cached internally for 5 minutes.
        10 minutes is time used for Google Autoscaler.
        """
        now = time.time()
        # We have cached False response
        if self.available_timestamp is not None and now < self.available_timestamp:
            return False

        # Get oldestTask from queue stats
        exc = None
        for _repeat in range(6):
            try:
                msg = self._get_message(block=False)
                break
            except Exception as e:
                sleep(_repeat * 2 + 1)
                exc = e
        else:
            if exc is not None:
                raise exc
            return False
        # There is at least one availabe task
        if msg is not None:
            self.update(lease_time=2, msg=msg)
            return True
        # No available task, cache this response for 5 minutes
        self.available_timestamp = now + 300 # 5 minutes
        return False

