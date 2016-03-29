"""Google Task Pull Queues."""

from Queue import Empty

from time import sleep
from gcloud_taskqueue import Taskqueue, Client
from gcloud.exceptions import GCloudError

from .base import BaseQueue

import json
import errno


class GtqConnection(object):

    def __init__(self):
        self.client = Client()

    def queue(self, name):
        return Queue(Taskqueue(client=self.client, id=name), self.client)


class Queue(BaseQueue):
    """
    Google Task Pull Queues

    Note that items gotten from the queue MUST be acknowledged by
    calling task_done(). Otherwise they will appear back in the
    queue, after a period of time called 'visibility time'. This
    parameter, and others, are configured outside this module.
    """

    def __init__(self, handle, client):
        self.handle = handle
        self.client = client
        self.message = None

    def qsize(self):
        """WARNING! Not implemented in gcloud_taskqueue module
        REST API available:
        GET https://www.googleapis.com/taskqueue/v1beta2/projects/project/taskqueues/taskqueue?getStats=true
        Response: see https://cloud.google.com/appengine/docs/python/taskqueue/rest/taskqueues#resource
        """
        return 0

    def put(self, item, block=True, timeout=None, delay=None):
        """Put item into the queue.

        Note that GTQ doesn't implement non-blocking or timeouts for writes,
        so both 'block' and 'timeout' must have their default values only.
        """
        if not (block and timeout is None):
            raise Exception('block and timeout must have default values')
        for _ in range(6):
            try:
                self.handle.insert_task(description=json.dumps(item), client=self.client)
                break
            except IOError as e:
                if e.errno == errno.EPIPE:
                    self.client = Client()
                sleep(10)
            except GCloudError:
                sleep(30)

    def _get_message(self, lease_time):
        """Get one message with lease_time
        """
        for _ in range(6):
            try:
                for task in self.handle.lease(lease_time=lease_time, num_tasks=1, client=self.client):
                    return task
                return None
            except IOError as e:
                if e.errno == errno.EPIPE:
                    self.client = Client()
                sleep(10)
            except GCloudError:
                sleep(30)

    def get(self, block=True, timeout=None, lease_time=3600):
        """Get item from the queue.

        Default lease_time is 1 hour.
        """

        self.message = self._get_message(lease_time)
        if block:
            while self.message is None:
                # Sleep at least 20 seconds before next message receive
                sleep(timeout if timeout is not None else 20)
                self.message = self._get_message(lease_time)

        if self.message is None:
            raise Empty

        return json.loads(self.message.description)

    def task_done(self):
        """Acknowledge that a formerly enqueued task is complete.

        Note that this method MUST be called for each item.
        See the class docstring for details.
        """
        if self.message is None:
            raise Exception('no message to acknowledge')
        for _ in range(6):
            try:
                self.message.delete(client=self.client)
                self.message = None
                break
            except IOError as e:
                if e.errno == errno.EPIPE:
                    self.client = Client()
                sleep(10)
            except GCloudError:
                sleep(30)
