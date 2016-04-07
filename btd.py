"""BeansTalkd Queues."""

from Queue import Empty

from time import sleep

from .base import BaseQueue

import json
import time
import beanstalkc


class BtdConnection(object):

    def __init__(self, host='localhost', port=11300):
        self.host = host
        self.port = int(port)

    def queue(self, name):
        return Queue(beanstalkc.Connection(self.host, self.port), name)


class Queue(BaseQueue):
    """
    BeansTalkd Queues

    Note that items gotten from the queue MUST be acknowledged by
    calling task_done(). Otherwise they will appear back in the
    queue, after a period of time called 'visibility time'. This
    parameter, and others, are configured outside this module.
    """

    def __init__(self, handle, name):
        self.handle = handle
        self.name = name
        self.handle.use(self.name)
        self.handle.watch(self.name)
        # Ignore others tubes
        for tube in self.handle.watching():
            if not self.name == tube:
                self.handle.ignore(tube)
        self.message = None
        self.available_timestamp = None

    def qsize(self):
        """Get size of ready jobs in current tube
        """
        stats = self.handle.stats_tube(self.name)
        return stats['current-jobs-ready']

    def put(self, item, block=True, timeout=None, delay=0, ttr=3600, priority=beanstalkc.DEFAULT_PRIORITY):
        """Put item into the queue.

        Note that BeansTalkc doesn't implement non-blocking or timeouts for writes,
        so both 'block' and 'timeout' must have their default values only.

        Default ttr (Time To Release) is 1 hour.
        """
        if not (block and timeout is None):
            raise Exception('block and timeout must have default values')
        self.handle.put(json.dumps(item), ttr=ttr, delay=delay, priority=priority)

    def get(self, block=True, timeout=None):
        """Get item from the queue.
        """
        self.message = None
        if block and timeout is None:
            self.message = self.handle.reserve(timeout=timeout)
        elif not block and not timeout is None:
            self.message = self.handle.reserve(timeout=timeout)
        else:
            raise Exception('invalid arguments')

        if self.message is None:
            raise Empty
        return json.loads(self.message.body)

    def task_done(self):
        """Acknowledge that a formerly enqueued task is complete.

        Note that this method MUST be called for each item.
        See the class docstring for details.
        """
        if self.message is None:
            raise Exception('no message to acknowledge')
        self.message.delete()
        self.message = None

    def touch(self):
        """Touch actual task to extend time to release
        """
        if self.message is None:
            raise Exception('no message to acknowledge')
        self.message.touch()


    def has_available(self):
        """Is any message available for lease.

        If there is no message, this state is cached internally for 5 minutes.
        10 minutes is time used for Google Autoscaler.
        """
        now = time.time()
        # We have cached False response
        if self.available_timestamp is not None and self.available_timestamp < now:
            return False

        # Get oldestTask from queue stats
        exc = None
        for _ in range(3):
            try:
                stats = self.handle.stats_tube(self.name)
                break
            except IOError as e:
                exc = e
                sleep(10)
        else:
            if exc is not None:
                raise exc
            return False
        # There is at least one availabe task
        if int(stats['current-jobs-ready']) > 0:
            return True
        # No available task, cache this response for 5 minutes
        self.available_timestamp = now + 300 # 5 minutes
        return False

