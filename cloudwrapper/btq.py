"""BeansTalkd Queues.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import json
import sys
import errno

from time import sleep, time
from .base import BaseQueue

if sys.version[0] == '2':
    from Queue import Empty
else:
    from queue import Empty

try:
    import yaml  # noqa
    from beanstalkc import Connection, SocketError, DEFAULT_PRIORITY
except ImportError:
    from warnings import warn
    install_modules = [
        'beanstalkc3==0.4.0',
        'pyyaml==3.11',
    ]
    warn('cloudwrapper.btq requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise


class BtqConnection(object):

    def __init__(self, host='localhost', port=11300, max_size=65300):
        self.host = host
        self.port = int(port)
        self.max_size = max(int(max_size or 0), 65300)


    def queue(self, name):
        return Queue(Connection(self.host, self.port), name, self.max_size)


    def clear(self, name):
        q = self.queue(name)
        q.clear()



class Queue(BaseQueue):
    """
    BeansTalkd Queues

    Note that items gotten from the queue MUST be acknowledged by
    calling task_done(). Otherwise they will appear back in the
    queue, after a period of time called 'visibility time'. This
    parameter, and others, are configured outside this module.
    """

    def __init__(self, handle, name, max_size):
        self.handle = handle
        self.name = name
        self.max_size = max_size
        self.message = None
        self.available_timestamp = None

        self.reconnectAttempts = 6
        self.reconnectTimeout = 2

        self._reconnect()


    def _reconnect(self):
        for _repeat in range(self.reconnectAttempts):
            try:
                self.handle.reconnect()
                self.handle.use(self.name)
                self.handle.watch(self.name)
                break
            except SocketError:
                sleep(_repeat * self.reconnectTimeout + 1)
        else:
            raise Exception('Cannot reconnect to the beanstalk server.')
        # Ignore others tubes
        for tube in self.handle.watching():
            if not self.name == tube:
                self.handle.ignore(tube)


    def _wrap_handle(self, method, *args, **kwargs):
        for _repeat in range(self.reconnectAttempts):
            try:
                return getattr(self.handle, method)(*args, **kwargs)
            except IOError as e:
                if e.errno == errno.EPIPE:
                    sleep(_repeat * self.reconnectTimeout + 1)
                    self._reconnect()
            except SocketError:
                sleep(_repeat * self.reconnectTimeout + 1)
                self._reconnect()
        return None


    def verify_task(self, task):
        try:
            self.serialize_task(task)
            return True
        except:
            return False


    def serialize_task(self, task):
        task_s = json.dumps(task, separators=(',', ':'))
        if len(task_s) > self.max_size:
            raise Exception('This task is larger than allowed size {}.'.format(
                self.max_size))
        return task_s


    def deserialize_task(self, task):
        try:
            task_o = json.loads(task)
            return task_o
        except:
            return task


    def setReconnectOptions(self, attempts, timeout):
        """Set reconnect options: number of attempts, timeout before reconnect [s]
        """
        self.reconnectAttempts = attempts
        self.reconnectTimeout = timeout


    def qsize(self):
        """Get size of ready and reserved jobs in current tube
        """
        stats = self._wrap_handle('stats_tube', self.name)
        num = 0
        if 'current-jobs-ready' in stats:
            num += stats['current-jobs-ready']
        if 'current-jobs-reserved' in stats:
            num += stats['current-jobs-reserved']
        if 'current-jobs-delayed' in stats:
            num += stats['current-jobs-delayed']
        return num


    def put(self, item, block=True, timeout=None, delay=0, ttr=3600, priority=DEFAULT_PRIORITY):
        """Put item into the queue.

        Note that BeansTalkc doesn't implement non-blocking or timeouts for writes,
        so both 'block' and 'timeout' must have their default values only.

        Default ttr (Time To Release) is 1 hour.
        """
        if not (block and timeout is None):
            raise Exception('BtqConnection::Queue::put() - Block and timeout must have default values.')
        self._wrap_handle('put', self.serialize_task(item), ttr=ttr, delay=delay, priority=priority)


    def get(self, block=True, timeout=None):
        """Get an item from the queue.
        """
        self.message = None
        if not( (block and timeout is None) or (not block and timeout is not None) ):
            raise Exception('BtqConnection::Queue::get() - invalid arguments.')

        self.message = self._wrap_handle('reserve', timeout=timeout)
        if self.message is None:
            raise Empty
        return self.deserialize_task(self.message.body)


    def task_done(self):
        """Acknowledge that a formerly enqueued task is complete.

        Note that this method MUST be called for each item.
        See the class docstring for details.
        """
        if self.message is None:
            raise Exception('BtqConnection::Queue::task_done() - no message to acknowledge.')
        self._wrap_handle('delete', self.message.jid)
        self.message = None


    def touch(self):
        """Touch a formerly enqueued task to extend time to release
        """
        if self.message is None:
            raise Exception('BtqConnection::Queue::touch() - no message to acknowledge.')
        self._wrap_handle('touch', self.message.jid)


    def update(self, lease_time=None):
        """Update TTR (TimeToRelease) for a formerly enqueued task.
        """
        self.touch()


    def release(self, delay=300, priority=0):
        """Release job with optional delay back to ready queue."""
        if self.message is None:
            raise Exception('BtqConnection::Queue::task_done() - no message to acknowledge.')
        self._wrap_handle('release', self.message.jid, priority, delay)
        self.message = None


    def has_available(self):
        """Is any message available for lease.

        If there is no message, this state is cached internally for 5 minutes.
        10 minutes is time used for Google Autoscaler.
        """
        now = time()
        # We have cached False response
        if self.available_timestamp is not None and now < self.available_timestamp:
            return False

        # Get oldestTask from queue stats
        exc = None
        for _repeat in range(self.reconnectAttempts):
            try:
                stats = self.handle.stats_tube(self.name)
                break
            except IOError as e:
                exc = e
                sleep(_repeat * self.reconnectTimeout + 1)
                if e.errno == errno.EPIPE:
                    self._reconnect()
            except SocketError as e:
                exc = e
                sleep(_repeat * self.reconnectTimeout + 1)
                self._reconnect()
        else:
            if exc is not None:
                raise exc
            return False
        # There is at least one availabe task
        if int(stats['current-jobs-ready'] if 'current-jobs-ready' in stats else 0) > 0:
            return True
        # No available task, cache this response for 5 minutes
        self.available_timestamp = now + 300  # 5 minutes
        return False


    def clear(self):
        while True:
            try:
                self.get(block=False, timeout=0)
            except Empty:
                break
            self.task_done()
