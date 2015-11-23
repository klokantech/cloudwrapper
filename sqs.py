"""Amazon SQS."""

from Queue import Empty

from boto.sqs import connect_to_region
from boto.sqs.jsonmessage import JSONMessage

from .base import BaseQueue


class SqsConnection(object):

    def __init__(self, region, key=None, secret=None):
        self.connection = connect_to_region(
            region,
            aws_access_key_id=key,
            aws_secret_access_key=secret)

    def queue(self, name):
        return Queue(self.connection.get_queue(name))


class Queue(BaseQueue):
    """
    Amazon SQS queue.

    Note that items gotten from the queue MUST be acknowledged by
    calling task_done(). Otherwise they will appear back in the
    queue, after a period of time called 'visibility time'. This
    parameter, and others, are configured outside this module.
    """

    def __init__(self, handle):
        handle.set_message_class(JSONMessage)
        self.handle = handle
        self.message = None

    def qsize(self):
        return self.handle.count()

    def put(self, item, block=True, timeout=None):
        """Put item into the queue.

        Note that SQS doesn't implement non-blocking or timeouts for writes,
        so both 'block' and 'timeout' must have their default values only.
        """
        if not (block and timeout is None):
            raise Exception('block and timeout must have default values')
        self.handle.write(self.handle.new_message(item))

    def get(self, block=True, timeout=None):
        """Get item from the queue.

        Note that SQS can block either indefinitely, or between 1 and 20 seconds.
        """
        if block and timeout is None:
            self.message = self.handle.read(wait_time_seconds=20)
            while self.message is None:
                self.message = self.handle.read(wait_time_seconds=20)
        elif block and 1 <= timeout <= 20:
            self.message = self.handle.read(wait_time_seconds=timeout)
        elif not block and timeout is None:
            self.message = self.handle.read(wait_time_seconds=0)
        else:
            raise Exception('invalid arguments')
        if self.message is None:
            raise Empty
        return self.message.get_body()

    def task_done(self):
        """Acknowledge that a formerly enqueued task is complete.

        Note that this method MUST be called for each item.
        See the class docstring for details.
        """
        if self.message is None:
            raise Exception('no message to acknowledge')
        self.handle.delete_message(self.message)
        self.message = None