"""
Base classes.

BaseQueue
BaseBucket

Copyright (C) 2018 Klokan Technologies GmbH (http://www.klokantech.com/)
"""


class BaseQueue(object):

    def empty(self):
        return self.qsize() == 0

    @staticmethod
    def full():
        return False

    def put_nowait(self, item):
        return self.put(item, False)

    def get_nowait(self):
        return self.get(False)


class BaseBucket(object):

    def put(self, source, target):
        raise NotImplementedError

    def get(self, source, target):
        raise NotImplementedError

    def has(self, source):
        raise NotImplementedError

    def list(self, prefix=None):
        raise NotImplementedError

    def size(self, source):
        raise NotImplementedError

    def is_public(self, source):
        return True

    def make_public(self, source):
        pass
