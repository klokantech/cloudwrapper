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
