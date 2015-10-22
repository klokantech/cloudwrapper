"""Amazon S3."""

from boto.s3 import connect_to_region


class S3Connection(object):

    def __init__(self, region, key=None, secret=None):
        self.connection = connect_to_region(
            region,
            aws_access_key_id=key,
            aws_secret_access_key=secret)

    def bucket(self, name):
        return Bucket(self.connection.get_bucket(name))


class Bucket(object):

    def __init__(self, handle):
        self.handle = handle

    def put(self, source, target):
        key = self.handle.new_key(target)
        key.set_contents_from_filename(source)

    def get(self, source, target):
        key = self.handle.get_key(source, validate=False)
        key.get_contents_to_filename(target)
