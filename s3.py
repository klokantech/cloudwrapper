"""Amazon S3.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Vaclav Klusak <vaclav.klusak@klokantech.com>
"""

import os

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

    PART_LIMIT = (4 << 30)  # 4 GB

    def __init__(self, handle):
        self.handle = handle

    def put(self, source, target):
        key = self.handle.new_key(target)
        source_size = os.stat(source).st_size
        if source_size <= self.PART_LIMIT:
            key.set_contents_from_filename(source)
            return
        multipart = self.handle.initiate_multipart_upload(target)
        try:
            with open(source, 'rb') as fp:
                offsets = xrange(0, source_size, self.PART_LIMIT)
                for part, offset in enumerate(offsets, start=1):
                    size = min(source_size, offset + self.PART_LIMIT) - offset
                    fp.seek(offset)
                    multipart.upload_part_from_file(fp, part, size=size)
            multipart.complete_upload()
        except:
            multipart.cancel_upload()
            raise

    def get(self, source, target):
        key = self.handle.get_key(source, validate=False)
        key.get_contents_to_filename(target)
