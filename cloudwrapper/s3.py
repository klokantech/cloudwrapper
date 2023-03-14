"""Amazon S3.

Copyright (C) 2016-2020 Klokan Technologies GmbH (https://www.klokantech.com/)
Author: Vaclav Klusak <vaclav.klusak@klokantech.com>
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import os

from .base import BaseBucket

try:
    from boto.s3 import connect_to_region, connection
    from boto.exception import S3ResponseError
    from boto.s3.connection import ProtocolIndependentOrdinaryCallingFormat
except ImportError:
    from warnings import warn
    install_modules = [
        'boto==2.48.0',
    ]
    warn('cloudwrapper.s3 requires these packages:\n  - {}'.format(
        '\n  - '.join(install_modules)))
    raise

try:
    for _ in xrange(1):
        pass
except NameError:
    xrange = range


class S3Connection(object):

    def __init__(self, region, key=None, secret=None, host=None, anon=None):
        if region is None and host is not None:
            self.connection = connection.S3Connection(
                host=host,
                aws_access_key_id=key,
                aws_secret_access_key=secret,
                anon=anon,
                calling_format=ProtocolIndependentOrdinaryCallingFormat())
        else:
            self.connection = connect_to_region(
                region,
                aws_access_key_id=key,
                aws_secret_access_key=secret,
                calling_format=ProtocolIndependentOrdinaryCallingFormat())

    def bucket(self, name, create=False):
        for _ in range(6):
            try:
                return Bucket(self.connection.get_bucket(name))
            except S3ResponseError as se:
                if se.status == 404 and create:
                    self.connection.create_bucket(name)
                    continue
                raise


class Bucket(BaseBucket):

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

    def has(self, source):
        key = self.handle.get_key(source, validate=False)
        return key.exists()

    def list(self, prefix=None):
        for key in self.handle.get_all_keys(prefix=prefix):
            yield key
