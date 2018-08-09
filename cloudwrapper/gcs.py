"""Google Cloud Storage using GCE Authentication.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import errno

from time import sleep

from .base import BaseBucket

try:
    from gcloud import storage, exceptions
except ImportError:
    from warnings import warn
    install_modules = [
        'gcloud==0.13.0',
    ]
    warn('cloudwrapper.gcs requires these packages:\n  - {}'.format(
        '\n  - '.join(install_modules)))
    raise

try:
    # python2
    from httplib import BadStatusLine, ResponseNotReady
except ImportError:
    # python3
    from http.client import BadStatusLine, ResponseNotReady


class GcsConnection(object):

    def __init__(self):
        self.connection = storage.Client()


    def bucket(self, name, create=False):
        for _repeat in range(6):
            try:
                return Bucket(self.connection.get_bucket(name))
            except (exceptions.NotFound) as e:
                if create:
                    self.connection.create_bucket(name)
                    continue
                raise
            except (IOError, BadStatusLine, ResponseNotReady) as e:
                sleep(_repeat * 2 + 1)
                if e.errno == errno.EPIPE:
                    self.connection = storage.Client()


    def list(self):
        for _repeat in range(6):
            buckets = []
            try:
                for bucket in self.connection.list_buckets():
                    buckets.append(bucket.name)
                break
            except (IOError, BadStatusLine) as e:
                sleep(_repeat * 2 + 1)
                if e.errno == errno.EPIPE:
                    self.connection = storage.Client()
        return buckets


class Bucket(BaseBucket):

    CHUNK_SIZE = (500 << 20)  # 500 MB

    def __init__(self, handle):
        self.handle = handle
        self.name = handle.name


    def _reconnect(self, name):
        connection = storage.Client()
        self.handle = connection.get_bucket(name)


    def put(self, source, target):
        for _repeat in range(6):
            try:
                key = self.handle.blob(target, chunk_size=self.CHUNK_SIZE)
                key.upload_from_filename(source)
                break
            except (IOError, BadStatusLine) as e:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)
            except:
                pass


    def get(self, source, target):
        key = self.handle.get_blob(source)
        if key is None:
            raise Exception("Object {} not exists in bucket {}.".format(
                source, self.handle.id))
        key.chunk_size = self.CHUNK_SIZE
        for _repeat in range(6):
            try:
                key.download_to_filename(target)
            except (IOError, BadStatusLine) as e:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)
                key = self.handle.get_blob(source)
            except:
                pass


    def has(self, source):
        key = self.handle.blob(source)
        return key.exists()


    def list(self, prefix=None):
        for key in self.handle.list_blobs(prefix=prefix):
            yield key


    def size(self, source):
        key = self.handle.get_blob(source)
        return key.size if key is not None else 0


    def is_public(self, source):
        for _repeat in range(6):
            try:
                key = self.handle.get_blob(source)
                if key is None:
                    return False
                return 'READER' in key.acl.all().get_roles()
            except (IOError, BadStatusLine) as e:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)
            except:
                pass


    def make_public(self, source):
        for _repeat in range(6):
            try:
                key = self.handle.get_blob(source)
                if key and not ('READER' in key.acl.all().get_roles()):
                    key.make_public()
                break
            except (IOError, BadStatusLine) as e:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)
            except:
                pass

    def is_remote(self, source):
        return True

    def get_public_url(self, source):
        if self.has(source):
            key = self.handle.blob(source)
            return key.public_url if self.is_public(source) else None
        return None
