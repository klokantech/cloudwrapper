"""Google Cloud Storage using GCE Authentication.

Copyright (C) 2016-2020 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import base64
import crc32c
import errno
import os
import struct

from time import sleep

from .base import BaseBucket

try:
    from gcloud import storage, exceptions
except ImportError:
    from warnings import warn
    install_modules = [
        'gcloud==0.18.3',
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


class DifferentHashException(Exception):
        pass


class Crc32cCalculator:
    """The Google Python client doesn't provide a way to stream a file being
       written, so we can wrap the file object in an additional class to
       do custom handling. This is so we don't need to download the file
       and then stream read it again to calculate the hash.
       https://vsoch.github.io/2020/crc32c-validation-google-storage/
   """

    def __init__(self, fileobj):
        self._fileobj = fileobj
        self.crc32 = crc32c.Checksum()

    def write(self, chunk):
        self._fileobj.write(chunk)
        self.crc32.update(chunk)



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
            except (IOError, BadStatusLine, exceptions.GCloudError) as e:
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

    def download_with_verification(self, blob, target):
        if not os.path.exists(target):
            with open(target, "wb") as blob_file:
                parser = Crc32cCalculator(blob_file)
                blob.download_to_file(parser)

            if base64.b64encode(struct.pack(">I", parser.crc32._crc)).decode("utf-8") != source_blob_crc32c:
                os.remove(target)
                raise DifferentHashException("The hash of source and target are different.")


    def put(self, source, target):
        last_ex = None
        for _repeat in range(6):
            try:
                key = self.handle.blob(target, chunk_size=self.CHUNK_SIZE)
                key.upload_from_filename(source)
                break
            except (IOError, BadStatusLine, exceptions.GCloudError) as ex:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)
                last_ex = ex
            except Exception as ex:
                last_ex = ex
        else:
            raise Exception("Object {} cannot put into the bucket {}: {}!".format(
                source, self.handle.id,
                str(last_ex)))

    def get(self, source, target):
        key = self.handle.get_blob(source)
        if key is None:
            raise Exception("Object {} not exists in bucket {}.".format(
                source, self.handle.id))
        key.chunk_size = self.CHUNK_SIZE
        last_ex = None
        for _repeat in range(6):
            try:
                self.download_with_verification(key, target)
                break
            except (IOError, DifferentHashException, BadStatusLine, exceptions.GCloudError) as ex:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)
                key = self.handle.get_blob(source)
                last_ex = ex
            except Exception as ex:
                last_ex = ex
        else:
            raise Exception("Object {} cannot get from the bucket {}: {}!".format(
                source, self.handle.id,
                str(last_ex)))

    def rename(self, source, target):
        key = self.handle.get_blob(source)
        if key is None:
            # Already renamed
            if self.has(target):
                return True
            raise Exception("Object {} not exists in bucket {}.".format(
                source, self.handle.id))
        for _repeat in range(6):
            try:
                self.handle.rename_blob(key, target)
                break
            except:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)
                key = self.handle.get_blob(source)
                if key is None:
                    return self.has(target)
        return self.has(target)

    def has(self, source):
        key = self.handle.blob(source)
        for _repeat in range(6):
            try:
                return key.exists()
            except:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)
                key = self.handle.blob(source)
        else:
            return False

    def list(self, prefix=None):
        for key in self.handle.list_blobs(prefix=prefix):
            yield key

    def size(self, source):
        for _repeat in range(6):
            try:
                key = self.handle.get_blob(source)
                return key.size if key is not None else 0
            except (IOError, BadStatusLine, exceptions.GCloudError):
                sleep(_repeat * 2 + 1)
                self._reconnect(self.name)

    def is_public(self, source):
        for _repeat in range(6):
            try:
                key = self.handle.get_blob(source)
                if key is None:
                    return False
                return 'READER' in key.acl.all().get_roles()
            except (IOError, BadStatusLine, exceptions.GCloudError):
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
            except (IOError, BadStatusLine, exceptions.GCloudError):
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
