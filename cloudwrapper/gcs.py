"""Google Cloud Storage using GCE Authentication.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import os
import errno

from time import sleep

try:
    from gcloud import storage
except ImportError:
    from warnings import warn
    install_modules = [
        'gcloud==0.13.0',
    ]
    warn('cloudwrapper.gcs requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise

try:
    # python2
    from httplib import BadStatusLine
except ImportError:
    # python3
    from http.client import BadStatusLine


class GcsConnection(object):

    def __init__(self):
        self.connection = storage.Client()


    def bucket(self, name):
        for _repeat in range(6):
            try:
                return Bucket(self.connection.get_bucket(name))
            except IOError as e:
                sleep(_repeat * 2 + 1)
                if e.errno == errno.EPIPE:
                    self.connection = storage.Client()



class Bucket(object):

    CHUNK_SIZE = (500 << 20)  # 500 MB

    def __init__(self, handle):
        self.handle = handle


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
                self._reconnect(self.handle.name)
            except:
                pass

        # multipart = self.handle.initiate_multipart_upload(target)
        # try:
        #     with open(source, 'rb') as fp:
        #         offsets = xrange(0, source_size, self.PART_LIMIT)
        #         for part, offset in enumerate(offsets, start=1):
        #             size = min(source_size, offset + self.PART_LIMIT) - offset
        #             fp.seek(offset)
        #             multipart.upload_part_from_file(fp, part, size=size)
        #     multipart.complete_upload()
        # except:
        #     multipart.cancel_upload()
        #     raise


    def get(self, source, target):
        key = self.handle.get_blob(source)
        if key is None:
            raise Exception("Object {} not exists in bucket {}.".format(source, self.handle.id))
        key.chunk_size = self.CHUNK_SIZE
        for _repeat in range(6):
            try:
                key.download_to_filename(target)
            except (IOError, BadStatusLine) as e:
                sleep(_repeat * 2 + 1)
                self._reconnect(self.handle.name)
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
                self._reconnect(self.handle.name)
            except:
                pass
