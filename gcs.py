"""Google Cloud Storage using GCE Authentication."""

import os
import errno
import httplib2

from time import sleep
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from apiclient.http import MediaIoBaseDownload

class GcsConnection(object):

    def __init__(self):
        self.credentials = GoogleCredentials.get_application_default()

    def bucket(self, name):
        for _ in range(6):
            try:
                return Bucket(discovery.build('storage', 'v1', credentials=self.credentials), name)
            except IOError as e:
                if e.errno == errno.EPIPE:
                    self.credentials = GoogleCredentials.get_application_default()
                sleep(10)



class Bucket(object):


    CHUNK_SIZE = (500 << 20)  # 500 MB

    def __init__(self, handle, name):
        self.handle = handle
        self.name = name

    def put(self, source, target):
        # Resumable has to be True, otherwise it is not working correctly
        media = MediaFileUpload(source, chunksize=self.CHUNK_SIZE, resumable=True)
        request = self.handle.objects().insert(
            bucket=self.name,
            name=target,
            media_body=media)

        response = None
        num_retries = 5
        while response is None:
            error = None
            try:
                progress, response = request.next_chunk()
            except HttpError, err:
                if err.resp.status < 500:
                    raise err
                error = err
            except (httplib2.HttpLib2Error, IOError), err:
                error = err
            if error:
                if num_retries > 0:
                    num_retries -= 1
                    sleep(10)
                else:
                    raise error


    def get(self, source, target):
        # Use get_media instead of get to get the actual contents of the object.
        # http://g.co/dev/resources/api-libraries/documentation/storage/v1/python/latest/storage_v1.objects.html#get_media
        request = self.handle.objects().get_media(bucket=self.name, object=source)
        with open(target, 'wb') as out_file:
            downloader = MediaIoBaseDownload(out_file, request, chunksize=self.CHUNK_SIZE)
            done = False
            num_retries = 5
            while not done:
                error = None
                try:
                    status, done = downloader.next_chunk()
                except HttpError, err:
                    if err.resp.status < 500:
                        raise err
                    error = err
                except (httplib2.HttpLib2Error, IOError), err:
                    error = err
                if error:
                    if num_retries > 0:
                        num_retries -= 1
                        sleep(10)
                    else:
                        raise error


    def has(self, source):
        request = self.handle.objects().get(bucket=self.name, object=source)
        resp, _ = request.http.request(request.uri, 'GET')
        return resp.status == 200
