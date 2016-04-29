"""
Influx DB Metric.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import json
import errno
import datetime
import socket

from time import sleep
from influxdb import InfluxDBClient

class IdmConnection(object):

    def __init__(self, user, pswd, host='localhost', port=8086, db='metrics'):
        self.host = host
        self.port = int(port)
        self.client = InfluxDBClient(self.host, self.port, user, pswd, db)
        self.client.create_database(db, if_not_exists=True)


    def metric(self, name):
        return Metric(name, self.client)


class Metric(object):

    def _format_rfc3339(self, dt):
        """Formats a datetime per RFC 3339.
        :param dt: Datetime instanec to format, defaults to utcnow
        """
        return dt.isoformat("T") + "Z"


    def __init__(self, name, client):
        self.metricName = name
        self.client = client
        self.points = []
        self.my_hostname = socket.gethostname()


    def name(self):
        return self.metricName


    def create(self):
        # No creation is necessary
        pass


    def get(self):
        # TODO what to return? Schema of metric table?
        pass


    def has(self):
        # Each metric exists all the time
        return True


    def read(self, startTime=None, endTime=None, pageSize=10):
        # TODO
        return []
        # try:
        #     if startTime is None:
        #         startTime = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
        #     elif not isinstance(startTime, datetime):
        #         raise Exception('Datetime object is required as startTime!')
        #     if endTime is None:
        #         endTime = datetime.datetime.utcnow()
        #     elif not isinstance(endTime, datetime):
        #         raise Exception('Datetime object is required as endTime!')
        #     request = self.client.timeseries().list(
        #         project=self.projectId,
        #         mentric="{}/{}".format(
        #             self.CUSTOM_METRIC_DOMAIN,
        #             self.metricName),
        #         count=pageSize,
        #         # interval_startTime=self._format_rfc3339(startTime),
        #         youngest=self._format_rfc3339(startTime),
        #         # interval_endTime=self._format_rfc3339(endTime)
        #         oldest=self._format_rfc3339(endTime)
        #     )
        #     response = request.execute(num_retries=3)
        #     return response
        # except:
        #     return []


    def _addPoint(self, value, tags=None, startTime=None, endTime=None):
        if self.valueType is None:
            # Read and save valueType
            self.get()

        if startTime is None:
            startTime = datetime.datetime.utcnow()
        elif not isinstance(startTime, datetime):
            raise Exception('Datetime object is required as startTime!')

        if tags is None:
            tags = {}

        # if endTime is None:
        #     endTime = datetime.datetime.utcnow()
        # elif not isinstance(startTime, datetime):
        #     raise Exception('Datetime object is required as endTime!')

        self.points.append({
            'time': int(startTime.strftime('%s')),
            'measurement': self.metricName,
            'fields': {
                'value': value,
            },
            'tags': tags
        })


    def write(self, value, startTime=None, endTime=None, metricLabels=None):
        # if len(self.points) == 0:
        #     raise Exception('Missing at least one point of metric to write!')
        self.points = []

        if metricLabels is not None:
            metricLabels.update({
                'hostname': self.my_hostname
            })

        self._addPoint(value, metricLabels, startTime, endTime)
        lastException = None
        for _ in range(6):
            try:
                self.client.write_points( self.points )
                self.points = []
                return True
            except IOError as e:
                lastException = e
                sleep(10)
        if lastException is not None:
            raise lastException

