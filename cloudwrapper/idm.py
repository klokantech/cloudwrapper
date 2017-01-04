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

try:
    from influxdb import InfluxDBClient
except ImportError:
    from warnings import warn
    install_modules = [
        'influxdb==3.0.0',
    ]
    warn('cloudwrapper.idm requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise

class IdmConnection(object):

    def __init__(self, user, pswd, host='localhost', port=8086, db='metrics'):
        self.host = host
        self.port = int(port)
        self.client = InfluxDBClient(self.host, self.port, user, pswd, db)
        self.client.create_database(db)
        self.globalLabels = {}


    def addGlobalLabel(self, name, value):
        """
        Add one label with value into global labels.
        """
        self.globalLabels[name] = value


    def setGlobalLabels(self, labels, append=False):
        """
        Set global labels for this metric.

        Default clear previous labels, unless append is True
        """
        if not append:
            self.globalLabels = {}
        self.globalLabels.update(labels)


    def metric(self, name):
        return Metric(name, self.client, self.globalLabels.copy())



class Metric(object):

    def _format_rfc3339(self, dt):
        """
        Formats a datetime per RFC 3339.
        :param dt: Datetime instance to format, defaults to utcnow
        """
        return dt.isoformat("T") + "Z"


    def __init__(self, name, client, globalLabels=None):
        """
        Create Metric object with name, using client connection.

        Setup my_hostname used for each metric value in write() method
        """
        self.metricName = name
        self.client = client
        self.points = []
        self.my_hostname = socket.gethostname()
        if globalLabels is None:
            globalLabels = {}
        self.globalLabels = globalLabels


    def name(self):
        return self.metricName


    def create(self):
        # No creation is necessary
        pass


    def get(self):
        # TODO what to return? Schema of metric table?
        pass


    def has(self):
        """
        Each metric exists all the time in Influx DB.
        """
        return True


    def read(self, startTime=None, endTime=None, pageSize=10):
        """
        Read values for this metric.

        To be implemented.
        """
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


    def addGlobalLabel(self, name, value):
        """
        Add one label with value into global labels.
        """
        self.globalLabels[name] = value


    def setGlobalLabels(self, labels, append=False):
        """
        Set global labels for this metric.

        Default clear previous labels, unless append is True
        """
        if not append:
            self.globalLabels = {}
        self.globalLabels.update(labels)


    def _addPoint(self, value, labels=None, startTime=None, endTime=None):
        """
        Add one point value with optional labels.

        Label parameter doesn't use global labels
        Default value for startTime and endTime is time.now()
        """

        if startTime is None:
            startTime = datetime.datetime.utcnow()
        elif not isinstance(startTime, datetime):
            raise Exception('Datetime object is required as startTime!')

        if labels is None:
            labels = {}

        # if endTime is None:
        #     endTime = datetime.datetime.utcnow()
        # elif not isinstance(startTime, datetime):
        #     raise Exception('Datetime object is required as endTime!')

        self.points.append({
            # Time requires nanoseconds since 1.1. 1970 UTC
            # 'time': int(startTime.strftime('%s')),
            'measurement': self.metricName,
            'fields': {
                'value': value,
            },
            'tags': labels
        })


    def write(self, value, startTime=None, endTime=None, metricLabels=None):
        """
        Write one value into this metric.

        Default value for startTime and endTime is time.now()
        Each metric value has label with hostname of current machine
        """
        # if len(self.points) == 0:
        #     raise Exception('Missing at least one point of metric to write!')
        self.points = []

        labels = self.globalLabels.copy()
        if 'hostname' not in labels:
            labels.update({
                'hostname': self.my_hostname
            })
        if metricLabels is not None:
            labels.update(metricLabels)

        self._addPoint(value, labels, startTime, endTime)
        lastException = None
        for _repeat in range(6):
            try:
                self.client.write_points( self.points )
                self.points = []
                return True
            except IOError as e:
                sleep(_repeat * 2 + 1)
                lastException = e
        if lastException is not None:
            raise lastException

