"""
Google Custom Metric using API v3.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import json
import errno
import datetime

from time import sleep

try:
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from oauth2client.client import GoogleCredentials
except ImportError:
    from warnings import warn
    install_modules = [
        'google-api-python-client==1.5.1',
        'oauth2client==2.0.2',
        'requests==2.9.1',
    ]
    warn('cloudwrapper.gcm3 requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise

from .gce import GoogleComputeEngine


class GcmConnection(object):

    def __init__(self):
        self.credentials = GoogleCredentials.get_application_default()
        self.client = build('monitoring', 'v3', credentials=self.credentials)


    def metric(self, name, projectId=None):
        return Metric(name, projectId, self.client, self.credentials)


class Metric(object):

    # This is the namespace for all custom metrics
    CUSTOM_METRIC_DOMAIN = "custom.googleapis.com"

    def _format_rfc3339(self, dt):
        """Formats a datetime per RFC 3339.
        :param dt: Datetime instanec to format, defaults to utcnow
        """
        return dt.isoformat("T") + "Z"


    def __init__(self, name, projectId, client, credentials):
        self.metricType = name
        self.gce = GoogleComputeEngine()
        if projectId is None:
            projectId = 'projects/' + self.gce.projectId()
        elif 'projects/' not in projectId:
            projectId = 'projects/' + projectId
        self.projectId = projectId
        self.points = []
        self.valueType = None
        self.metricKind = None
        self.client = client
        self.credentials = credentials


    def _reconnect(self):
        self.credentials = GoogleCredentials.get_application_default()
        self.client = build('monitoring', 'v3', credentials=self.credentials)
        self.gce = GoogleComputeEngine()


    def name(self):
        return self.metricType


    def fullName(self):
        return '{}/{}'.format(self.CUSTOM_METRIC_DOMAIN, self.metricType)


    def create(self, metricKind, valueType='DOUBLE', description='', displayName=None):
        if displayName is None:
            displayName = self.metricType.replace('/', ' ')
        metrics_descriptor = {
            'name': '{}/metricDescriptors/{}/{}'.format(
                self.projectId, self.CUSTOM_METRIC_DOMAIN, self.metricType),
            'type': '{}/{}'.format(self.CUSTOM_METRIC_DOMAIN, self.metricType),
            'metricKind': metricKind,
            'valueType': valueType,
            'unit': 'items',
            'description': description,
            'displayName': displayName,
            'labels': [
                {
                    'key': 'compute.googleapis.com/resource_id',
                    'valueType': 'STRING',
                    'description': 'Google Compute Instance ID'
                }
            ]
        }

        try:
            response = self.client.projects().metricDescriptors().create(
                name=self.projectId,
                body=metrics_descriptor
            ).execute(num_retries=6)
        except Exception as e:
            raise Exception('Failed to create custom metric: {}'.format(e))


        metric = self.get()
        while metric is None:
            sleep(1)
            metric = self.get()
        # Metric was created and exists with correct name
        return response if metric['name'] == response['name'] else None


    def get(self):
        try:
            request = self.client.projects().metricDescriptors().get(
                name='{}/metricDescriptors/{}/{}'.format(
                    self.projectId,
                    self.CUSTOM_METRIC_DOMAIN,
                    self.metricType)
            )
            response = request.execute(num_retries=3)
            metric = None
            if (response is not None and 'name' in response and
                response['name'] == '{}/{}'.format(self.CUSTOM_METRIC_DOMAIN, self.metricType)):
                metric = response
            else:
                #raise Exception('Failed to get custom metric {}: {}'.format(self.metricType, str(response)))
                return None

            if 'valueType' in metric:
                self.valueType = metric['valueType'].upper()
            if 'metricKind' in metric:
                self.metricKind = metric['metricKind'].upper()
            return metric
        except HttpError as ex:
            if ex.resp.status == 404:
                return None
            raise


    def has(self):
        return True if self.get() is not None else False


    def read(self, startTime=None, endTime=None, pageSize=10):
        try:
            if startTime is None:
                startTime = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
            elif not isinstance(startTime, datetime):
                raise Exception('Datetime object is required as startTime!')
            if endTime is None:
                endTime = datetime.datetime.utcnow()
            elif not isinstance(endTime, datetime):
                raise Exception('Datetime object is required as endTime!')
            request = self.client.projects().timeseries().list(
                name=self.projectId,
                filter='metric.type="{}"'.format(
                    self.CUSTOM_METRIC_DOMAIN,
                    self.metricType),
                pageSize=pageSize,
                interval_startTime=self._format_rfc3339(startTime),
                interval_endTime=self._format_rfc3339(endTime),
            )
            response = request.execute(num_retries=3)
            return response
        except:
            return []


    def _addPoint(self, value, startTime=None, endTime=None):
        if self.valueType is None:
            # Read and save valueType
            self.get()
        if self.valueType is None:
            raise Exception('Custom metric {}/{} not exists!'.format(
                self.CUSTOM_METRIC_DOMAIN, self.metricType))

        # Detect current point value type based on this metric value type
        if self.valueType == 'BOOL' and isinstance(value, bool):
            valueType = 'boolValue'
        elif self.valueType == 'INT64' and isinstance(value, int):
            valueType = 'int64Value'
        elif self.valueType == 'DOUBLE' and (isinstance(value, float) or isinstance(value, int)):
            valueType = 'doubleValue'
            value = float(value)
        elif self.valueType == 'STRING' and isinstance(value, str):
            valueType = 'stringValue'
        elif self.valueType == 'DISTRIBUTION' and 'count' in value:
            valueType = 'distributionValue'
        else:
            raise Exception('Invalid value type for this point: {}, but required {}'.format(
                type(value), self.valueType))

        if startTime is None:
            startTime = datetime.datetime.utcnow()
        elif not isinstance(startTime, datetime):
            raise Exception('Datetime object is required as startTime!')

        # Gauge requires same start and end time
        if self.metricKind == 'GAUGE':
            # Ignore endtime
            endTime = startTime
        else:
            if endTime is None:
                endTime = datetime.datetime.utcnow()
            elif not isinstance(startTime, datetime):
                raise Exception('Datetime object is required as endTime!')

        self.points.append({
            'interval': {
                'startTime': self._format_rfc3339(startTime),
                'endTime': self._format_rfc3339(endTime)
            },
            'value': {
                valueType: value
            }
        })


    def write(self, value, startTime=None, endTime=None, metricLabels=None):
        if metricLabels is None:
            metricLabels = {}
        else:
            metricLabels = metricLabels.copy()
        # if len(self.points) == 0:
        #     raise Exception('Missing at least one point of metric to write!')

        self.points = []
        self._addPoint(value, startTime, endTime)

        lastException = None
        for _repeat in range(6):
            try:
                metricLabels.update({
                    'compute.googleapis.com/resource_id': self.gce.instanceId()
                })
                resource = {}
                if self.gce.isInstance():
                    resource = {
                       'type': 'gce_instance',
                       'labels': {
                           'instance_id': self.gce.instanceId(),
                           'zone': self.gce.instanceZone(),
                       }
                    }
                timeseries_desc = {
                    'metric': {
                       'type': '{}/{}'.format(self.CUSTOM_METRIC_DOMAIN, self.metricType),
                       'labels': metricLabels
                    },
                    'resource': resource,
                    'points': self.points
                }
                # timeseries_data = {
                    # 'timeseriesDesc': timeseries_desc,
                    # 'point': self.points[0]
                # }

                request = self.client.projects().timeseries().create(
                    name=self.projectId,
                    body={"timeSeries": [timeseries_data, ]})
                request.execute(num_retries=3)
                self.points = []
                return True
            except IOError as e:
                sleep(_repeat * 2 + 1)
                if e.errno == errno.EPIPE:
                    self._reconnect()
                    credentials = GoogleCredentials.get_application_default()
                lastException = e
        if lastException is not None:
            raise lastException

