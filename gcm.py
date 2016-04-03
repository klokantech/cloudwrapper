"""Google Custom Metric."""

import json
import errno
import datetime

from time import sleep
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials
from googleapiclient.errors import HttpError

from .gce import GoogleComputeEngine

class GoogleCustomMetric(object):

    # This is the namespace for all custom metrics
    CUSTOM_METRIC_DOMAIN = "custom.googleapis.com"

    def _format_rfc3339(self, dt):
        """Formats a datetime per RFC 3339.
        :param dt: Datetime instanec to format, defaults to utcnow
        """
        return dt.isoformat("T") + "Z"


    def __init__(self, metricType, projectId=None):
        self.metricType = metricType
        credentials = GoogleCredentials.get_application_default()
        self.client = build('monitoring', 'v3', credentials=credentials)
        self.gce = GoogleComputeEngine()
        if projectId is None:
            projectId = 'projects/' + self.gce.projectId()
        elif 'projects/' not in projectId:
            projectId = 'projects/' + projectId
        self.projectId = projectId
        self.points = []
        self.valueType = None
        self.metricKind = None


    def create(self, metricKind, valueType='DOUBLE', description='', displayName=None):
        if displayName is None:
            displayName = self.metricType.replace('/', ' ')
        metrics_descriptor = {
            'name': 'metricDescriptors/{}/{}'.format(self.CUSTOM_METRIC_DOMAIN, self.metricType),
            'type': '{}/{}'.format(self.CUSTOM_METRIC_DOMAIN, self.metricType),
            'metricKind': metricKind,
            'valueType': valueType,
            'unit': 'items',
            'description': description,
            'displayName': displayName
        }

        response = self.client.projects().metricDescriptors().create(
            name=self.projectId, body=metrics_descriptor).execute()

        metric = self.get()
        while metric is None:
            sleep(1)
            metric = self.get()
        # Metric was created and exists with correct name
        return response if metric['name'] == response['name'] else None


    def get(self):
        try:
            request = self.client.projects().metricDescriptors().get(
                name='{}/metricDescriptors/{}/{}'.format(self.projectId,
                    self.CUSTOM_METRIC_DOMAIN,
                    self.metricType))
            response = request.execute()
            if 'valueType' in response:
                self.valueType = response['valueType']
            if 'metricKind' in response:
                self.metricKind = response['metricKind']
            return response
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
            request = self.client.projects().timeSeries().list(
                name=self.projectId,
                filter='metric.type="{}/{}"'.format(
                    self.CUSTOM_METRIC_DOMAIN,
                    self.metricType),
                pageSize=pageSize,
                interval_startTime=self._format_rfc3339(startTime),
                interval_endTime=self._format_rfc3339(endTime)
            )
            response = request.execute()
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
        # if len(self.points) == 0:
        #     raise Exception('Missing at least one point of metric to write!')
        self.points = []
        self._addPoint(value, startTime, endTime)
        try:
            timeseries_data = {
                'metric': {
                    'type': '{}/{}'.format(self.CUSTOM_METRIC_DOMAIN, self.metricType),
                    'labels': metricLabels
                },
                'resource': {
                    'type': 'gce_instance' if self.gce.isInstance() else 'none',
                    'labels': {
                        'instance_id': self.gce.instanceId(),
                        'zone': self.gce.instanceZone(),
                    }
                },
                'points': self.points
            }

            request = self.client.projects().timeSeries().create(
                name=self.projectId, body={"timeSeries": [timeseries_data]})
            request.execute()
            self.points = []
            return True
        except:
            # raise
            return False

