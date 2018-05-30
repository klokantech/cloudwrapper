"""
Google Custom Metric using API v3.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import errno
import datetime

from time import sleep

try:
    from google.cloud import monitoring
    from google.api.core.exceptions import GoogleAPIError
    from google.cloud.exceptions import NotFound
except ImportError:
    from warnings import warn
    install_modules = [
        'google-cloud-monitoring==0.27.0',
        'google-cloud-core==0.27.1',
        'google-auth==1.0.2',
        'oauth2client==2.0.2',
        'requests==2.18.4',
    ]
    warn('cloudwrapper.gcm3 requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise

from .gce import GoogleComputeEngine


class GcmConnection(object):

    def __init__(self):
        pass


    def metric(self, name, project_id=None):
        return Metric(name, project_id)


class Metric(object):

    # This is the namespace for all custom metrics
    CUSTOM_METRIC_DOMAIN = "custom.googleapis.com"

    def _format_rfc3339(self, dt):
        """Format a datetime per RFC 3339.

        :param dt: Datetime instanec to format, defaults to utcnow
        """
        return dt.isoformat("T") + "Z"


    def __init__(self, name, project_id):
        self.metricType = name
        self.gce = GoogleComputeEngine()
        if project_id is None:
            project_id = self.gce.projectId()
        elif 'projects/' in project_id:
            project_id = project_id.split('/')[-1]
        self.project_id = project_id
        self.points = []
        self.valueType = None
        self.metricKind = None
        self.client = monitoring.Client(project=self.project_id)


    def _reconnect(self):
        self.client = monitoring.Client(project=self.project_id)
        self.gce = GoogleComputeEngine()


    def name(self):
        return self.metricType


    def fullName(self):
        return '{}/{}'.format(self.CUSTOM_METRIC_DOMAIN, self.metricType)


    def create(self, metricKind, valueType='DOUBLE', description='', displayName=None, labels=()):
        if displayName is None:
            displayName = self.metricType.replace('/', ' ')
        for i, l in enumerate(labels):
            if not isinstance(l, monitoring.LabelDescriptor):
                labels[i] = monitoring.LabelDescriptor._from_dict(l)
            # Verify value type
            if labels[i].value_type not in ('STRING', 'INT64', 'BOOL'):
                raise Exception('Unsupported value type {} for label {}/{}.'.format(
                    labels[i].value_type, self.metricType, labels[i].key))
        if valueType not in ('BOOL', 'INT64', 'DOUBLE', 'STRING', 'DISTRIBUTION'):
            raise Exception('Unsupported value type {} of the metric {}.'.format(
                valueType, self.metricType))
        descriptor = self.client.metric_descriptor(
            self.fullName(),
            metric_kind=metricKind,
            value_type=valueType,
            description=description,
            display_name=displayName,
            unit='items',
            labels=labels
        )
        # 'labels': [
        #     {
        #         'key': 'gce_instance_id',
        #         'valueType': 'STRING',
        #         'description': 'Google Compute Instance ID'
        #     }
        # ]

        last_ex = ''
        for _repeat in range(6):
            try:
                descriptor.create()
                metric = self.get()
                if metric:
                    break
                raise Exception()
            except NotFound:
                sleep(_repeat * 2 + 1)
                continue
            except Exception as e:
                last_ex = e
                sleep(_repeat * 2 + 1)
                if hasattr(e, 'errno') and e.errno == errno.EPIPE:
                    self._reconnect()
        else:
            raise Exception('Failed to create custom metric: {}.'.format(last_ex))

        return metric


    def get(self):
        for _repeat in range(6):
            try:
                metric = self.client.fetch_metric_descriptor(self.fullName())
                if not metric:
                    raise Exception()
                self.valueType = metric.value_type.upper()
                self.metricKind = metric.metric_kind.upper()
                return metric
            except NotFound:
                return None
            except Exception as e:
                sleep(_repeat * 2 + 1)
                if hasattr(e, 'errno') and e.errno == errno.EPIPE:
                    self._reconnect()
        return None


    def has(self):
        return True if self.get() is not None else False


    def read(self, startTime=None, endTime=None, pageSize=10):
        if startTime is None:
            startTime = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
        elif not isinstance(startTime, datetime):
            raise Exception('Datetime object is required as startTime!')
        if endTime is None:
            endTime = datetime.datetime.utcnow()
        elif not isinstance(endTime, datetime):
            raise Exception('Datetime object is required as endTime!')
        try:
            query = self.client.query(
                metric_type=self.fullName()
            ).select_interval(
                end_time=endTime,
                start_time=startTime)
            for ts in query.iter(page_size=pageSize):
                for p in ts.points:
                    yield p
        except:
            pass


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

        if startTime is not None and not isinstance(startTime, datetime):
            raise Exception('Datetime object is required as startTime!')

        # Gauge requires same start and end time
        if self.metricKind == 'GAUGE':
            # Ignore endtime
            endTime = startTime
        else:
            if endTime is not None and not isinstance(startTime, datetime):
                raise Exception('Datetime object is required as endTime!')

        # valueType - not used variable
        self.points.append({
            'start_time': startTime,
            'end_time': endTime,
            'value': value,
        })


    def write(self, value, startTime=None, endTime=None, metricLabels={}):
        if len(metricLabels):
            metricLabels = metricLabels.copy()

        self.points = []
        self._addPoint(value, startTime, endTime)

        lastException = None
        for _repeat in range(6):
            try:
                resource = None
                if self.gce.isInstance():
                    resource = self.client.resource(
                        'gce_instance',
                        labels={
                            'instance_id': self.gce.instanceId(),
                            'zone': self.gce.instanceZone(),
                        }
                    )
                metric = self.client.metric(
                    type_=self.fullName(),
                    labels=metricLabels
                )
                # Point is dictionary which corresponds with write_points args
                for point in self.points:
                    self.client.write_point(metric, resource, **point)
                self.points = []
                return True
            except IOError as e:
                sleep(_repeat * 2 + 1)
                if e.errno == errno.EPIPE:
                    self._reconnect()
                lastException = e
        if lastException is not None:
            raise lastException
