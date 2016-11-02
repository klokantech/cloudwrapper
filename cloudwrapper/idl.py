"""
Influx DB Logging.

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import logging
import json
import errno
import datetime
import socket

from collections import OrderedDict
from time import sleep

try:
    from influxdb import InfluxDBClient
except ImportError:
    from warnings import warn
    install_modules = [
        'influxdb==3.0.0',
    ]
    warn('cloudwrapper.idl requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise


class IdlConnection(object):

    def __init__(self, user, pswd, host='localhost', port=8086, db='logs'):
        self.host = host
        self.port = int(port)
        self.client = InfluxDBClient(self.host, self.port, user, pswd, db)
        self.client.create_database(db)
        self.client.switch_database(db)
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


    def handler(self, logId):
        return Handler(self.client, logId, self.globalLabels.copy())



class Handler(logging.Handler):

    def _format_rfc3339(self, dt):
        """
        Formats a datetime per RFC 3339.
        :param dt: Datetime instance to format, defaults to utcnow
        """
        return dt.isoformat("T") + "Z"


    def _format_json(self, record):
        data = OrderedDict()
        defaultFormatter = logging.Formatter()
        data['moment'] = defaultFormatter.formatTime(record)
        data['severity'] = record.levelname
        if isinstance(record.msg, dict):
            for key in sorted(record.msg.keys()):
                data[key] = record.msg[key]
        else:
            try:
                data['message'] = record.msg % record.args
            except Exception:
                pass
        if record.exc_info is not None:
            data.setdefault('message', str(record.exc_info[1]))
            data['traceback'] = defaultFormatter.formatException(record.exc_info)
        return json.dumps(data)


    def __init__(self, client, logId, globalLabels=None, *args, **kwargs):
        super(Handler, self).__init__(*args, **kwargs)
        """
        Create Handler object with logId, using client connection.

        Setup my_hostname used for each log value in write() method
        """
        self.logId = logId
        self.client = client
        self.points = []
        self.my_hostname = socket.gethostname()
        if globalLabels is None:
            globalLabels = {}
        self.globalLabels = globalLabels
        self.entries = []


    def logId(self):
        return self.logId


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


    def emit(self, record):
        d = datetime.datetime.utcnow() # <-- get time in UTC
        tags = self.globalLabels.copy()
        tags.update({
            'severity': record.levelname,
        })
        fields = self.format(record)
        if not isinstance(fields, dict):
            fields = json.loads(self._format_json(record))
        fields.update({
            'timestamp': self._format_rfc3339(d),
        })
        self.entries.append({
            'measurement': self.logId,
            'fields': fields,
            'tags': tags,
        })


    def flush(self):
        if not self.entries:
            return
        lastException = None
        for _repeat in range(6):
            try:
                self.client.write_points( self.entries )
                self.entries = []
                lastException = None
                break
            except IOError as e:
                sleep(_repeat * 2 + 1)
                lastException = e
            except Exception:
                sleep(_repeat * 2 + 5)
        if lastException is not None:
            raise lastException


    def list(self, columns=None, filter=None, orderAsc=True):

        sql = 'SELECT '
        sqlCols = []
        for col in columns:
            sqlcols.append('"{}"'.format(col))
        if not sqlCols:
            sqlCols.append('*')
        sql += ','.join(sqlCols)

        sql += 'FROM "{}"'.format(self.logId)

        # filter and orderAsc is not implemented yet
        rs = client.query(sql)
        if rs:
            for row in rs:
                yield row
