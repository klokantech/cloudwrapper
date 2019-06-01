"""
Influx DB direct use (as SQL database).

Copyright (C) 2016 Klokan Technologies GmbH (http://www.klokantech.com/)
Author: Martin Mikita <martin.mikita@klokantech.com>
"""

import json
import collections

try:
    from influxdb import InfluxDBClient
except ImportError:
    from warnings import warn
    install_modules = [
        'influxdb==3.0.0',
    ]
    warn('cloudwrapper.idb requires these packages:\n  - {}'.format('\n  - '.join(install_modules)))
    raise


# Python 3 compatible
try:
    UNICODE_EXISTS = bool(type(unicode))
except NameError:
    unicode = str


class IdbConnection(object):

    def __init__(self, user, pswd, host='localhost', port=8086, db='static'):
        self.host = host
        self.port = int(port)
        self.client = InfluxDBClient(self.host, self.port, user, pswd, db)
        self.client.create_database(db)
        self.client.switch_database(db)


    def table(self, name, tags=None):
        """
        Return Table object, tags is list of columns that should be indexed
        """
        return Table(self.client, name, tags)


    def drop(self, name, silent=True):
        self.table(name).drop(silent)
        return True



class Table(object):

    def __init__(self, client, name, tags=None):
        """
        Create Table object with name, using client connection.
        """
        self.name = name
        self.client = client
        self.tags = tags


    def insert(self, data):
        """
        Insert data into this table
        """
        tagsData = {}
        fieldsData = {}
        # Separate tags from other values - fields
        if not isinstance(data, dict):
            raise Exception('Invalid format of data, expected dict')
        for col in data:
            val = data[col]
            if not isinstance(val, (str, unicode)):
                val = json.dumps(val, separators=(',', ':'))
            if col in self.tags:
                tagsData[col] = val
            else:
                fieldsData[col] = val
        points = [{
            'measurement': self.name,
            'fields': fieldsData,
            'tags': tagsData,
        }]
        try:
            return self.client.write_points(points)
        except Exception as e:
            raise Exception('Unable to insert data into this table: '+str(e))


    def list(self, columns=None, where=None, sort=None):
        sql = 'SELECT '
        sqlCols = []

        if columns is None:
            sqlCols.append('*')
        else:
            for col in columns:
                sqlCols.append('"{}"'.format(col))

        sql += ','.join(sqlCols)

        sql += ' FROM "{}"'.format(self.name)

        if where is not None:
            sqlWhere = []
            if isinstance(where, dict):
                for col in where:
                    sqlWhere.append('"{}" = \'{}\''.format(col, where[col]))
            elif isinstance(where, (str, unicode)):
                sqlWhere.append(where)
            elif isinstance(where, collections.Iterable):
                sqlWhere.extend(where)
            else:
                raise Exception('Unable to parse where argument: type {}'.format(type(where)))
            sql += ' WHERE '
            sql += ' AND '.join(sqlWhere)

        if sort is not None:
            sqlSort = []
            if isinstance(sort, dict):
                for col in sort:
                    if sort[col].upper() in ['ASC', 'DESC']:
                        sqlSort.append('"{}" {}'.format(col, sort[col]))
            elif isinstance(sort, (str, unicode)):
                sqlSort.append(sort)
            else:
                raise Exception('Unable to parse sort argument: type {}'.format(type(sort)))
            sql += ' ORDER BY '
            sql += ' , '.join(sqlSort)
        # Add default sorting by time DESC
        else:
            sql += ' ORDER BY time DESC '

        rs = self.client.query(sql)
        if rs:
            for row in rs.get_points():
                myrow = {}
                for x in row:
                    if isinstance(row[x], (str, unicode)):
                        try:
                            myrow[x] = json.loads(row[x])
                        except ValueError:
                            myrow[x] = row[x]
                    else:
                        myrow[x] = row[x]
                yield myrow


    def get(self, columns=None, where=None, sort=None):
        result = None
        for row in self.list(columns, where, sort):
            result = row
            break
        return result


    def drop(self, silent=True):
        try:
            sql = 'DROP MEASUREMENT "{}"'.format(self.name)
            rs = self.client.query(sql)
        except:
            if not silent:
                raise
            pass
