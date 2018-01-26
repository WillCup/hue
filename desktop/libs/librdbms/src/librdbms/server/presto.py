#!/usr/bin/env python
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import urllib2

try:
    import prestodb as Database
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured

    raise ImproperlyConfigured("Error loading Prestodb module: %s" % e)

# We want version (1, 2, 1, 'final', 2) or later. We can't just use
# lexicographic ordering in this check because then (1, 2, 1, 'gamma')
# inadvertently passes the version test.

from django.utils.translation import ugettext as _
from MySQLdb.converters import FIELD_TYPE

from librdbms.server.rdbms_base_lib import BaseRDBMSDataTable, BaseRDBMSResult, BaseRDMSClient

LOG = logging.getLogger(__name__)


class DataTable(BaseRDBMSDataTable): pass


class Result(BaseRDBMSResult): pass


def get_coordinator_host(app_name, rm):
    print 'http://{rm}/ws/v1/cluster/apps?states=RUNNING'.format(rm=rm)
    content = urllib2.urlopen('http://{rm}/ws/v1/cluster/apps?states=RUNNING'.format(rm=rm)).read()
    results = json.loads(content)
    for app in results['apps']['app']:
        if app['name'] == app_name:
            break

    print app['trackingUrl']
    result = urllib2.urlopen(app['trackingUrl']).read()
    for line in result.split('\n'):
        if 'COORDINATOR Host' in line:
            print line
            mid = line.strip().split(' ')[2]
            print mid
            return mid.split('/')[0].replace('[', '')


class PrestoClient(BaseRDMSClient):
    """Same API as Beeswax"""

    data_table_cls = DataTable
    result_cls = Result

    def __init__(self, *args, **kwargs):
        super(PrestoClient, self).__init__(*args, **kwargs)
        self.connection = Database.connect(**self._conn_params)

    @property
    def _conn_params(self):
        params = {
            'user': self.query_server['username'],
            'passwd': self.query_server['password'] or '',  # Presto can accept an empty password
            'host': get_coordinator_host(self.query_server['app_name'], self.query_server['resource_manager']),
            'port': self.query_server['server_port']
        }

        if self.query_server['options']:
            params.update(self.query_server['options'])

        if 'name' in self.query_server:
            params['db'] = self.query_server['name']

        return params

    def execute_statement(self, statement):
        cursor = self.connection.cursor()
        cursor.execute(statement)
        self.connection.commit()

        if cursor.description:
            columns = [column[0] for column in cursor.description]
        else:
            columns = []
        return self.data_table_cls(cursor, columns)

    def get_databases(self):
        cursor = self.connection.cursor()
        cursor.execute("SHOW SCHEMAS")
        self.connection.commit()
        databases = [row[0] for row in cursor.fetchall()]
        if 'db' in self._conn_params:
            if self._conn_params['db'] in databases:
                return [self._conn_params['db']]
            else:
                raise RuntimeError(
                    _("Cannot locate the %s database. Are you sure your configuration is correct?") % self._conn_params[
                        'db'])
        else:
            return databases

    def get_tables(self, database, table_names=[]):
        cursor = self.connection.cursor()
        query = 'SHOW TABLES'
        if table_names:
            clause = ' OR '.join(
                ["`Tables_in_%(database)s` LIKE '%%%(table)s%%'" % {'database': database, 'table': table} for table in
                 table_names])
            query += ' FROM `%(database)s` WHERE (%(clause)s)' % {'database': database, 'clause': clause}
        cursor.execute(query)
        self.connection.commit()
        return [row[0] for row in cursor.fetchall()]

    def get_columns(self, database, table, names_only=True):
        cursor = self.connection.cursor()
        cursor.execute("SHOW COLUMNS FROM %s.%s" % (database, table))
        self.connection.commit()
        if names_only:
            columns = [row[0] for row in cursor.fetchall()]
        else:
            columns = [dict(name=row[0], type=row[1], comment='') for row in cursor.fetchall()]
        return columns

    def get_sample_data(self, database, table, column=None, limit=100):
        column = '`%s`' % column if column else '*'
        statement = "SELECT %s FROM `%s`.`%s` LIMIT %d" % (column, database, table, limit)
        return self.execute_statement(statement)
