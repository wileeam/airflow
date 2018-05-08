# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys

from airflow.contrib.hooks.gcp_cloudsql_hook import GoogleCloudSQLHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudSQLImportOperator(BaseOperator):
    """
    Imports a file to a Google Cloud SQL database.

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: string
    :param object: The name of the object to download in the Google cloud
        storage bucket.
    :type object: string
    :param filename: The file path on the local file system (where the
        operator is being executed) that the file should be downloaded to.
        If no filename passed, the downloaded data will not be stored on the local file
        system.
    :type filename: string
    :param store_to_xcom_key: If this param is set, the operator will push
        the contents of the downloaded file to XCom with the key set in this
        parameter. If not set, the downloaded data will not be pushed to XCom.
    :type store_to_xcom_key: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: string
    """
    template_fields = ('project', 'instance', 'bucket', 'object', 'database',
                       'table', 'columns', 'user')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 project,
                 instance,
                 bucket,
                 object,
                 database=None,
                 table=None,
                 columns=None,
                 user=None,
                 file_type='SQL',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleCloudSQLImportOperator, self).__init__(*args, **kwargs)
        self.project = project
        self.instance = instance
        self.bucket = bucket
        self.object = object
        self.database = database
        self.table = table
        self.columns = columns
        self.user = user
        self.file_type = file_type
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        # self.log.info('Executing download: %s, %s, %s', self.bucket,
        #               self.object, self.filename)
        hook = GoogleCloudSQLHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        hook.upload(
            project=self.project,
            instance=self.instance,
            bucket=self.bucket,
            object=self.object,
            database=self.database,
            table=self.table,
            columns=self.columns,
            user=self.user,
            file_type=self.file_type)


class GoogleCloudSQLExportOperator(BaseOperator):
    """
    Exports a Google Cloud SQL database to a file.

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: string
    :param object: The name of the object to download in the Google cloud
        storage bucket.
    :type object: string
    :param filename: The file path on the local file system (where the
        operator is being executed) that the file should be downloaded to.
        If no filename passed, the downloaded data will not be stored on the local file
        system.
    :type filename: string
    :param store_to_xcom_key: If this param is set, the operator will push
        the contents of the downloaded file to XCom with the key set in this
        parameter. If not set, the downloaded data will not be pushed to XCom.
    :type store_to_xcom_key: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: string
    """
    template_fields = ('project', 'instance', 'bucket', 'object', 'databases',
                       'tables', 'query')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 project,
                 instance,
                 bucket,
                 object,
                 databases=None,
                 tables=None,
                 only_schema=False,
                 query=None,
                 file_type='SQL',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleCloudSQLExportOperator, self).__init__(*args, **kwargs)
        self.project = project
        self.instance = instance
        self.bucket = bucket
        self.object = object
        self.databases = databases
        self.tables = tables
        self.only_schema = only_schema
        self.query = query
        self.file_type = file_type
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        # self.log.info('Executing download: %s, %s, %s', self.bucket,
        #               self.object, self.filename)
        hook = GoogleCloudSQLHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)

        hook.download(
            project=self.project,
            instance=self.instance,
            bucket=self.bucket,
            object=self.object,
            databases=self.databases,
            tables=self.tables,
            only_schema=self.only_schema,
            query=self.query,
            file_type=self.file_type)
