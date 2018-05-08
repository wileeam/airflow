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
#
from apiclient.discovery import build
from googleapiclient import errors

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.exceptions import AirflowException


class GoogleCloudSQLHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud SQL. This hook uses the Google Cloud Platform
    connection.
    """

    def __init__(self,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GoogleCloudSQLHook, self).__init__(google_cloud_storage_conn_id,
                                                 delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud SQL service object.
        """
        credentials = self._get_credentials()
        return build('sqladmin', 'v1beta4', credentials=credentials)

    def _get_instance_sa(self, project, instance):
        service = self.get_conn()
        try:
            response = service \
                       .instances() \
                       .get(project=project, instance=instance) \
                       .execute()
            return response['serviceAccountEmailAddress']
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return ''
            raise

    def upload(self,
               project,
               instance,
               bucket,
               object,
               database=None,
               table=None,
               columns=None,
               user=None,
               file_type='SQL'):
        """
        Imports a csv or sql file into a database of a Google Cloud SQL instance
        """

        if object_type.endswith('.gz') and fyle_type != 'SQL':
            raise AirflowException(
                "Compressed files files are only supported with SQL statements"
            )

        if file_type == 'SQL' and database is None:
            self.log.warn(
                'Database parameter not especified. Input file is expected to do so.'
            )

        if file_type == 'CSV':
            if database is None:
                raise AirflowException(
                    'Database parameter expected for CSV input files.')
            if table is None:
                raise AirflowException(
                    'Table parameter expected for CSV input files.')

        gcs_uri = 'gs://{0}/{1}'.format(bucket, object)

        import_request = {
            "importContext": {
                "kind": "sql#importContext",
                "fileType": file_type,
                "uri": gcs_uri
            }
        }

        if database:
            import_request['importContext']['database'] = database
        if user:
            import_request['importContext']['importUser'] = user

        if file_type == 'CSV':
            import_request['importContext']['csvImportOptions'] = {
                'table': table
            }
            if columns and len(columns) > 0:
                import_request['importContext']['csvImportOptions'][
                    'columns'] = columns

        service = self.get_conn()
        try:
            service \
                .instances() \
                .import_(project=project, instance=instance,
                         body=import_request) \
                .execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

    def download(self,
                 project,
                 instance,
                 bucket,
                 object,
                 databases=None,
                 tables=None,
                 only_schema=False,
                 query=None,
                 file_type='SQL'):
        """
        Exports one or more databases of a Google Cloud SQL instance to csv or sql files
        """

        if file_type == 'CSV' and len(databases) > 1:
            raise AirflowException(
                'Cannot export to more than one file when file type is csv.')

        if tables and len(tables) > 1:
            raise AirflowException(
                'Cannot specify more than one table to export.')

        gcs_uri = 'gs://{0}/{1}'.format(bucket, object)

        export_request = {
            "exportContext": {
                "kind": "sql#exportContext",
                "fileType": file_type,
                "uri": gcs_uri,
                "sqlExportOptions": {
                    "schemaOnly": only_schema
                }
            }
        }

        if databases and len(databases) > 0:
            export_request['exportContext']['databases'] = databases

        if tables and len(tables) > 0:
            export_request['exportContext']['sqlExportOptions'][
                'tables'] = tables

        if query:
            export_request['exportContext']['csvExportOptions'] = {
                'selectQuery': query
            }

        self.log.info('Request: {}'.format(export_request))

        service = self.get_conn()
        try:
            response = service \
                .instances() \
                .export(project=project, instance=instance,
                        body=export_request) \
                .execute()
            self.log.info(response)
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise
