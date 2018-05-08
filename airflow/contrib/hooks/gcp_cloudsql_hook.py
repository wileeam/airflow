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
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowException

# class _GoogleCloudSQLJob(LoggingMixin):
#     # {u'status': u'PENDING', u'insertTime': u'2018-05-08T11:15:18.432Z', u'exportContext': {u'kind': u'sql#exportContext', u'sqlExportOptions': {u'schemaOnly': False}, u'uri': u'gs://models.bbr-recommendations.b17g.services/export/test', u'databases': [u'airflow']}, u'targetId': u'airflow-dev', u'targetLink': u'https://www.googleapis.com/sql/v1beta4/projects/bb-analytics-1/instances/airflow-dev', u'kind': u'sql#operation', u'name': u'0dc8210f-9738-4182-a2f4-5b548427a15c', u'targetProject': u'bb-analytics-1', u'operationType': u'EXPORT', u'selfLink': u'https://www.googleapis.com/sql/v1beta4/projects/bb-analytics-1/operations/0dc8210f-9738-4182-a2f4-5b548427a15c', u'user': u'guillermo.rodriguezcano@bonnierbroadcasting.com'}
#     def __init__(self, cloudsql, name, poll_sleep=10):
#         self._cloudsql = cloudsql
#         self._job_name = name
#         self._job_id = None
#         self._job = self._get_job()
#         self._poll_sleep = poll_sleep
# 
#     def _get_job_id_from_name(self):
#         jobs = self._cloudsql.operations().list(project=project, instance=instance)
#         jobs = self._dataflow.projects().locations().jobs().list(
#             projectId=self._project_number,
#             location=self._job_location
#         ).execute()
#         for job in jobs['jobs']:
#             if job['name'] == self._job_name:
#                 self._job_id = job['id']
#                 return job
#         return None
# 
#     def _get_job(self):
#         if self._job_name:
#             job = self._get_job_id_from_name()
#         else:
#             job = self._dataflow.projects().jobs().get(
#                 projectId=self._project_number,
#                 jobId=self._job_id
#             ).execute()
# 
#         if job and 'currentState' in job:
#             self.log.info(
#                 'Google Cloud DataFlow job %s is %s',
#                 job['name'], job['currentState']
#             )
#         elif job:
#             self.log.info(
#                 'Google Cloud DataFlow with job_id %s has name %s',
#                 self._job_id, job['name']
#             )
#         else:
#             self.log.info(
#                 'Google Cloud DataFlow job not available yet..'
#             )
# 
#         return job
# 
#     def wait_for_done(self):
#         while True:
#             if self._job and 'currentState' in self._job:
#                 if 'JOB_STATE_DONE' == self._job['currentState']:
#                     return True
#                 elif 'JOB_STATE_RUNNING' == self._job['currentState'] and \
#                      'JOB_TYPE_STREAMING' == self._job['type']:
#                     return True
#                 elif 'JOB_STATE_FAILED' == self._job['currentState']:
#                     raise Exception("Google Cloud Dataflow job {} has failed.".format(
#                         self._job['name']))
#                 elif 'JOB_STATE_CANCELLED' == self._job['currentState']:
#                     raise Exception("Google Cloud Dataflow job {} was cancelled.".format(
#                         self._job['name']))
#                 elif 'JOB_STATE_RUNNING' == self._job['currentState']:
#                     time.sleep(self._poll_sleep)
#                 elif 'JOB_STATE_PENDING' == self._job['currentState']:
#                     time.sleep(15)
#                 else:
#                     self.log.debug(str(self._job))
#                     raise Exception(
#                         "Google Cloud Dataflow job {} was unknown state: {}".format(
#                             self._job['name'], self._job['currentState']))
#             else:
#                 time.sleep(15)
# 
#             self._job = self._get_job()
# 
#     def get(self):
#         return self._job
# 
# 
# class _Dataflow(LoggingMixin):
#     def __init__(self, cmd):
#         self.log.info("Running command: %s", ' '.join(cmd))
#         self._proc = subprocess.Popen(
#             cmd,
#             shell=False,
#             stdout=subprocess.PIPE,
#             stderr=subprocess.PIPE,
#             close_fds=True)
# 
#     def _line(self, fd):
#         if fd == self._proc.stderr.fileno():
#             lines = self._proc.stderr.readlines()
#             for line in lines:
#                 self.log.warning(line[:-1])
#             if lines:
#                 return lines[-1]
#         if fd == self._proc.stdout.fileno():
#             line = self._proc.stdout.readline()
#             return line
# 
#     @staticmethod
#     def _extract_job(line):
#         if line is not None:
#             if line.startswith("Submitted job: "):
#                 return line[15:-1]
# 
#     def wait_for_done(self):
#         reads = [self._proc.stderr.fileno(), self._proc.stdout.fileno()]
#         self.log.info("Start waiting for DataFlow process to complete.")
#         while self._proc.poll() is None:
#             ret = select.select(reads, [], [], 5)
#             if ret is not None:
#                 for fd in ret[0]:
#                     line = self._line(fd)
#                     if line:
#                         self.log.debug(line[:-1])
#             else:
#                 self.log.info("Waiting for DataFlow process to complete.")
#         if self._proc.returncode is not 0:
#             raise Exception("DataFlow failed with return code {}".format(
#                 self._proc.returncode))


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
