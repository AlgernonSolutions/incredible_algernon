import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict

import boto3
from algernon import ajson
from algernon.aws import Bullhorn
from botocore.exceptions import ClientError

from toll_booth.obj.credible_fe import CredibleFrontEndDriver


class CredibleTasks:
    @staticmethod
    def get_credentials(**kwargs):
        id_source = kwargs['id_source']
        try:
            credentials = _download_object(os.environ['STORAGE_BUCKET'], id_source, 'credentials')
        except ClientError:
            return None
        return credentials

    @staticmethod
    def push_credentials(**kwargs):
        id_source = kwargs['id_source']
        _upload_object(os.environ['STORAGE_BUCKET'], id_source, 'credentials', kwargs['credentials'])

    @staticmethod
    def _get_encounter(driver: CredibleFrontEndDriver,
                       encounter_id: str,
                       id_source: str,
                       patient_id: Decimal,
                       provider_id: Decimal,
                       encounter_datetime_in: datetime,
                       encounter_datetime_out: datetime,
                       encounter_type: str,
                       **kwargs) -> Dict[str, Any]:
        encounter_text = driver.retrieve_client_encounter(encounter_id)
        encounter_data = {
            'source': {
                'patient_id': patient_id,
                'provider_id': provider_id,
                'id_source': id_source,
                'encounter_id': encounter_id,
                'documentation': encounter_text,
                'encounter_datetime_in': encounter_datetime_in,
                'encounter_datetime_out': encounter_datetime_out,
                'encounter_type': encounter_type
            },
            'patient_data': [{
                'last_name': kwargs['patient_last_name'],
                'first_name': kwargs['patient_first_name'],
                'dob': kwargs['patient_dob']
            }]
        }
        message = {
            'task_name': 'leech',
            'task_kwargs': {
                'object_type': 'Encounter',
                'extracted_data': encounter_data
            },
            'flow_id': f'{kwargs["flow_id"]}#leech'
        }
        bullhorn = Bullhorn.retrieve()
        listener_arn = bullhorn.find_task_arn('leech')
        strung_event = ajson.dumps(message)
        bullhorn.publish('new_event', listener_arn, strung_event)
        return encounter_data

    @staticmethod
    def _get_client_encounter_ids(**kwargs):
        patient_id = kwargs['patient_id']
        encounter_search_data = {
            'clientvisit_id': 1,
            'client_id': patient_id,
            'service_type': 1,
            'non_billable': 1,
            'client_int_id': 1,
            'emp_int_id': 1,
            'non_billable1': 3,
            'visittype': 1,
            'timein': 1,
            'timeout': 1,
            'data_dict_ids': [3, 4, 6, 70, 74, 83, 86, 87]
        }
        driver = kwargs['driver']
        results = driver.process_advanced_search('ClientVisit', encounter_search_data)
        encounters = []
        bullhorn = Bullhorn.retrieve()
        next_task_name = 'get_encounter'
        for encounter in results:
            encounter_id = encounter['Service ID']
            encounter_data = {
                'encounter_id': encounter_id,
                'encounter_datetime_in': encounter['Time In'],
                'encounter_datetime_out': encounter['Time Out'],
                'patient_id': patient_id,
                'provider_id': encounter['Staff ID'],
                'encounter_type': encounter['Visit Type'],
                'id_source': kwargs['id_source'],
                'patient_last_name': kwargs['patient_last_name'],
                'patient_first_name': kwargs['patient_first_name'],
                'patient_dob': kwargs['patient_dob']
            }
            encounters.append(encounter_data)
            new_task = {
                'task_name': next_task_name,
                'flow_id': f'{kwargs["flow_id"]}#{next_task_name}-{encounter_id}',
                'task_kwargs': encounter_data
            }
            strung_event = ajson.dumps(new_task)
            listener_arn = bullhorn.find_task_arn('credible_tasks')
            bullhorn.publish('new_event', listener_arn, strung_event)
        return encounters

    @staticmethod
    def _get_client_ids(**kwargs):
        client_search_data = {
            'teams': 1,
            'client_id': 1,
            'last_name': 1,
            'first_name': 1,
            'text28': 1,
            'dob': 1,
            'ssn': 1,
            'primary_assigned': 1,
            'client_status_f': 'ALL ACTIVE'
        }
        id_source = kwargs['id_source']
        driver = kwargs['driver']
        results = driver.process_advanced_search('Clients', client_search_data)
        client_ids = [x[' Id'] for x in results]
        bullhorn = Bullhorn.retrieve()
        next_task_name = 'get_client_encounter_ids'
        for entry in results:
            patient_id = entry[' Id']
            new_task = {
                'task_name': next_task_name,
                'flow_id': f'{kwargs["flow_id"]}#{next_task_name}-{patient_id}',
                'task_kwargs': {
                    'patient_id': patient_id,
                    'patient_first_name': entry['First Name'],
                    'patient_last_name': entry['Last Name'],
                    'patient_dob': entry['DOB'],
                    'id_source': id_source
                }
            }
            strung_task = ajson.dumps(new_task)
            listener_arn = bullhorn.find_task_arn('credible_tasks')
            bullhorn.publish('new_event', listener_arn, strung_task)
        return client_ids


def _upload_object(bucket_name: str, folder_name: str, object_name: str, obj: Any):
    resource = boto3.resource('s3')
    object_key = f'{folder_name}/{object_name}'
    resource.Object(bucket_name, object_key).put(Body=ajson.dumps(obj))


def _download_object(bucket_name: str, folder_name: str, object_name: str) -> Any:
    resource = boto3.resource('s3')
    object_key = f'{folder_name}/{object_name}'
    stored_object = resource.Object(bucket_name, object_key).get()
    string_body = stored_object['Body'].read()
    return ajson.loads(string_body)
