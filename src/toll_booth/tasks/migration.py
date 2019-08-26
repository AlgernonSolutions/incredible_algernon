import logging

from algernon import ajson
from aws_xray_sdk.core import xray_recorder
from toll_booth.obj.credible_fe import CredibleFrontEndDriver


@xray_recorder.capture()
def get_credible_object(object_type, id_value, extracted_data, driver):
    if object_type == 'Encounter':
        encounter = driver.retrieve_client_encounter(id_value)
        logging.info(f'completed a call to get a credible object: {object_type}.{id_value}, '
                     f'results: {ajson.dumps(encounter)}')
        return {
            'source': {
                'encounter_id': extracted_data['encounter_id'],
                'provider_id': extracted_data['provider_id'],
                'patient_id': extracted_data['patient_id'],
                'encounter_type': extracted_data['encounter_type'],
                'encounter_datetime_in': extracted_data['encounter_datetime_in'],
                'encounter_datetime_out': extracted_data['encounter_datetime_out'],
                'id_source': driver.id_source,
                'documentation': encounter
            },
            'patient_data': [{
                'last_name': extracted_data['patient_last_name'],
                'first_name': extracted_data['patient_first_name'],
                'dob': extracted_data['patient_dob']
            }]
        }
    raise RuntimeError(f'get_credible_object is not equipped to retrieve object_type: {object_type}')


@xray_recorder.capture()
def get_credible_objects(object_type, id_values, extracted_data, driver: CredibleFrontEndDriver):
    if object_type == 'Encounter':
        encounters = driver.retrieve_client_encounters(id_values)
        logging.debug(f'completed a call to get a credible object: {object_type}.{id_values}, '
                      f'results: {ajson.dumps(encounters)}')
        return {'extracted_data': extracted_data, 'id_values': id_values, 'documents': encounters}
    raise RuntimeError(f'get_credible_object is not equipped to retrieve object_type: {object_type}')


@xray_recorder.capture()
def get_credible_object_range(object_type, local_max, max_entries_pulled, driver: CredibleFrontEndDriver):
    id_source = driver.id_source
    if object_type == 'Encounter':
        search_data = {
            'clientvisit_id': 1,
            'service_type': 1,
            'consumer_name': 1,
            'staff_name': 1,
            'client_int_id': 1,
            'emp_int_id': 1,
            'non_billable1': 3,
            'visittype': 1,
            'timein': 1,
            'wh_fld1': 'cv.ClientVisit_id',
            'show_unappr': 1,
            'wh_cmp1': '>',
            'wh_val1': local_max,
            'wh_andor': 'AND',
            'wh_fld2': 'cv.ClientVisit_id',
            'wh_cmp2': '<=',
            'wh_val2': local_max + max_entries_pulled,
            'data_dict_ids': [3, 4, 6, 70, 74, 83, 86, 87, 218, 641]
        }
        results = driver.process_advanced_search('ClientVisit', search_data)
        return [
            {
                'result': {
                    'encounter_id': x['Service ID'],
                    'id_source': id_source,
                    'provider_id': x['Staff ID'],
                    'patient_id': x['Consumer ID'],
                    'encounter_type': x['Visit Type'],
                    'encounter_datetime_in': x['Time In'],
                    'encounter_datetime_out': x['Time Out'],
                    'patient_last_name': x['Last Name'],
                    'patient_first_name': x['First Name'],
                    'patient_dob': x['DOB']
                },
                'id_value': x['Service ID']
            } for x in results[:100]]
    raise RuntimeError(f'get_credible_object_range is not equipped to retrieve object_type: {object_type}')
