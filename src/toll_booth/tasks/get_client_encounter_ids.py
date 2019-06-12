from algernon.aws import Bullhorn
from algernon import ajson


def get_client_encounter_ids(**kwargs):
    next_task_name = kwargs.get('next_task_name', 'get_encounter')
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
    listener_arn = bullhorn.find_task_arn(next_task_name)
    with bullhorn as batch_bullhorn:
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
                'task_kwargs': encounter_data
            }
            strung_event = ajson.dumps(new_task)
            batch_bullhorn.publish('new_event', listener_arn, strung_event)
    return encounters
