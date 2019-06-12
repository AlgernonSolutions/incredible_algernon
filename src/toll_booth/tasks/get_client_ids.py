from algernon.aws import Bullhorn
from algernon import ajson


def get_client_ids(**kwargs):
    next_task_name = kwargs.get('next_task_name', 'get_client_encounter_ids')
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
    listener_arn = bullhorn.find_task_arn(next_task_name)
    with bullhorn as batch_bullhorn:
        for entry in results:
            patient_id = entry[' Id']
            new_task = {
                'task_name': next_task_name,
                'task_kwargs': {
                    'patient_id': patient_id,
                    'patient_first_name': entry['First Name'],
                    'patient_last_name': entry['Last Name'],
                    'patient_dob': entry['DOB'],
                    'id_source': id_source
                }
            }
            strung_task = ajson.dumps(new_task)
            batch_bullhorn.publish('new_event', listener_arn, strung_task)
    return client_ids
