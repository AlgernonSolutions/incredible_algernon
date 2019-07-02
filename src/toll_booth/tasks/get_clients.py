from aws_xray_sdk.core import xray_recorder


@xray_recorder.capture()
def get_clients(**kwargs):
    driver = kwargs['driver']
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
    results = driver.process_advanced_search('Clients', client_search_data)
    return {'patients': results, 'id_source': driver.id_source}
