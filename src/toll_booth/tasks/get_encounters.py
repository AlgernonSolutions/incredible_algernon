from aws_xray_sdk.core import xray_recorder


@xray_recorder.capture()
def get_encounters(**kwargs):
    driver = kwargs['driver']
    encounter_search_data = {
        'clientvisit_id': 1,
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
    results = driver.process_advanced_search('ClientVisit', encounter_search_data)
    return {'encounters': results, 'id_source': driver.id_source}
