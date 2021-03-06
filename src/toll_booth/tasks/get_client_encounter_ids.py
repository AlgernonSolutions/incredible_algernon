from aws_xray_sdk.core import xray_recorder


@xray_recorder.capture()
def get_client_encounter_ids(patient_id, id_source, driver, **kwargs):
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
    encounters = driver.process_advanced_search('ClientVisit', encounter_search_data)
    return encounters
