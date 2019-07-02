from aws_xray_sdk.core import xray_recorder


@xray_recorder.capture()
def get_providers(**kwargs):
    driver = kwargs['driver']
    staff_search_data = {
        'emp_status_f': 'ACTIVE',
        'first_name': 1,
        'last_name': 1,
        'emp_id': 1,
        'profile_code': 1,
        'asgn_supervisors': 1,
        'asgn_supervisees': 1
    }
    results = driver.process_advanced_search('Employees', staff_search_data)
    return {'providers': results, 'id_source': driver.id_source}
