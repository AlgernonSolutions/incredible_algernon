CREDIBLE_DATE_FORMAT = '%m/%d/%Y'


def get_days_encounters(driver, transfer_date):
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
        'data_dict_ids': [3, 4, 6, 70, 74, 83, 86, 87],
        'wh_fld1': 'cv.transfer_date',
        'wh_cmp1': '=',
        'wh_val1': transfer_date.strftime(CREDIBLE_DATE_FORMAT),
    }
    encounters = driver.process_advanced_search('ClientVisit', encounter_search_data)
    return encounters
