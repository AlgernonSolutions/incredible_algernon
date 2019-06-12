from datetime import datetime
from decimal import Decimal
from typing import Dict, Any

from algernon.aws import Bullhorn
from algernon import ajson

from toll_booth.obj.credible_fe import CredibleFrontEndDriver


def get_encounter(driver: CredibleFrontEndDriver,
                  encounter_id: str,
                  id_source: str,
                  patient_id: Decimal,
                  provider_id: Decimal,
                  encounter_datetime_in: datetime,
                  encounter_datetime_out: datetime,
                  encounter_type: str,
                  **kwargs) -> Dict[str, Any]:
    next_task_name = kwargs.get('next_task_name', 'leech')
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
        'task_name': next_task_name,
        'task_kwargs': {
            'object_type': 'Encounter',
            'extracted_data': encounter_data
        }
    }
    bullhorn = Bullhorn.retrieve()
    listener_arn = bullhorn.find_task_arn(next_task_name)
    strung_event = ajson.dumps(message)
    bullhorn.publish('new_event', listener_arn, strung_event)
    return encounter_data
