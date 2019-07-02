import logging

from algernon import ajson
from aws_xray_sdk.core import xray_recorder


@xray_recorder.capture()
def get_credible_object(id_source, object_type, id_value, driver):
    if object_type == 'Encounter':
        encounter = driver.retrieve_client_encounter(id_value)
        logging.info(f'completed a call to get a credible object: {object_type}.{id_value}, '
                     f'results: {ajson.dumps(encounter)}')
        return {
            'credible_object': encounter,
            'id_source': id_source,
            'id_value': id_value,
            'object_type': object_type
        }
    raise RuntimeError(f'get_credible_object is not equipped to retrieve object_type: {object_type}')
