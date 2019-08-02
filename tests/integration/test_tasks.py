import csv

import pytest
import rapidjson
import requests
from algernon import rebuild_event

from toll_booth import handler, query_object_range_h, extract_credible_objects_h, parse_batch_encounters


@pytest.mark.tasks_integration
@pytest.mark.usefixtures('integration_env')
class TestTasks:
    def test_parse_batch(self, mock_context):
        event = {
            "_alg_class": "StoredData",
            "_alg_module": "algernon.aws.snakes",
            "value": {
                "pointer": "algernonsolutions-leech-dev#cache/f33c7af6-dc4d-4aa7-b5e5-bea98263656f!1563903684.207266.json"
            }
        }
        event = rebuild_event(event)
        results = parse_batch_encounters(event, mock_context)


    def test_get_objects(self, mock_context):
        event = {'payload': {
            'id_source': 'ICFS',
            'object_type': 'Encounter',
            'id_values': ['3144872', '3080626'],
            'extracted_data': {}
        }}
        results = extract_credible_objects_h(event, mock_context)
        assert results

    def test_get_object_range(self, mock_context):
        event = {'payload': {
            'id_source': 'PSI',
            'object_type': 'Encounter',
            'local_max': '100001',
            'remote_max': '100065'
        }}
        results = query_object_range_h(event, mock_context)
        assert results

    def test_task(self, mock_context):
        event = {
            "task_name": "get_encounters",
            "task_kwargs": {
                "id_source": "PSI"
            }
        }
        event_string = rapidjson.dumps(event)
        message_object = {'Message': event_string}
        body_object = {'body': rapidjson.dumps(message_object)}
        event = {'Records': [body_object]}
        results = handler(event, mock_context)
        assert results
