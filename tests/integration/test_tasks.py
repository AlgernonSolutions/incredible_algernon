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

    @pytest.mark.get_objects_i
    def test_get_objects(self, mock_context):
        event = {
            "payload": {
                "extracted_data": {
                    "_alg_class": "StoredData",
                    "_alg_module": "algernon.aws.snakes",
                    "value": {
                        "pointer": "algernonsolutions-leech-dev#cache/902773b7-67c8-4645-a7fd-0774bcea3676!1565012856.447914.json"
                    }
                },
                "object_type": "Encounter",
                "id_source": "ICFS",
                "id_values": [
                    "3228525",
                    "3228735",
                    "3228674",
                    "3228473",
                    "3228345",
                    "3228393",
                    "3228406",
                    "3228445",
                    "3228457",
                    "3228723",
                    "3228741",
                    "3228754",
                    "3228768",
                    "3228503",
                    "3228524",
                    "3228627",
                    "3228456",
                    "3228515",
                    "3228747",
                    "3228460",
                    "3228492"
                ]
            }
        }
        results = extract_credible_objects_h(event, mock_context)
        assert results

    @pytest.mark.query_range_i
    def test_get_object_range(self, mock_context):
        event = {
            "identifier": "#ICFS#Encounter#",
            "object_type": "Encounter",
            "id_source": "ICFS",
            "counter": {
                "count": 100,
                "index": 1,
                "more": True
            },
            "local_max": 3228275
        }
        event = {'payload': event}
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
