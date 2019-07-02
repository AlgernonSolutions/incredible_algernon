import csv

import pytest
import rapidjson
import requests

from toll_booth import handler


@pytest.mark.tasks_integration
@pytest.mark.usefixtures('integration_env')
class TestTasks:
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
