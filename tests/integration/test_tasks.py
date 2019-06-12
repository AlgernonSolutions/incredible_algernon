from datetime import datetime
from decimal import Decimal

import pytest

from toll_booth import handler
from algernon import ajson


@pytest.mark.tasks_integration
@pytest.mark.usefixtures('integration_env', 'dev_s3_resource')
class TestTasks:
    def test_task(self, mock_context):
        event = {'task_name': 'get_client_encounter_ids', 'flow_id': 'get_client_encounter_ids-1457199-test-credible#1002', 'task_kwargs': {'patient_id': {'_alg_class': 'decimal', 'value': '1457199'}, 'patient_first_name': 'QUANNEKA', 'patient_last_name': 'BOYD', 'patient_dob': {'_alg_class': 'datetime', 'value': {'tz': None, 'timestamp': 558316800.0}}, 'id_source': 'PSI'}}
        results = handler(event, mock_context)
        assert results
