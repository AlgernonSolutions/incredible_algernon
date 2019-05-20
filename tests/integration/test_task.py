import os

import pytest

from toll_booth.tasks import handler


@pytest.mark.credible_tasks_integration
@pytest.mark.usefixtures('dev_s3_resource', 'dev_sns_resource')
class TestTask:
    def test_task(self, test_task_event, mock_context):
        os.environ['LEECH_LISTENER_ARN'] = 'arn:aws:sns:us-east-1:726075243133:leech-dev-Listener-1EV4D8VOW7L37'
        os.environ['LISTENER_ARN'] = 'arn:aws:sns:us-east-1:726075243133:credible-dev-Listener-EI7SS094P7RY'
        results = handler(test_task_event, mock_context)
        assert results

