import os

import pytest

from toll_booth.tasks import handler


@pytest.mark.credible_tasks
@pytest.mark.usefixtures('dev_s3_resource')
class TestTask:
    def test_task(self, test_task_event, mock_context, mock_bullhorn):
        os.environ['LEECH_LISTENER_ARN'] = 'mock_arn'
        results = handler(test_task_event, mock_context)
        assert results
        assert mock_bullhorn.called
        assert mock_bullhorn.call_count == 1
