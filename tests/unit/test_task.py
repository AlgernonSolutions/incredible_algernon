import pytest

from toll_booth import handler


@pytest.mark.tasks_unit
@pytest.mark.usefixtures('dev_s3_resource')
class TestTask:
    def test_task(self, test_task_event, mock_context, mock_bullhorn):
        results = handler(test_task_event, mock_context)
        assert results
        assert mock_bullhorn.called
        assert mock_bullhorn.call_count == 1
