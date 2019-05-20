import os
from os import path

import pytest
import json
from unittest.mock import patch

import rapidjson


@pytest.fixture(autouse=True)
def test_environment(**kwargs):
    os.environ['STORAGE_BUCKET'] = kwargs.get('storage_bucket', 'algernonsolutions-gentlemen-dev')
    os.environ['LISTENER_ARN'] = kwargs.get('listener_arn', 'some_arn')


@pytest.fixture
def mock_bullhorn():
    boto_patch = patch('toll_booth.tasks.credible.Bullhorn')
    mock_boto = boto_patch.start()
    yield mock_boto
    boto_patch.stop()


@pytest.fixture
def dev_s3_resource():
    dynamo_patch = patch('toll_booth.tasks.credible.boto3.resource')
    mock_dynamo = dynamo_patch.start()
    import boto3
    mock_dynamo.return_value = boto3.Session(profile_name='dev').resource('s3')
    yield mock_dynamo
    mock_dynamo.stop()


@pytest.fixture
def mock_sqs_event():
    return _read_test_event('test_sqs_event')


@pytest.fixture(autouse=True)
def silence_x_ray():
    x_ray_patch_all = 'algernon.aws.lambda_logging.patch_all'
    patch(x_ray_patch_all).start()
    yield
    patch.stopall()


@pytest.fixture
def mock_context():
    from unittest.mock import MagicMock
    context = MagicMock(name='context')
    context.__reduce__ = cheap_mock
    context.function_name = 'test_function'
    context.invoked_function_arn = 'test_function_arn'
    context.aws_request_id = '12344_request_id'
    context.get_remaining_time_in_millis.side_effect = [1000001, 500001, 250000, 0]
    return context


@pytest.fixture(params=['get_client_ids', 'get_client_encounter_ids', 'get_encounter'])
def test_task_event(request):
    params = request.param
    return _generate_queued_sns_event(params)


def cheap_mock(*args):
    from unittest.mock import Mock
    return Mock, ()


def _read_test_event(event_name):
    with open(path.join('tests', 'test_events', f'{event_name}.json')) as json_file:
        event = json.load(json_file)
        return event


def _generate_queued_sns_event(event_name):
    event = _read_test_event(event_name)
    event_string = rapidjson.dumps(event)
    message_object = {'Message': event_string}
    body_object = {'body': rapidjson.dumps(message_object)}
    return {'Records': [body_object]}
