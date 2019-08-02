import json
import logging
import os
import re
import uuid
from datetime import datetime
from decimal import Decimal
from multiprocessing.pool import ThreadPool

import boto3
import rapidjson
from algernon import queued, rebuild_event, ajson
from algernon.aws import lambda_logged, StoredData, Bullhorn

from toll_booth import tasks


class FireHoseEncoder(json.JSONEncoder):
    @classmethod
    def default(cls, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super(FireHoseEncoder, cls()).default(obj)


def _generate_stored_results(results):
    stored_results = rapidjson.loads(ajson.dumps(StoredData.from_object(uuid.uuid4(), results, True)))
    return stored_results


def _lookup_resource(resource_name):
    client = boto3.client('ssm')
    response = client.get_parameter(
        Name=resource_name
    )
    parameter = response['Parameter']
    return parameter['Value']


@lambda_logged
@queued(preserve=False)
# @xray_recorder.capture('incredible_algernon')
def handler(event, context):
    logging.info(f'received a call for a credible task: {event}/{context}')
    event = rebuild_event(event)
    logging.info(f'started a call for a credible task, event/context: {event}/{context}')
    task_name = event['task_name']
    task_kwargs = event.get('task_kwargs', {})
    id_source = task_kwargs['id_source']
    driver = context.get('driver')
    if driver is None:
        driver = tasks.build_driver(id_source)
    if driver.id_source != id_source:
        driver = tasks.build_driver(id_source)
    task_function = getattr(tasks, task_name)
    results = task_function(**task_kwargs, driver=driver)
    context['driver'] = driver
    logging.info(f' completed a call for a credible task, event: {event}, results: {results}')
    return results


@lambda_logged
def query_object_range_h(event, context):
    logging.info(f'started a call for a query_object_range task, event: {event}, context: {context}')
    event = rebuild_event(event['payload'])
    id_source = event['id_source']
    driver = tasks.build_driver(id_source)
    max_entries = event.get('max_entries', 1000)
    results = tasks.get_credible_object_range(event['object_type'], event['local_max'], max_entries, driver)
    logging.info(f'completed a call for generate_source_vertex task, event: {event}, results: {results}')
    id_values = [str(x['id_value']) for x in results]
    return {
        'results': _generate_stored_results(results),
        'iterator': {'count': len(results)},
        'id_values': id_values
    }


@lambda_logged
def extract_credible_object_h(event, context):
    logging.info(f'started a call for a extract_credible_object task, event: {event}, context: {context}')
    event = rebuild_event(event['payload'])
    extracted_data = event['extracted_data']
    id_source = event['id_source']
    driver = tasks.build_driver(id_source)
    results = tasks.get_credible_object(event['object_type'], event['id_value'], extracted_data, driver)
    logging.info(f'completed a call for extract_credible_object task, event: {event}, results: {results}')
    documentation = {}
    return _generate_stored_results(results)


@lambda_logged
def extract_credible_objects_h(event, context):
    logging.info(f'started a call for a extract_credible_objects task, event: {event}, context: {context}')
    event = rebuild_event(event['payload'])
    extracted_data = event['extracted_data']
    id_source = event['id_source']
    driver = tasks.build_driver(id_source)
    results = tasks.get_credible_objects(event['object_type'], event['id_values'], extracted_data, driver)
    logging.info(f'completed a call for extract_credible_objects task, event: {event}, results: {results}')
    stored_results = _generate_stored_results(results)
    return stored_results


@lambda_logged
def parse_batch_encounters(event, context):
    migration_table_name = os.environ['MIGRATION_TABLE_NAME']
    if 'Records' in event:
        return _process_queued_parse_task(event, context)
    event = rebuild_event(event)
    parsed_events = _parse_batch_event(event)
    _fire_hose_parsed_events(parsed_events)
    for parsed_event in parsed_events:
        _mark_parsing_complete(parsed_event, migration_table_name)
    return ajson.dumps(StoredData.from_object(uuid.uuid4(), parsed_events, full_unpack=True))


def _process_queued_parse_task(event, context):
    bullhorn = Bullhorn.retrieve()
    topic_arn = bullhorn.find_task_arn('aio_leech')
    batch = []
    for entry in event['Records']:
        entry_body = rapidjson.loads(entry['body'])
        original_payload = rapidjson.loads(entry_body['Message'])
        original_payload['topic_arn'] = topic_arn
        batch.append((original_payload, context))
    pool = ThreadPool(len(batch))
    pool.starmap(parse_batch_encounters, batch)
    pool.close()
    pool.join()


def _mark_parsing_complete(parsed_event, table_name):
    resource = boto3.resource('dynamodb')
    table = resource.Table(table_name)
    table.update_item(
        Key={
            'identifier': parsed_event['identifier'],
            'id_value': int(parsed_event['id_value'])
        },
        UpdateExpression='SET #x=:x',
        ExpressionAttributeNames={'#x': 'batch_parsing'},
        ExpressionAttributeValues={':x': {
            'completed_at': datetime.now().isoformat(),
            'stage_results': ajson.dumps(parsed_event)
        }}
    )


def _fire_hose_parsed_events(parsed_events):
    batch = [{'Data': rapidjson.dumps(x, default=FireHoseEncoder.default).encode()} for x in parsed_events]
    stream_name = _lookup_resource('extraction_fire_hose')
    client = boto3.client('firehose')
    response = client.put_record_batch(
        DeliveryStreamName=stream_name,
        Records=batch
    )
    if response['FailedPutCount']:
        failed_records = [x for x in response['RequestResponses'] if 'ErrorCode' in x]
        raise RuntimeError(f'could not fire_hose these records: {failed_records}')


def _parse_batch_event(event):
    completed_batch = []
    identifier = event['identifier']
    id_source = event['id_source']
    object_type = event['object_type']
    extraction = event['extraction']
    extracted_data = {str(x['id_value']): x['result'] for x in extraction['extracted_data']}
    id_values = extraction['id_values']
    documents = extraction['documents']
    parsed_documents = _parse_documents(documents)
    for id_value in id_values:
        id_value = str(id_value)
        id_extracted_data = extracted_data[id_value]
        id_documentation = parsed_documents[id_value]
        collected_extracted_data = {
            'source': {
                'id_source': id_source,
                'encounter_id': id_extracted_data['encounter_id'],
                'provider_id': id_extracted_data['provider_id'],
                'patient_id': id_extracted_data['patient_id'],
                'encounter_type': id_extracted_data['encounter_type'],
                'encounter_datetime_in': id_extracted_data['encounter_datetime_in'],
                'encounter_datetime_out': id_extracted_data['encounter_datetime_out'],
                'documentation': id_documentation
            },
            'patient_data': [{
                'last_name': id_extracted_data['patient_last_name'],
                'first_name': id_extracted_data['patient_first_name'],
                'dob': id_extracted_data['patient_dob'],
            }]
        }
        completed_batch.append({
            'object_type': object_type,
            'identifier': identifier,
            'id_value': id_value,
            'extracted_data': collected_extracted_data
        })
    return completed_batch


def _parse_documents(documents):
    returned = {}
    top_page = '<title>ConsumerService Multi-View</title>'
    page_separator = "<p class='page'>"
    encounter_pattern = re.compile(r'(Consumer)(?P<space>(&nbsp;)|(\s*))(Service)(?P=space)(ID:)((?P=space)*)(?P<encounter_id>\d*)')
    encounter_documents = documents.split(page_separator)
    for encounter_document in encounter_documents:
        if top_page in encounter_document:
            continue
        matches = encounter_pattern.search(encounter_document)
        encounter_id = matches.group('encounter_id')
        returned[encounter_id] = encounter_document
    return returned


def _retrieve_event_driven_payload(original_payload):
    logging.info(f'received a notice of a new s3_object: {original_payload}')
    pattern = re.compile(r'#!#')
    event_detail = original_payload['detail']
    request_params = event_detail['requestParameters']
    bucket_name = request_params['bucketName']
    file_key = request_params['key']
    stored_event_obj = boto3.resource('s3').Object(bucket_name, file_key)
    stored_event = stored_event_obj.get()['Body'].read()
    stored_event = stored_event.replace('} {', '}#!#{')
    return [rapidjson.loads(x) for x in pattern.split(stored_event) if x]
