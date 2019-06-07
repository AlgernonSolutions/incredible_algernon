from decimal import Decimal

import boto3
import rapidjson
from algernon.aws.gql import GqlNotary
from algernon import ajson
from botocore.exceptions import ClientError

from toll_booth.obj.credible_fe import CredibleFrontEndDriver


_get_flow = """
    query getState($flow_id: String!, $token: String){
        listStateEntries(flow_id: $flow_id, nextToken: $token){
            items{
                state_id
                state_type
                state_timestamp
                state_properties{
                    property_name
                    property_value
                }
            }
            nextToken
        }
    }
"""

_get_encounter = """
    query get_encounter($identifier_stem: String! $encounter_id: String!){
        get_vertex(identifier_stem: $identifier_stem, sid_value: $encounter_id){
            internal_id
            id_value{
                property_value{
                    ... on LocalPropertyValue{
                        data_type
                        property_value
                    }
                }
            }
            vertex_properties{
                property_name
                property_value{
                    ... on LocalPropertyValue{
                        data_type
                        property_value
                    }
                    ... on StoredPropertyValue{
                        data_type
                        storage_uri
                    }
                }
            }
        }
    }
"""

_get_client_encounters = """
    query get_client_internal_id($identifier_stem: String!, $client_id: String! $token:ID){
        get_vertex(identifier_stem: $identifier_stem, sid_value: $client_id){
            internal_id
            connected_edges(edge_labels:["_received_"], token: $token){
                page_info{
                    more
                    token
                }
                edges{
                    inbound{
                        edge_label
                        from_vertex{
                            vertex_type
                            id_value{
                                property_value{
                                    ... on LocalPropertyValue{
                                        data_type
                                        property_value
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
"""


def check_encounter_id(id_source, encounter_id, gql_endpoint):
    identifier_stem = f'#vertex#Encounter#{{\"id_source\": \"{id_source}\"}}#'
    gql_client = GqlNotary(gql_endpoint)
    variables = {
        'identifier_stem': identifier_stem,
        'encounter_id': str(encounter_id)
    }
    results = gql_client.send(_get_encounter, variables)
    parsed_results = rapidjson.loads(results)
    query_data = parsed_results['data']['get_vertex']
    return query_data


def check_client_encounter_ids(id_source, client_id, gql_endpoint):
    encounter_ids = []
    identifier_stem = f'#vertex#Patient#{{\"id_source\": \"{id_source}\"}}#'
    gql_client = GqlNotary(gql_endpoint)
    variables = {
        'identifier_stem': identifier_stem,
        'client_id': str(client_id)
    }
    results = gql_client.send(_get_client_encounters, variables)
    parsed_results = rapidjson.loads(results)
    query_data = parsed_results['data']['get_vertex']
    if not query_data:
        return None, None
    edge_data = query_data['connected_edges']['edges']['inbound']
    internal_id = query_data['internal_id']
    for edge in edge_data:
        encounter_id_value = edge['from_vertex']['id_value']['property_value']
        encounter_id = encounter_id_value['property_value']
        data_type = encounter_id_value['data_type']
        if data_type == 'N':
            encounter_id = Decimal(encounter_id)
        encounter_ids.append(encounter_id)
    return internal_id, encounter_ids


def get_remote_client_encounter_ids(id_source, client_id):
    with CredibleFrontEndDriver(id_source) as driver:
        encounter_search_data = {
            'clientvisit_id': 1,
            'client_id': client_id,
            'service_type': 1,
            'non_billable': 1,
            'client_int_id': 1,
            'emp_int_id': 1,
            'non_billable1': 3,
            'visittype': 1,
            'timein': 1,
            'timeout': 1,
            'data_dict_ids': [3, 4, 6, 70, 74, 83, 86, 87]
        }
        return driver.process_advanced_search('ClientVisit', encounter_search_data)


def get_remote_client_ids(id_source, driver=None):
    if not driver:
        driver = CredibleFrontEndDriver(id_source)
    client_search_data = {
        'teams': 1,
        'client_id': 1,
        'last_name': 1,
        'first_name': 1,
        'text28': 1,
        'dob': 1,
        'ssn': 1,
        'primary_assigned': 1,
        'client_status_f': 'ALL ACTIVE'
    }
    return driver.process_advanced_search('Clients', client_search_data)


def check_for_archived_encounter(bucket_name, id_source, client_id, encounter_id):
    file_key = f'{id_source}/{client_id}/{encounter_id}'
    stored_encounter = boto3.resource('s3').Object(bucket_name, file_key)
    try:
        encounter_response = stored_encounter.get()
        encounter = encounter_response['Body'].read()
        return rapidjson.loads(encounter)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise e


def check_flow_logs(client_id, encounter_id, state_gql_endpoint):
    results = set()
    flow_id = f"leech-psi-201905291215#get_client_encounter_ids-{client_id}#get_encounter-{encounter_id}"
    gql_client = GqlNotary(state_gql_endpoint)
    states, token = _paginate_flow(flow_id, gql_client)
    while token:
        new_states, token = _paginate_flow(flow_id, gql_client, token)
        states.extend(new_states)
    complete_events = [x for x in states if x['state_type'] == 'EventCompleted']
    for complete_event in complete_events:
        for property_entry in complete_event['state_properties']:
            if property_entry['property_name'] == 'task_results':
                results.add(property_entry['property_value'])
    if len(results) > 1:
        raise RuntimeError(f'too many results for flow_id: {flow_id}')
    for result in results:
        return ajson.loads(result)


def publish_to_leech(bullhorn,
                     flow_id,
                     id_source,
                     patient_id,
                     provider_id,
                     encounter_id,
                     encounter_text,
                     encounter_datetime_in,
                     encounter_datetime_out,
                     encounter_type,
                     patient_last_name,
                     patient_first_name,
                     patient_dob):
    encounter_data = {
        'source': {
            'patient_id': patient_id,
            'provider_id': provider_id,
            'id_source': id_source,
            'encounter_id': encounter_id,
            'documentation': encounter_text,
            'encounter_datetime_in': encounter_datetime_in,
            'encounter_datetime_out': encounter_datetime_out,
            'encounter_type': encounter_type
        },
        'patient_data': [{
            'last_name': patient_last_name,
            'first_name': patient_first_name,
            'dob':patient_dob
        }]
    }
    message = {
        'task_name': 'leech',
        'task_kwargs': {
            'object_type': 'Encounter',
            'extracted_data': encounter_data
        },
        'flow_id': f'{flow_id}#leech'
    }
    listener_arn = bullhorn.find_task_arn('leech')
    strung_event = ajson.dumps(message)
    return bullhorn.publish('new_event', listener_arn, strung_event)


def _paginate_flow(flow_id, gql_client, token=None):
    variables = {'flow_id': flow_id}
    if flow_id:
        variables.update({'nextToken': token})
    response = gql_client.send(_get_flow, variables)
    results = rapidjson.loads(response)
    entry_data = results['data']['listStateEntries']
    items = entry_data.get('items')
    token = entry_data.get('nextToken')
    return items, token
