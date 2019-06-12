from algernon.aws import Bullhorn
from algernon import ajson

from toll_booth.obj.credible_fe import CredibleFrontEndDriver
from admin import sorter


id_source = 'PSI'
bucket_name = 'algernonsolutions-gentlemen-dev'
gql_endpoint = 'jlgmowxwofe33pdekndakyzx4i.appsync-api.us-east-1.amazonaws.com'
state_gql_endpoint = 'mh5syterirdvzji7tdbrrmpe7m.appsync-api.us-east-1.amazonaws.com'
bullhorn = Bullhorn.retrieve(profile='dev')
auditor_listener = bullhorn.find_task_arn('auditor')
leech = bullhorn.find_task_arn('leech')
client_progress = 0

with CredibleFrontEndDriver(id_source) as driver:
    remote_clients = sorter.get_remote_client_ids(id_source, driver=driver)
    client_total = len(remote_clients)
    for client in remote_clients:
        encounter_progress = 0
        client_id = client[' Id']
        remote_encounters = sorter.get_remote_client_encounter_ids(id_source, client_id)
        remote_encounter_data = {x['Service ID']: x for x in remote_encounters}
        encounter_total = len(remote_encounter_data)
        for encounter_id, remote_encounter in remote_encounter_data.items():
            task_kwargs = {
                'id_source': id_source,
                'client_id': client_id,
                'encounter_id': encounter_id,
                'encounter_data': remote_encounter,
                'encounter_bucket': bucket_name
            }
            msg = {
                'task_name': 'audit_surgeon_graph',
                'task_kwargs': task_kwargs,
                'flow_id': f'surgical_review-2-{client_id}-{encounter_id}'
            }
            bullhorn.publish('new_event', auditor_listener, ajson.dumps(msg))
            encounter_progress += 1
            client_display_total = f'{client_progress}/{client_total}'
            encounter_display_total = f'{encounter_progress}/{encounter_total}'
            print(f'##### {client_display_total} ##### {client_id} ##### {encounter_display_total} ##### ')
        client_progress += 1
        print(f'##### ##### {client_progress}/{client_total} ##### ##### ')
