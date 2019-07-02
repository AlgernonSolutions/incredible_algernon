import rapidjson
from algernon.aws import Bullhorn
from algernon import ajson

file_name = '413dadbf-d939-42da-9e3d-3634cee1388d!1561142042.186607.json'
bullhorn = Bullhorn.retrieve()

with open(file_name) as file:
    json_file = rapidjson.load(file)
    encounters = json_file['data_string']['encounters']
    id_source = json_file['data_string']['id_source']
    task_name = 'check_credible_object'
    topic_arn = bullhorn.find_task_arn(task_name)
    progress = 122635
    total = len(encounters)
    with bullhorn as batch:
        for pointer, encounter in enumerate(encounters):
            if pointer <= 122635:
                continue
            encounter['id_source'] = id_source
            msg = {
                'task_name': task_name,
                'task_kwargs': {
                    'obj': encounter,
                    'object_type': 'Encounter'
                }
            }
            topic_arn = bullhorn.find_task_arn(task_name)
            results = bullhorn.publish('test_event', topic_arn, ajson.dumps(msg))
            progress += 1
            print(f'{progress}/{total}')
