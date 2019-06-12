import logging

from algernon import ajson, queued
from algernon.aws import lambda_logged

from toll_booth import tasks


def _rebuild_event(original_event):
    return ajson.loads(ajson.dumps(original_event))


@lambda_logged
@queued
def handler(event, context):
    logging.info(f'started a call for a credible task, event/context: {event}/{context}')
    event = _rebuild_event(event)
    task_name = event['task_name']
    task_kwargs = event.get('task_kwargs', {})
    if task_kwargs is None:
        task_kwargs = {}
    task_function = getattr(tasks, task_name)
    driver = tasks.build_driver(task_kwargs['id_source'])
    results = task_function(**task_kwargs, driver=driver)
    logging.info(f'completed a call for a credible task, event: {event}, results: {results}')
    return ajson.dumps(results)
