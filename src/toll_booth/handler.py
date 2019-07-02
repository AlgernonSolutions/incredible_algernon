import logging

from algernon import queued, rebuild_event
from algernon.aws import lambda_logged
from aws_xray_sdk.core import xray_recorder

from toll_booth import tasks


@lambda_logged
@queued
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
