import logging

import requests
from algernon import queued
from algernon.aws import lambda_logged

from toll_booth.obj.credible_fe import CredibleFrontEndDriver, CredibleLoginCredentials
from toll_booth.tasks.credible import CredibleTasks


def _build_driver(id_source, existing_credentials):
    session = requests.session()
    if not existing_credentials:
        return CredibleFrontEndDriver(id_source, credentials=existing_credentials, session=session)
    if not existing_credentials.validate(session=session):
        new_credentials = CredibleLoginCredentials.retrieve(id_source, session=session)
        return CredibleFrontEndDriver(id_source, credentials=new_credentials, session=session)
    return CredibleFrontEndDriver(id_source, credentials=existing_credentials, session=session)


@lambda_logged
@queued
def task(event, context):
    logging.info(f'started a call for a credible task, event: {event}')
    task_name = event['task_name']
    task_args = event.get('task_args', ())
    if task_args is None:
        task_args = ()
    task_kwargs = event.get('task_kwargs', {})
    if task_kwargs is None:
        task_kwargs = {}
    task_function = getattr(CredibleTasks, f'_{task_name}')
    id_source = task_kwargs['id_source']
    existing_credentials = CredibleTasks.get_credentials(**task_kwargs)
    driver = _build_driver(id_source, existing_credentials)
    if driver.credentials != existing_credentials:
        CredibleTasks.push_credentials(id_source=id_source, credentials=driver.credentials)
    results = task_function(*task_args, **task_kwargs, driver=driver)
    logging.info(f'completed a call for a credible task, event: {event}, results: {results}')
    return results
