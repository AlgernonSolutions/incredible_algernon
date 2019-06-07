import logging

import requests
from algernon.aws import lambda_logged
from algernon import ajson

from toll_booth.obj.credible_fe import CredibleFrontEndDriver, CredibleLoginCredentials
from toll_booth.tasks.credible import CredibleTasks


def _build_driver(id_source, existing_credentials):
    session = requests.session()
    if not existing_credentials:
        return CredibleFrontEndDriver(id_source, credentials=existing_credentials, session=session)
    if not existing_credentials.validate(session=session):
        logging.info(f'existing credentials are currently invalid, retrieving new ones')
        new_credentials = CredibleLoginCredentials.retrieve(id_source, session=session)
        logging.info(f'retrieved new credentials: {new_credentials}')
        return CredibleFrontEndDriver(id_source, credentials=new_credentials, session=session)
    logging.info(f'existing credentials are valid, no need to retrieve new ones')
    return CredibleFrontEndDriver(id_source, credentials=existing_credentials, session=session)


def _rebuild_event(original_event):
    return ajson.loads(ajson.dumps(original_event))


@lambda_logged
def task(event, context):
    logging.info(f'started a call for a credible task, event: {event}')
    event = _rebuild_event(event)
    task_name = event['task_name']
    flow_id = event['flow_id']
    task_args = event.get('task_args', ())
    if task_args is None:
        task_args = ()
    task_kwargs = event.get('task_kwargs', {})
    if task_kwargs is None:
        task_kwargs = {}
    task_function = getattr(CredibleTasks, f'_{task_name}')
    id_source = task_kwargs['id_source']
    existing_credentials = CredibleTasks.get_credentials(**task_kwargs)
    logging.info(f'retrieved existing credentials: {existing_credentials}')
    driver = _build_driver(id_source, existing_credentials)
    if driver.credentials != existing_credentials:
        logging.info(f'after constructing the driver, '
                     f'the existing credentials are no longer valid, pushing new credentials: {driver.credentials}')
        CredibleTasks.push_credentials(id_source=id_source, credentials=driver.credentials)
    results = task_function(*task_args, **task_kwargs, flow_id=flow_id, driver=driver)
    logging.info(f'completed a call for a credible task, event: {event}, results: {results}')
    return ajson.dumps(results)
