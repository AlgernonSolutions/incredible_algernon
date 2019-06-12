import logging
import os
from typing import Any

import boto3
import requests
from botocore.exceptions import ClientError

from algernon import ajson

from toll_booth.obj.credible_fe import CredibleFrontEndDriver, CredibleLoginCredentials


def build_driver(id_source):
    existing_credentials = _get_credentials(id_source)
    logging.info(f'retrieved existing credentials')
    driver = _construct_driver(id_source, existing_credentials)
    if driver.credentials != existing_credentials:
        logging.debug(f'after constructing the driver, '
                      f'the existing credentials are no longer valid, '
                      f'pushing new credentials')
        _push_credentials(id_source=id_source, credentials=driver.credentials)
    return driver


def _construct_driver(id_source, existing_credentials):
    session = requests.session()
    if not existing_credentials:
        return CredibleFrontEndDriver(id_source, credentials=existing_credentials, session=session)
    if not existing_credentials.validate(session=session):
        logging.debug(f'existing credentials are currently invalid, retrieving new ones')
        new_credentials = CredibleLoginCredentials.retrieve(id_source, session=session)
        logging.debug(f'retrieved new credentials')
        return CredibleFrontEndDriver(id_source, credentials=new_credentials, session=session)
    logging.debug(f'existing credentials are valid, no need to retrieve new ones')
    return CredibleFrontEndDriver(id_source, credentials=existing_credentials, session=session)


def _get_credentials(id_source):
    try:
        credentials = _download_object(os.environ['STORAGE_BUCKET'], id_source, 'credentials')
    except ClientError:
        return None
    return credentials


def _push_credentials(**kwargs):
    id_source = kwargs['id_source']
    _upload_object(os.environ['STORAGE_BUCKET'], id_source, 'credentials', kwargs['credentials'])


def _upload_object(bucket_name: str, folder_name: str, object_name: str, obj: Any):
    resource = boto3.resource('s3')
    object_key = f'{folder_name}/{object_name}'
    resource.Object(bucket_name, object_key).put(Body=ajson.dumps(obj))


def _download_object(bucket_name: str, folder_name: str, object_name: str) -> Any:
    resource = boto3.resource('s3')
    object_key = f'{folder_name}/{object_name}'
    stored_object = resource.Object(bucket_name, object_key).get()
    string_body = stored_object['Body'].read()
    return ajson.loads(string_body)
