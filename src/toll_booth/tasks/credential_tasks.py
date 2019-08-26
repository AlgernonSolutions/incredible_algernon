import logging
import os
from typing import Any

import boto3
import requests
from aws_xray_sdk.core import xray_recorder
from botocore.exceptions import ClientError

from algernon import ajson

from toll_booth.obj.credible_fe import CredibleFrontEndDriver
from toll_booth.obj.credible_fe_credentials import CredibleLoginCredentials


@xray_recorder.capture()
def build_driver(id_source, existing_credentials=None):
    if not existing_credentials:
        existing_credentials = _get_credentials(id_source)
        logging.info(f'retrieved existing credentials')
    driver = _construct_driver(id_source, existing_credentials)
    if driver.credentials != existing_credentials:
        logging.debug(f'after constructing the driver, '
                      f'the existing credentials are no longer valid, '
                      f'pushing new credentials')
        _push_credentials(id_source=id_source, credentials=driver.credentials)
    return driver


@xray_recorder.capture()
def _construct_driver(id_source, existing_credentials: CredibleLoginCredentials):
    if not existing_credentials:
        logging.info(f'no existing credentials, retrieving new ones')
        return CredibleFrontEndDriver(id_source, credentials=existing_credentials)
    if not existing_credentials.validate():
        session = requests.session()
        logging.info(f'existing credentials are currently invalid, retrieving new ones')
        new_credentials = CredibleLoginCredentials.retrieve(id_source, session=session)
        logging.debug(f'retrieved new credentials')
        return CredibleFrontEndDriver(id_source, credentials=new_credentials)
    logging.debug(f'existing credentials are valid, no need to retrieve new ones')
    return CredibleFrontEndDriver(id_source, credentials=existing_credentials)


# @xray_recorder.capture()
def _get_credentials(id_source):
    try:
        credentials = _download_object(os.environ['STORAGE_BUCKET_NAME'], id_source, 'credentials')
    except ClientError:
        return None
    return credentials


# @xray_recorder.capture()
def _push_credentials(**kwargs):
    id_source = kwargs['id_source']
    _upload_object(os.environ['STORAGE_BUCKET_NAME'], id_source, 'credentials', kwargs['credentials'])


# @xray_recorder.capture()
def _upload_object(bucket_name: str, folder_name: str, object_name: str, obj: Any):
    resource = boto3.resource('s3')
    object_key = f'{folder_name}/{object_name}'
    resource.Object(bucket_name, object_key).put(Body=ajson.dumps(obj))


# @xray_recorder.capture()
def _download_object(bucket_name: str, folder_name: str, object_name: str) -> Any:
    resource = boto3.resource('s3')
    object_key = f'{folder_name}/{object_name}'
    stored_object = resource.Object(bucket_name, object_key).get()
    string_body = stored_object['Body'].read()
    return ajson.loads(string_body)
