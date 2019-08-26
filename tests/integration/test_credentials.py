import os

import pytest

from toll_booth.obj.credible_fe_credentials import CredibleLoginCredentials
from toll_booth.tasks.credential_tasks import _push_credentials, _get_credentials
from algernon import ajson


@pytest.mark.credentials_i
class TestCredentials:
    def test_credentials_upload_download(self):
        os.environ['STORAGE_BUCKET_NAME'] = 'algernonsolutions-leech-prod'
        id_source = 'ICFS'
        credentials = CredibleLoginCredentials.retrieve(id_source)
        _push_credentials(id_source=id_source, credentials=credentials)
        pulled_credentials = _get_credentials(id_source)
        assert pulled_credentials
        credentials.refresh_if_stale(30)

    def test_credentials_timing(self):
        id_source = 'ICFS'
        credentials = CredibleLoginCredentials.retrieve(id_source)
        credentials.refresh_if_stale(30)

    def test_credential_to_json(self):
        id_source = 'ICFS'
        credentials = CredibleLoginCredentials.retrieve(id_source)
        json_string = ajson.dumps(credentials)
        assert json_string
        rebuilt_credentials = ajson.loads(json_string)
        assert isinstance(rebuilt_credentials, CredibleLoginCredentials)
        assert rebuilt_credentials.validate()
