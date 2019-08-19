import pytest

from toll_booth.obj.credible_fe_credentials import CredibleLoginCredentials
from algernon import ajson


@pytest.mark.credentials_i
class TestCredentials:
    def test_credential_to_json(self):
        id_source = 'ICFS'
        credentials = CredibleLoginCredentials.retrieve(id_source)
        json_string = ajson.dumps(credentials)
        assert json_string
        rebuilt_credentials = ajson.loads(json_string)
        assert isinstance(rebuilt_credentials, CredibleLoginCredentials)
        assert rebuilt_credentials.validate()
