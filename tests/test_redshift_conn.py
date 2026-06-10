import signal
from unittest.mock import patch

import pytest

from redshift_conn import SamlAuthTimeout, connect_password, connect_saml


class TestSamlAuthTimeout:
    def test_is_exception(self):
        assert issubclass(SamlAuthTimeout, Exception)

    def test_message(self):
        exc = SamlAuthTimeout("test msg")
        assert str(exc) == "test msg"


class TestConnectPassword:
    @patch("redshift_conn.redshift_connector.connect")
    def test_calls_connector_with_correct_args(self, mock_connect):
        mock_connect.return_value = "connection"

        result = connect_password(
            host="host.redshift.amazonaws.com",
            database="dev",
            user="admin",
            password="secret",
        )

        mock_connect.assert_called_once_with(
            host="host.redshift.amazonaws.com",
            port=5439,
            database="dev",
            user="admin",
            password="secret",
        )
        assert result == "connection"

    @patch("redshift_conn.redshift_connector.connect")
    def test_default_port_is_5439(self, mock_connect):
        connect_password(host="h", database="d", user="u", password="p")
        assert mock_connect.call_args[1]["port"] == 5439


class TestConnectSaml:
    @patch("redshift_conn.redshift_connector.connect")
    def test_calls_connector_with_saml_params(self, mock_connect):
        mock_connect.return_value = "conn"

        result = connect_saml(
            host="h",
            cluster="c",
            database="d",
            user="u",
            login_url="https://idp.example.com",
            region="us-east-1",
            auth_timeout=60,
        )

        mock_connect.assert_called_once_with(
            iam=True,
            host="h",
            port=5439,
            cluster_identifier="c",
            database="d",
            db_user="u",
            region="us-east-1",
            credentials_provider="BrowserSamlCredentialsProvider",
            login_url="https://idp.example.com",
        )
        assert result == "conn"

    @patch("redshift_conn.redshift_connector.connect")
    def test_restores_original_signal_handler(self, mock_connect):
        original = signal.getsignal(signal.SIGALRM)
        connect_saml(
            host="h",
            cluster="c",
            database="d",
            user="u",
            login_url="https://idp.example.com",
        )
        restored = signal.getsignal(signal.SIGALRM)
        assert restored == original

    @patch("redshift_conn.redshift_connector.connect")
    def test_default_region_and_timeout(self, mock_connect):
        connect_saml(
            host="h",
            cluster="c",
            database="d",
            user="u",
            login_url="https://idp.example.com",
        )
        assert mock_connect.call_args[1]["region"] == "eu-west-1"

    def test_timeout_raises_exception(self, monkeypatch):
        import time
        monkeypatch.setattr("redshift_conn.redshift_connector.connect", lambda **kw: time.sleep(10))
        with pytest.raises(SamlAuthTimeout):
            connect_saml(
                host="h",
                cluster="c",
                database="d",
                user="u",
                login_url="https://idp.example.com",
                auth_timeout=1,
            )
