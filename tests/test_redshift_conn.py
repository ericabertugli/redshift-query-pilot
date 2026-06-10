import signal
import time
from unittest.mock import patch

import pytest

from redshift_conn import SamlAuthTimeoutError, connect_password, connect_saml


class TestSamlAuthTimeoutError:
    def test_message(self):
        exc = SamlAuthTimeoutError("test msg")
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

    def test_timeout_raises_exception(self, monkeypatch):
        monkeypatch.setattr("redshift_conn.redshift_connector.connect", lambda **kw: time.sleep(10))
        with pytest.raises(SamlAuthTimeoutError):
            connect_saml(
                host="h",
                cluster="c",
                database="d",
                user="u",
                login_url="https://idp.example.com",
                auth_timeout=1,
            )
