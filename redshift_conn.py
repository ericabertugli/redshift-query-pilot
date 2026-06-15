"""Shared Redshift connection factory.

Two authentication approaches:
- connect_saml(): browser-based IdP authentication (e.g. Okta).
- connect_password(): direct user/password connection.
"""

import redshift_connector

from timeout import ToolTimeoutError, with_timeout

# Keep backward-compatible alias so existing callers importing
# ``SamlAuthTimeoutError`` from this module keep working.
SamlAuthTimeoutError = ToolTimeoutError


def connect_saml(
    host: str,
    cluster: str,
    database: str,
    user: str,
    login_url: str,
    region: str = "eu-west-1",
    auth_timeout: int = 120,
) -> redshift_connector.Connection:
    """Connect to Redshift via browser-based SAML/IdP authentication."""

    @with_timeout(auth_timeout)
    def _connect():
        return redshift_connector.connect(
            iam=True,
            host=host,
            port=5439,
            cluster_identifier=cluster,
            database=database,
            db_user=user,
            region=region,
            credentials_provider="BrowserSamlCredentialsProvider",
            login_url=login_url,
        )

    return _connect()


def connect_password(
    host: str,
    database: str,
    user: str,
    password: str,
) -> redshift_connector.Connection:
    """Connect to Redshift via user/password authentication."""
    return redshift_connector.connect(
        host=host,
        port=5439,
        database=database,
        user=user,
        password=password,
    )
