"""Shared Redshift connection factory.

Two authentication approaches:
- connect_saml(): browser-based IdP authentication (e.g. Okta).
- connect_password(): direct user/password connection.
"""

import signal

import redshift_connector


class SamlAuthTimeoutError(Exception):
    """Raised when SAML authentication times out."""

    pass


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

    def timeout_handler(signum, frame):
        raise SamlAuthTimeoutError(
            f"SAML authentication timed out after {auth_timeout} seconds"
        )

    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(auth_timeout)
    try:
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
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


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
