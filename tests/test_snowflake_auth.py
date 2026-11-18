import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from lakehouse.snowflake_auth import MissingSnowflakeConfig, connection_kwargs


def _pem() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption()).decode()


def test_key_pair_auth_from_env():
    kwargs = connection_kwargs({"SNOWFLAKE_ACCOUNT": "acct", "SNOWFLAKE_USER": "svc", "SNOWFLAKE_PRIVATE_KEY": _pem()})
    assert isinstance(kwargs["private_key"], bytes)
    assert "password" not in kwargs
    assert kwargs["role"] == "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE"
    assert kwargs["warehouse"] == "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS"


def test_escaped_newlines_in_secret_are_accepted():
    pem = _pem().replace("\n", "\\n")  # how a one-line CI secret often arrives
    assert connection_kwargs({"SNOWFLAKE_ACCOUNT": "a", "SNOWFLAKE_USER": "u", "SNOWFLAKE_PRIVATE_KEY": pem})


def test_missing_key_or_account_fails_clearly():
    with pytest.raises(MissingSnowflakeConfig, match="SNOWFLAKE_ACCOUNT"):
        connection_kwargs({"SNOWFLAKE_USER": "u"})
    with pytest.raises(MissingSnowflakeConfig, match="key-pair"):
        connection_kwargs({"SNOWFLAKE_ACCOUNT": "a", "SNOWFLAKE_USER": "u", "SNOWFLAKE_PASSWORD": "nope"})
