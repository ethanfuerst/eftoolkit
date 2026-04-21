"""Tests for load_service_account_credentials."""

import json
from unittest.mock import MagicMock, patch

import pytest

from eftoolkit.gsheets.utils import load_service_account_credentials

# Minimal synthetic service account payload — not a real key
SAMPLE_SA_JSON = (
    '{"type": "service_account", "project_id": "demo", '
    '"private_key_id": "abc123", '
    '"private_key": "-----BEGIN PRIVATE KEY-----\\nFAKE\\n-----END PRIVATE KEY-----\\n", '
    '"client_email": "demo@demo.iam.gserviceaccount.com", '
    '"client_id": "1", "auth_uri": "https://accounts.google.com/o/oauth2/auth", '
    '"token_uri": "https://oauth2.googleapis.com/token"}'
)


def test_load_service_account_credentials_returns_credentials(monkeypatch):
    """load_service_account_credentials builds a Credentials object from env."""
    monkeypatch.setenv('GSPREAD_CREDENTIALS', SAMPLE_SA_JSON)

    with patch(
        'eftoolkit.gsheets.utils.Credentials.from_service_account_info'
    ) as mock_from_info:
        mock_creds = MagicMock()
        mock_from_info.return_value = mock_creds

        result = load_service_account_credentials()

    assert result is mock_creds
    info_arg, _ = mock_from_info.call_args
    assert info_arg[0]['type'] == 'service_account'
    assert info_arg[0]['project_id'] == 'demo'


def test_load_service_account_credentials_default_scopes(monkeypatch):
    """load_service_account_credentials uses Sheets + Drive scopes by default."""
    monkeypatch.setenv('GSPREAD_CREDENTIALS', SAMPLE_SA_JSON)

    with patch(
        'eftoolkit.gsheets.utils.Credentials.from_service_account_info'
    ) as mock_from_info:
        load_service_account_credentials()

    _, kwargs = mock_from_info.call_args

    assert kwargs['scopes'] == [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]


def test_load_service_account_credentials_custom_scopes(monkeypatch):
    """load_service_account_credentials passes through custom scopes."""
    monkeypatch.setenv('GSPREAD_CREDENTIALS', SAMPLE_SA_JSON)
    custom = ['https://www.googleapis.com/auth/bigquery']

    with patch(
        'eftoolkit.gsheets.utils.Credentials.from_service_account_info'
    ) as mock_from_info:
        load_service_account_credentials(scopes=custom)

    _, kwargs = mock_from_info.call_args

    assert kwargs['scopes'] == custom


def test_load_service_account_credentials_custom_env(monkeypatch):
    """load_service_account_credentials accepts a non-default env var name."""
    monkeypatch.delenv('GSPREAD_CREDENTIALS', raising=False)
    monkeypatch.setenv('MY_CREDS', SAMPLE_SA_JSON)

    with patch(
        'eftoolkit.gsheets.utils.Credentials.from_service_account_info'
    ) as mock_from_info:
        load_service_account_credentials(env='MY_CREDS')

    info_arg, _ = mock_from_info.call_args

    assert info_arg[0]['client_email'] == 'demo@demo.iam.gserviceaccount.com'


def test_load_service_account_credentials_missing_env_raises(monkeypatch):
    """load_service_account_credentials raises when env var is unset."""
    monkeypatch.delenv('GSPREAD_CREDENTIALS', raising=False)

    with pytest.raises(ValueError, match="'GSPREAD_CREDENTIALS' is not set"):
        load_service_account_credentials()


def test_load_service_account_credentials_invalid_json(monkeypatch):
    """load_service_account_credentials raises JSONDecodeError for bad JSON."""
    monkeypatch.setenv('GSPREAD_CREDENTIALS', 'not valid json')

    with pytest.raises(json.JSONDecodeError):
        load_service_account_credentials()


def test_load_service_account_credentials_handles_literal_newlines(monkeypatch):
    """load_service_account_credentials handles env vars with real LF chars."""
    # Simulate what happens when a UI paste unescapes \n into real newlines
    raw = (
        '{"type": "service_account", "project_id": "demo", '
        '"private_key_id": "abc", '
        '"private_key": "-----BEGIN-----\nKEY\n-----END-----\n", '
        '"client_email": "demo@demo.iam.gserviceaccount.com", '
        '"client_id": "1", "auth_uri": "x", "token_uri": "y"}'
    )
    monkeypatch.setenv('GSPREAD_CREDENTIALS', raw)

    with patch(
        'eftoolkit.gsheets.utils.Credentials.from_service_account_info'
    ) as mock_from_info:
        load_service_account_credentials()

    info_arg, _ = mock_from_info.call_args

    assert info_arg[0]['private_key'] == '-----BEGIN-----\nKEY\n-----END-----\n'
