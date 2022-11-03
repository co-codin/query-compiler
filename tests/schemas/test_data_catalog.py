import json
import pytest
import requests

from unittest.mock import patch, Mock
from requests.exceptions import RequestException
from requests.adapters import HTTPAdapter, Retry

from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.errors.schemas_errors import HTTPErrorFromDataCatalog
from query_compiler.configs.settings import settings


@pytest.mark.parametrize(
    'field_name, is_in_attributes',
    (
            ('attribute1', True),
            ('attribute2', False)
    )
)
def test_is_field_in_attributes_dict(field_name, is_in_attributes):
    DataCatalog._attributes = {'attribute1': {}}
    actual_output = DataCatalog.is_field_in_attributes_dict(field_name)
    assert is_in_attributes == actual_output


def test_get_http_session_positive():
    url = f"{settings.data_catalog_url}:{settings.data_catalog_port}"
    retry_strategy = Retry(
        total=settings.retries,
        status_forcelist=settings.retry_status_list,
        method_whitelist=settings.retry_method_list
    )
    expected_http_session = requests.Session()
    expected_http_session.hooks["response"] = [
        lambda response, *args, **kwargs: response.raise_for_status()
    ]
    expected_http_session.mount(url, HTTPAdapter(max_retries=retry_strategy))
    actual_http_session = DataCatalog._get_http_session(url)
    expected_hooks = expected_http_session.hooks
    actual_hooks = actual_http_session.hooks
    expected_retry: Retry = expected_http_session.adapters[url].max_retries
    actual_retry: Retry = actual_http_session.adapters[url].max_retries

    assert len(expected_hooks['response']) == len(actual_hooks['response'])
    assert len(expected_hooks['response']) == 1
    assert expected_retry.total == actual_retry.total
    assert expected_retry.status_forcelist == actual_retry.status_forcelist
    assert expected_retry.allowed_methods == actual_retry.allowed_methods


@patch('query_compiler.schemas.data_catalog.DataCatalog._get_http_session')
def test_load_missing_attr_data_positive(mock_get_http_session: Mock):
    data_catalog_response = {
        'table': {
            'name': 'some_name', 'relation': ('some_relation',)
        },
        'field': 'some_field',
        'type': 'some_type'
    }
    mock_http_session: Mock = mock_get_http_session.return_value
    mock_get: Mock = mock_http_session.get
    mock_json = mock_get.return_value.json
    mock_json.return_value = data_catalog_response
    missing_attribute = 'missing_attribute'
    url = f"{settings.data_catalog_url}:{settings.data_catalog_port}" \
          f"/mappings/{missing_attribute}"
    DataCatalog.load_missing_attr_data(missing_attribute)

    assert DataCatalog._attributes[missing_attribute] == \
           data_catalog_response
    mock_get_http_session.assert_called_once_with(url)
    mock_get.assert_called_once_with(url, timeout=settings.timeout)
    mock_http_session.close.called_once()


@patch('query_compiler.schemas.data_catalog.DataCatalog._get_http_session')
def test_load_missing_attr_data_request_exception(mock_get_http_session: Mock):
    mock_http_session: Mock = mock_get_http_session.return_value
    mock_get: Mock = mock_http_session.get
    missing_attribute = 'missing_attribute'
    url = f"{settings.data_catalog_url}:{settings.data_catalog_port}" \
          f"/mappings/{missing_attribute}"
    mock_get.side_effect = RequestException(
        request=Mock(url=url, headers=None, data=None)
    )
    with pytest.raises(HTTPErrorFromDataCatalog):
        DataCatalog.load_missing_attr_data(missing_attribute)
    mock_get_http_session.assert_called_once_with(url)
    mock_get.assert_called_once_with(url, timeout=settings.timeout)
    mock_http_session.close.called_once()


@patch('query_compiler.schemas.data_catalog.DataCatalog._get_http_session')
def test_load_missing_attr_data_list_positive(mock_get_http_session: Mock):
    data_catalog_response = (
        {
            'table': {
                'name': 'some_name', 'relation': ('some_relation',)
            },
            'field': 'missing_attribute1',
            'type': 'some_type'
        },
        {
            'table': {
                'name': 'some_name', 'relation': ('some_relation',)
            },
            'field': 'missing_attribute2',
            'type': 'some_type'
        }
    )
    mock_http_session: Mock = mock_get_http_session.return_value
    mock_get: Mock = mock_http_session.get
    mock_json = mock_get.return_value.json
    mock_json.return_value = data_catalog_response
    missing_attributes = ['missing_attribute1', 'missing_attribute2']
    url = f"{settings.data_catalog_url}:{settings.data_catalog_port}" \
          f"/mappings"
    DataCatalog.load_missing_attr_data_list(missing_attributes)

    for key, field_data in zip(missing_attributes, data_catalog_response):
        assert DataCatalog._attributes[key] == field_data
    mock_get_http_session.assert_called_once_with(url)
    mock_get.assert_called_once_with(
        url,
        data=json.dumps({'attributes': missing_attributes}),
        timeout=settings.timeout
    )
    mock_http_session.close.called_once()


@patch('query_compiler.schemas.data_catalog.DataCatalog._get_http_session')
def test_load_missing_attr_data_list_request_exception(
        mock_get_http_session: Mock
):
    mock_http_session: Mock = mock_get_http_session.return_value
    mock_get: Mock = mock_http_session.get
    missing_attributes = ['missing_attribute1', 'missing_attribute2']
    url = f"{settings.data_catalog_url}:{settings.data_catalog_port}" \
          f"/mappings"
    mock_get.side_effect = RequestException(
        request=Mock(url=url, headers=None, data=None)
    )
    with pytest.raises(HTTPErrorFromDataCatalog):
        DataCatalog.load_missing_attr_data_list(missing_attributes)
    mock_get_http_session.assert_called_once_with(url)
    mock_get.assert_called_once_with(
        url,
        data=json.dumps({'attributes': missing_attributes}),
        timeout=settings.timeout
    )
    mock_http_session.close.called_once()
