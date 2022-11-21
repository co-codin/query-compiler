import pytest
import json

from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError
from query_compiler.utils.parse_utils import (
    get_guid_and_query_from_json, deserialize_json_query
)


def test_deserialize_request_positive(
        get_sample_request_json, get_sample_request_dict
):
    actual_query_dict = deserialize_json_query(get_sample_request_json)
    assert actual_query_dict == get_sample_request_dict


@pytest.mark.parametrize('request_', ('', 'some string', '11', '[]'))
def test_deserialize_request_decode_error(request_):
    with pytest.raises(DeserializeJSONQueryError):
        deserialize_json_query(request_)


def test_get_guid_and_query_from_json_positive(
        get_sample_request_json, get_sample_request_dict
):
    expected_guid, expected_query = get_sample_request_dict['guid'], \
                                    get_sample_request_dict['query']
    actual_guid, actual_query = get_guid_and_query_from_json(
        get_sample_request_json
    )
    assert expected_guid == actual_guid
    assert expected_query == actual_query


@pytest.mark.parametrize(
    'key_error_dict',
    (
        json.dumps({'guid': 1}),
        json.dumps({'query': {}})
    )
)
def test_get_guid_and_query_from_json_key_error(key_error_dict):
    with pytest.raises(DeserializeJSONQueryError):
        get_guid_and_query_from_json(key_error_dict)
