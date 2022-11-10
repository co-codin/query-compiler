import json
import logging
import pytest

from query_compiler.__main__ import _get_guid_and_query_from_json
from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError


def test_get_guid_and_query_from_json_positive(
        get_sample_request_json, get_sample_request_dict
):
    expected_guid, expected_query = get_sample_request_dict['guid'], \
                                    get_sample_request_dict['query']
    actual_guid, actual_query = _get_guid_and_query_from_json(
        get_sample_request_json, logging.getLogger()
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
        _get_guid_and_query_from_json(key_error_dict, logging.getLogger())
