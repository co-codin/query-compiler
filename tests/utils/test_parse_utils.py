import pytest

from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError
from query_compiler.utils.parse_utils import deserialize_json_query


def test_deserialize_request_positive(
        get_sample_request_json, get_sample_request_dict
):
    actual_query_dict = deserialize_json_query(get_sample_request_json)
    assert actual_query_dict == get_sample_request_dict


@pytest.mark.parametrize('request_', ('', 'some string', '11', '[]'))
def test_deserialize_request_decode_error(request_):
    with pytest.raises(DeserializeJSONQueryError):
        deserialize_json_query(request_)

