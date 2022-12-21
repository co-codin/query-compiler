import json
import logging

from typing import Dict

from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError


LOG = logging.getLogger(__name__)


def deserialize_json_query(json_query: str) -> Dict:
    try:
        payload = json.loads(json_query)
    except json.JSONDecodeError as json_decode_err:
        raise DeserializeJSONQueryError(json_query) from json_decode_err
    if not isinstance(payload, Dict):
        raise DeserializeJSONQueryError(json_query)
    return payload
