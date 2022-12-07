import json
import logging

from typing import Dict, Tuple

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


def get_guid_and_query_from_json(query: str) -> Tuple[str, str]:
    LOG.info(f"Deserializing an input request {query}")
    try:
        payload = deserialize_json_query(query)
        guid, json_query = payload['guid'], payload['query']
    except KeyError as key_err:
        raise DeserializeJSONQueryError(query) from key_err
    else:
        LOG.info("Request deserialization successfully completed")
        return guid, json_query
