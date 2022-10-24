import requests
import json

from requests import RequestException
from requests.adapters import HTTPAdapter, Retry

from query_compiler.errors.schemas_errors import \
    HTTPErrorFromDataCatalog
from query_compiler.schemas.table import Table
from query_compiler.configs.constants import NEO4J_URL, NEO4J_PORT, RETRIES, \
    RETRY_STATUS_LIST, RETRY_METHOD_LIST, TIMEOUT


class DataCatalog:
    _attributes = {
        "patient.age": {
            "db": "main",
            "table": {"name": "patient"},
            "field": "age",
            "type": "int",
        },
        "patient.appointment": {
            "db": "main",
            "table": {
                "name": "appointment",
                "relation": (
                    {
                        "table": "patient",
                        "on": ("id", "patient_id"),
                    },
                ),
            },
            "field": "id",
            "type": "int",
        },
        "patient.appointment.at": {
            "db": "main",
            "table": {
                "name": "appointment",
                "relation": (
                    {
                        "table": "patient",
                        "on": ("id", "patient_id"),
                    },
                )
            },
            "field": "age",
            "type": "date",
        }
    }

    @classmethod
    def get_table(cls, name):
        return Table(cls._attributes[name]['table'])

    @classmethod
    def get_field(cls, name):
        return cls._attributes[name]['field']

    @classmethod
    def get_type(cls, name):
        return cls._attributes[name]['type']

    @classmethod
    def load_missing_attr_data(cls, missing_attributes):
        url = f"{NEO4J_URL}:{NEO4J_PORT}/mappings"
        http_session = cls._get_http_session(url)
        try:
            graph_req = http_session.get(
                url,
                data=json.dumps({'attributes': missing_attributes}),
                timeout=TIMEOUT
            )
            graph_req.raise_for_status()
        except RequestException as request_error:
            req = request_error.request
            raise HTTPErrorFromDataCatalog(req.url, req.headers, req.body) \
                from request_error
        else:
            cls._attributes = {
                **cls._attributes,
                **dict(zip(missing_attributes, graph_req.json()))
            }
        finally:
            http_session.close()

    @staticmethod
    def _get_http_session(url):
        retry_strategy = Retry(
            total=RETRIES,
            status_forcelist=RETRY_STATUS_LIST,
            method_whitelist=RETRY_METHOD_LIST
        )
        http_session = requests.Session()
        http_session.mount(url, HTTPAdapter(max_retries=retry_strategy))
        return http_session

    @classmethod
    def is_field_in_attributes_dict(cls, field_name):
        return field_name in cls._attributes.keys()
