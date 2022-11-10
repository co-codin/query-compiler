import requests
import json

from typing import List
from requests import RequestException
from requests.adapters import HTTPAdapter, Retry

from query_compiler.errors.schemas_errors import \
    HTTPErrorFromDataCatalog
from query_compiler.schemas.table import Table
from query_compiler.configs.settings import settings


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
        },
        "case.doctor.person.name_sat.family_name": {
            "table": {
                "name": "dv_raw.person_name_sat",
                "relation": [
                    {
                        "table": "dv_raw.case_hub",
                        "on": ["_hash_key", "idcase_hash_fkey"]
                    },
                    {
                        "table": "dv_raw.case_doctor_link",
                        "on": ["iddoctor_hash_fkey", "iddoctor_hash_fkey"]
                    },
                    {
                        "table": "dv_raw.doctor_person_link",
                        "on": ["idperson_hash_fkey", "_hash_fkey"]
                    }
                ]
            },
            "field": "familyname",
            "type": "string"
        },
        "case.biz_key": {
            "table": {"name": "dv_raw.case_hub", "relation": []},
            "field": "_biz_key", "type": "string"
        },
        "case.doctor.person.sat.birth_date": {
            "table": {
                "name": "dv_raw.person_sat",
                "relation": [
                    {
                        "table": "dv_raw.case_hub",
                        "on": ["_hash_key", "idcase_hash_fkey"]
                    },
                    {
                        "table": "dv_raw.case_doctor_link",
                        "on": ["iddoctor_hash_fkey", "iddoctor_hash_fkey"]
                    },
                    {
                        "table": "dv_raw.doctor_person_link",
                        "on": ["idperson_hash_fkey", "_hash_fkey"]
                    }
                ]
            },
            "field": "birthdate",
            "type": "string"
        },
        "case.sat.open_date": {
            "table": {
                "name": "dv_raw.case_sat",
                "relation": [
                    {
                        "table": "dv_raw.case_hub",
                        "on": ["_hash_key", "_hash_fkey"]
                    }
                ]
            },
            "field": "opendate",
            "type": "string"
        }
    }

    @classmethod
    def get_table(cls, name: str) -> Table:
        return Table(cls._attributes[name]['table'])

    @classmethod
    def get_field(cls, name: str) -> str:
        return cls._attributes[name]['field']

    @classmethod
    def get_type(cls, name: str) -> str:
        try:
            attr_data = cls._attributes[name]
        except KeyError:
            cls.load_missing_attr_data(name)
            attr_data = cls._attributes[name]
        return attr_data['type']

    @classmethod
    def load_missing_attr_data(cls, attr_name: str):
        url = f"{settings.data_catalog_url}:" \
              f"{settings.data_catalog_port}/mappings/{attr_name}"
        http_session = cls._get_http_session(url)
        try:
            graph_req = http_session.get(
                url,
                timeout=settings.timeout
            )
        except RequestException as request_err:
            req = request_err.request
            raise HTTPErrorFromDataCatalog(req.url, req.headers, req.body) \
                from request_err
        else:
            cls._attributes[attr_name] = graph_req.json()
        finally:
            http_session.close()

    @classmethod
    def load_missing_attr_data_list(cls, missing_attributes: List[str]):
        url = f"{settings.data_catalog_url}:" \
              f"{settings.data_catalog_port}/mappings"
        http_session = cls._get_http_session(url)
        try:
            graph_req = http_session.get(
                url,
                data=json.dumps({'attributes': missing_attributes}),
                timeout=settings.timeout
            )
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
    def _get_http_session(url: str) -> requests.Session:
        retry_strategy = Retry(
            total=settings.retries,
            status_forcelist=settings.retry_status_list,
            method_whitelist=settings.retry_method_list
        )
        http_session = requests.Session()
        http_session.hooks["response"] = [
            lambda response, *args, **kwargs: response.raise_for_status()
        ]
        http_session.mount(url, HTTPAdapter(max_retries=retry_strategy))
        return http_session

    @classmethod
    def is_field_in_attributes_dict(cls, field_name: str) -> bool:
        return field_name in cls._attributes.keys()
