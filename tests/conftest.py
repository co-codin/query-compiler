import pytest

from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.schemas.attribute import AliasStorage, Attribute


@pytest.fixture()
def initiate_data_catalog_attrs():
    DataCatalog._attributes = {
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


@pytest.fixture()
def clear_all_aliases():
    AliasStorage.all_aliases.clear()


@pytest.fixture()
def clear_all_attributes():
    Attribute.all_attributes.clear()


@pytest.fixture()
def get_simple_filter_record():
    return {
            "operator": ">",
            "field": "patient.age",
            "value": 5
        }


@pytest.fixture()
def get_boolean_filter_record():
    return {
            "operator": "AND",
            "values": [
                {
                    "operator": "<",
                    "field": "patient.age",
                    "value": 35
                },
                {
                    "field": "patient.appointment.at",
                    "operator": ">",
                    "value": "2022-08-01",
                }
            ]
        }
