import pytest

from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.schemas.attribute import Alias, Attribute


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
        }
    }


@pytest.fixture()
def clear_all_aliases():
    Alias.all_aliases.clear()


@pytest.fixture()
def clear_all_attributes():
    Attribute.all_attributes.clear()


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
