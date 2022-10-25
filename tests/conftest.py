import pytest

from query_compiler.schemas.attribute import Alias, Attribute


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
