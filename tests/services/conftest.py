import pytest

from query_compiler.schemas.attribute import Alias, Attribute
from query_compiler.schemas.data_catalog import DataCatalog


@pytest.fixture()
def get_aliases_record():
    return {
        "aliases": {
            "appointments": {
                "aggregate": {
                    "function": "count",
                    "field": "patient.appointment",
                },
            },
            "appointment_time": {"field": "patient.appointment.at"}
        },
    }


@pytest.fixture()
def get_attributes_record():
    return {
        "attributes": (
            {
                "field": "patient.age",
            },
            {
                "alias": "appointments"
            }
        ),
        "group": (
            {
                "field": "patient.appointment",
            },
        ),
    }


@pytest.fixture()
def add_appointments_alias(clear_all_aliases, clear_all_attributes):
    Alias.all_aliases['appointments'] = Attribute.get(
        {
            "aggregate": {
                "function": "count",
                "field": "patient.appointment",
            }
        }
    )


@pytest.fixture()
def get_having_simple_record():
    return {
        "having": {
            "operator": ">",
            "alias": "appointments",
            "value": 5
        }
    }


@pytest.fixture()
def get_all_attributes_record():
    return (
        {'field': 'not_missing_field'},
        {'field': 'missing_field'},
        {'alias': 'not_missing_alias'},
        {'alias': 'missing_alias'},
        {
            'aggregate': {
                'function': 'count',
                'field': 'not_missing_field_from_aggregate'
            }
        },
        {
            'aggregate': {
                'function': 'count', 'field': 'missing_field_from_aggregate'
            }
        }
    )


@pytest.fixture()
def add_not_missing_attrs_to_data_catalog():
    DataCatalog._attributes = {
        'not_missing_field': {},
        'not_missing_alias': {},
        'not_missing_field_from_aggregate': {}
    }


@pytest.fixture()
def get_all_attributes(clear_all_attributes, get_all_attributes_record):
    for attr in get_all_attributes_record:
        Attribute.all_attributes.add(Attribute.get(attr))
