import pytest
import json

from query_compiler.schemas.attribute import Alias, Attribute


@pytest.fixture()
def get_sample_query_json():
    return json.dumps({
        "attributes": [
            {
                "field": "patient.age",
            },
            {
                "alias": "appointments"
            }
        ],
        "aliases": {
            "appointments": {
                "aggregate": {
                    "function": "count",
                    "field": "patient.appointment",
                }
            },
            "appointment_time": {"field": "patient.appointment.at"}
        },
        "group": [
            {"field": "patient.appointment", }
        ],
        "filter": {
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
        },
        "having": {
            "operator": ">",
            "alias": "appointments",
            "value": 5
        },
    })


@pytest.fixture()
def get_sample_query_dict():
    return {
        "attributes": [
            {
                "field": "patient.age",
            },
            {
                "alias": "appointments"
            }
        ],
        "aliases": {
            "appointments": {
                "aggregate": {
                    "function": "count",
                    "field": "patient.appointment",
                }
            },
            "appointment_time": {"field": "patient.appointment.at"}
        },
        "group": [
            {"field": "patient.appointment", }
        ],
        "filter": {
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
        },
        "having": {
            "operator": ">",
            "alias": "appointments",
            "value": 5
        },
    }


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
