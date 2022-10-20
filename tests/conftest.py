import pytest
import json

from query_compiler.schemas.attribute import Alias, Attribute


@pytest.fixture()
def get_field_alias_aggregate_records():
    return (
        {
            "field": "patient.age",
        },
        {
            "alias": "appointments"
        },
        {
            "aggregate": {
                "function": "count",
                "field": "patient.appointment",
            }
        }
    )


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
def get_simple_filter_record():
    return {
            "operator": ">",
            "field": "patient.age",
            "value": 5
        }


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
def get_aliases_dict_value_records_with_all_aggr_funcs():
    return {
        "aliases": {
            "appointments_count": {
                "aggregate": {
                    "function": "count",
                    "field": "patient.appointment",
                }
            },
            "appointments_avg": {
                "aggregate": {
                    "function": "avg",
                    "field": "patient.appointment",
                }
            },
            "appointments_sum": {
                "aggregate": {
                    "function": "sum",
                    "field": "patient.appointment",
                }
            },
            "appointments_min": {
                "aggregate": {
                    "function": "min",
                    "field": "patient.appointment",
                }
            },
            "appointments_max": {
                "aggregate": {
                    "function": "max",
                    "field": "patient.appointment",
                }
            },
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
def get_filter_having_with_all_aggregate_funcs(
        add_aliases_with_all_aggr_funcs
):
    return {
        "filter": (
            {
                "operator": "<",
                "field": "patient.age",
                "value": 35
            },
        ),
        "having": (
            {
                "operator": ">",
                "alias": "appointments_count",
                "value": 5
            },
            {
                "operator": ">",
                "alias": "appointments_avg",
                "value": 5.
            },
            {
                "operator": ">",
                "alias": "appointments_sum",
                "value": 5
            },
            {
                "operator": ">",
                "alias": "appointments_min",
                "value": 5
            },
            {
                "operator": ">",
                "alias": "appointments_max",
                "value": 5
            },
        )
    }


@pytest.fixture()
def get_simple_filters_with_fields_of_all_types():
    return (
        {
            "operator": "<",
            "field": "patient.age",
            "value": '35'
        },

        {
            "operator": "<",
            "field": "patient.age",
            "value": '35.'
        },

        {
            "operator": "<",
            "field": "patient.age",
            "value": 'string'
        },

        {
            "operator": "<",
            "field": "patient.age",
            "value": 'True'
        },

        {
            "operator": "<",
            "field": "patient.age",
            "value": '2022-08-01'
        },
    )


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
def add_aliases_with_all_aggr_funcs(
        clear_all_aliases, get_aliases_dict_value_records_with_all_aggr_funcs
):
    for alias, record in get_aliases_dict_value_records_with_all_aggr_funcs[
        'aliases'
    ].items():
        Alias.all_aliases[alias] = Attribute.get(record)


@pytest.fixture()
def clear_all_aliases():
    Alias.all_aliases.clear()


@pytest.fixture()
def clear_all_attributes():
    Attribute.all_attributes.clear()
