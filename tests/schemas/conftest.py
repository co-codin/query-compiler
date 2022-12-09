import pytest

from query_compiler.schemas.attribute import Alias, Attribute


@pytest.fixture()
def get_filter_having_with_all_aggregate_funcs(
        add_aliases_with_all_aggr_funcs
):
    return {
        'count': {
                "operator": ">",
                "alias": "appointments_count",
                "value": 5
            },
        'avg': {
                "operator": ">",
                "alias": "appointments_avg",
                "value": 5.
            },
        'sum': {
                "operator": ">",
                "alias": "appointments_sum",
                "value": 5
            },
        'min': {
                "operator": ">",
                "alias": "appointments_min",
                "value": 5
            },
        'max': {
                "operator": ">",
                "alias": "appointments_max",
                "value": 5
            },
    }


@pytest.fixture()
def add_aliases_with_all_aggr_funcs(
        clear_all_aliases, get_aliases_dict_value_records_with_all_aggr_funcs
):
    for alias, record in get_aliases_dict_value_records_with_all_aggr_funcs[
        'aliases'
    ].items():
        Alias.all_aliases[alias] = Attribute.get(record)


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
def get_simple_filters_with_fields_of_all_types():
    return {
        'int': {
            "operator": "<",
            "field": "patient.age",
            "value": '35'
        },
        'float': {
            "operator": "<",
            "field": "patient.age",
            "value": '35.'
        },
        'string': {
            "operator": "<",
            "field": "patient.age",
            "value": 'string'
        },
        'bool': {
            "operator": "<",
            "field": "patient.age",
            "value": 'True'
        },
        'date': {
            "operator": "<",
            "field": "patient.age",
            "value": '2022-08-01'
        }
    }


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
