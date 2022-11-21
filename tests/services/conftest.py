import pytest

from query_compiler.schemas.attribute import Alias, Attribute, Aggregate, Field
from query_compiler.schemas.filter import SimpleFilter, BooleanFilter
from query_compiler.schemas.table import Relation, Table


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
def add_appointments_alias():
    Alias.all_aliases['appointments'] = Attribute.get(
        {
            "aggregate": {
                "function": "count",
                "field": "patient.appointment",
            }
        }
    )


@pytest.fixture()
def get_having_simple_record(add_appointments_alias):
    return {
        "having": {
            "operator": ">",
            "alias": "appointments",
            "value": 5
        }
    }


@pytest.fixture()
def get_existing_and_not_existing_attrs():
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
def get_aggregate_and_pg_attr():
    return {
        'aggregate': Aggregate(
            {
                "aggregate": {
                    "function": "count",
                    "field": "patient.appointment",
                }
            }
        ),
        'pg_attribute': 'count(appointment.id)'
    }


@pytest.fixture()
def get_field_and_pg_attr():
    return {
        'field': Field({'field': 'patient.age'}),
        'pg_attribute': 'patient.age'
    }


@pytest.fixture()
def get_simple_filter_and_pg_filter(get_simple_filter_record):
    return {
        'filter': SimpleFilter(get_simple_filter_record),
        'pg_filter': 'patient.age > 5'
    }


@pytest.fixture()
def get_boolean_filter_and_pg_filter(get_boolean_filter_record):
    return {
        'filter': BooleanFilter(get_boolean_filter_record),
        'pg_filter': '(patient.age < 35) AND (appointment.age > 2022-08-01)'
    }


@pytest.fixture()
def get_attrs_and_sql_statement(add_appointments_alias):
    return {
        'attrs': (
            Field({"field": "patient.age"}),
            Alias({"alias": "appointments"})
        ),
        'sql_attr_part': 'patient.age, count(appointment.id)'
    }


@pytest.fixture()
def get_relations():
    return (
        Relation(
            'dv_raw.case_doctor_link',
            {
                'table': 'dv_raw.case_hub',
                'on': ('_hash_key', 'idcase_hash_fkey')
            }
        ),
        Relation(
            'dv_raw.doctor_person_link',
            {
                'table': 'dv_raw.case_doctor_link',
                'on': ('iddoctor_hash_fkey', 'iddoctor_hash_fkey')
            }
        ),
        Relation(
            'dv_raw.person_sat',
            {
                'table': 'dv_raw.doctor_person_link',
                'on': ('idperson_hash_fkey', '_hash_fkey')
            }
        ),
        Relation(
            'dv_raw.person_name_sat',
            {
                'table': 'dv_raw.doctor_person_link',
                'on': ('idperson_hash_fkey', '_hash_fkey')
            }
        ),
        Relation(
            'dv_raw.case_sat',
            {
                'table': 'dv_raw.case_hub',
                'on': ('_hash_key', '_hash_fkey')
            }
        ),
    )


@pytest.fixture()
def fill_attributes():
    for field in (
        {"field": "case.biz_key"},
        {"field": "case.sat.open_date"},
        {"field": "case.doctor.person.name_sat.family_name"},
        {"field": "case.doctor.person.sat.birth_date"},
    ):
        Field(field)


@pytest.fixture()
def get_from_clause_and_root_table(fill_attributes):
    root_table = Table({'name': 'dv_raw.case_hub'})
    join_statement = 'join dv_raw.case_doctor_link on ' \
                  'dv_raw.case_hub._hash_key = dv_raw.case_doctor_link.idcase_hash_fkey, ' \
                  'join dv_raw.doctor_person_link on ' \
                  'dv_raw.case_doctor_link.iddoctor_hash_fkey = dv_raw.doctor_person_link.iddoctor_hash_fkey, ' \
                  'join dv_raw.person_sat on ' \
                  'dv_raw.doctor_person_link.idperson_hash_fkey = dv_raw.person_sat._hash_fkey, ' \
                  'join dv_raw.person_name_sat on ' \
                  'dv_raw.doctor_person_link.idperson_hash_fkey = dv_raw.person_name_sat._hash_fkey, ' \
                  'join dv_raw.case_sat on ' \
                  'dv_raw.case_hub._hash_key = dv_raw.case_sat._hash_fkey'
    return {
        'root_table': root_table,
        'from_clause': f"from {root_table.name} {join_statement}"
    }


@pytest.fixture()
def get_filter_and_sql_statement(
        get_simple_filter_and_pg_filter, get_boolean_filter_and_pg_filter,
        add_appointments_alias
):
    return {
        'simple_filter': get_simple_filter_and_pg_filter['filter'],
        'boolean_filter': get_boolean_filter_and_pg_filter['filter'],
        'where': f"where {get_simple_filter_and_pg_filter['pg_filter']}",
        'having': f"having {get_boolean_filter_and_pg_filter['pg_filter']}"
    }
