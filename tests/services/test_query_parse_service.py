import json
import pytest

from unittest.mock import patch, Mock

from query_compiler.schemas.filter import BooleanFilter, SimpleFilter
from query_compiler.schemas.sample_query import SAMPLE_QUERY_GRAPH
from query_compiler.schemas.table import Table
from query_compiler.services.query_parse import (
    _parse_aliases, _parse_attributes, _parse_filter,
    _get_missing_attribute_names, _load_missing_attribute_data,
    _build_join_hierarchy, _get_pg_attribute, _get_pg_filter,
    _build_attributes_clause, _build_from_clause, _build_filter_clause, clear,
    _piece_sql_statements_together
)
from query_compiler.errors.schemas_errors import NoAttributesInInputQuery
from query_compiler.errors.query_parse_errors import NoRootTable
from query_compiler.schemas.attribute import *


def test_parse_aliases_positive(clear_all_aliases, get_aliases_record):
    expected_aliases = {
        'appointments': Aggregate(
            get_aliases_record['aliases']['appointments']
        ),
        'appointment_time': Field(
            get_aliases_record['aliases']['appointment_time']
        )
    }
    _parse_aliases(get_aliases_record)
    assert expected_aliases == Alias.all_aliases


def test_parse_aliases_no_aliases(clear_all_aliases):
    assert _parse_aliases({}) is None
    assert len(Alias.all_aliases) == 0


@pytest.mark.parametrize(
    'key, classes',
    (
        ('attributes', (Field, Alias)),
        ('group', (Field,))
    )
)
def test_parse_attributes_positive(get_attributes_record, key, classes):
    actual_attributes = _parse_attributes(get_attributes_record, key=key)
    expected_attributes = []
    for attr_record, class_ in zip(get_attributes_record[key], classes):
        expected_attributes.append(class_(attr_record))
    assert expected_attributes == actual_attributes


def test_parse_attributes_no_attributes():
    with pytest.raises(NoAttributesInInputQuery):
        _parse_attributes({}, key='attributes')


def test_parse_attributes_no_group():
    assert _parse_attributes({}, key='group') is None


def test_parse_filter_positive(
        initiate_data_catalog_attrs,
        get_boolean_filter_record
):
    record = {'filter': get_boolean_filter_record}
    actual_filter = _parse_filter(record, key='filter')
    expected_filter = BooleanFilter(record['filter'])
    assert actual_filter == expected_filter


def test_parse_having_positive(
        initiate_data_catalog_attrs,
        get_having_simple_record
):
    actual_filter = _parse_filter(get_having_simple_record, key='having')
    expected_filter = SimpleFilter(get_having_simple_record['having'])
    assert actual_filter == expected_filter


@pytest.mark.parametrize('key', ('filter', 'having'))
def test_parse_filter_no_filter(key):
    assert _parse_filter({}, key=key) is None


def test_get_missing_attribute_names(
        clear_all_attributes, get_existing_and_not_existing_attrs
):
    for attr in get_existing_and_not_existing_attrs:
        Attribute.get(attr)
    DataCatalog._attributes = {
        'not_missing_field': {},
        'not_missing_alias': {},
        'not_missing_field_from_aggregate': {}
    }
    actual_missing_attrs = _get_missing_attribute_names()
    expected_missing_attrs = {
        'missing_field', 'missing_field_from_aggregate'

    }
    assert set(actual_missing_attrs) == expected_missing_attrs


@patch('query_compiler.services.query_parse.DataCatalog.load_missing_attr_data')
@patch('query_compiler.services.query_parse.DataCatalog.load_missing_attr_data_list')
@patch('query_compiler.services.query_parse._get_missing_attribute_names')
def test_load_missing_attribute_data_no_missing_attrs(
        mock_get_missing_attribute_names: Mock,
        mock_load_missing_attr_data_list: Mock,
        mock_load_missing_attr_data: Mock
):
    mock_get_missing_attribute_names.return_value = tuple()
    _load_missing_attribute_data()
    assert mock_load_missing_attr_data_list.call_count == 0
    assert mock_load_missing_attr_data.call_count == 0
    mock_get_missing_attribute_names.assert_called_once()


@patch('query_compiler.services.query_parse.DataCatalog.load_missing_attr_data')
@patch('query_compiler.services.query_parse.DataCatalog.load_missing_attr_data_list')
@patch('query_compiler.services.query_parse._get_missing_attribute_names')
def test_load_missing_attribute_data_one_missing_attr(
        mock_get_missing_attribute_names: Mock,
        mock_load_missing_attr_data_list: Mock,
        mock_load_missing_attr_data: Mock
):
    missing_attributes = ('missing_attribute1',)
    mock_get_missing_attribute_names.return_value = missing_attributes
    _load_missing_attribute_data()
    assert mock_load_missing_attr_data_list.call_count == 0
    mock_load_missing_attr_data.assert_called_once_with(missing_attributes[0])
    mock_get_missing_attribute_names.assert_called_once()


@patch('query_compiler.services.query_parse.DataCatalog.load_missing_attr_data')
@patch('query_compiler.services.query_parse.DataCatalog.load_missing_attr_data_list')
@patch('query_compiler.services.query_parse._get_missing_attribute_names')
def test_load_missing_attribute_data_many_missing_attrs(
        mock_get_missing_attribute_names: Mock,
        mock_load_missing_attr_data_list: Mock,
        mock_load_missing_attr_data: Mock
):
    missing_attributes = ('missing_attribute1', 'missing_attribute2')
    mock_get_missing_attribute_names.return_value = missing_attributes
    _load_missing_attribute_data()
    assert mock_load_missing_attr_data.call_count == 0
    mock_load_missing_attr_data_list.assert_called_once_with(
        missing_attributes
    )
    mock_get_missing_attribute_names.assert_called_once()


def test_build_join_hierarchy_positive(clear_all_attributes, get_relations, initiate_data_catalog_attrs):
    for field in json.loads(SAMPLE_QUERY_GRAPH)['attributes']:
        Attribute.get(field)
    root_table, relations = _build_join_hierarchy()
    expected_relations = get_relations
    assert root_table == Table({'name': 'dv_raw.case_hub'})
    assert set(expected_relations) == set(relations)


def test_build_join_hierarchy_no_root_table(clear_all_attributes):
    with pytest.raises(NoRootTable):
        _build_join_hierarchy()


def test_get_pg_attribute_aggregate(
        get_aggregate_and_pg_attr, initiate_data_catalog_attrs
):
    actual_pg_attribute = _get_pg_attribute(get_aggregate_and_pg_attr['aggregate'])
    expected_pg_attribute = get_aggregate_and_pg_attr['pg_attribute']
    assert actual_pg_attribute == expected_pg_attribute


def test_get_pg_attribute_field(
        get_field_and_pg_attr, initiate_data_catalog_attrs
):
    actual_pg_attribute = _get_pg_attribute(get_field_and_pg_attr['field'])
    expected_pg_attribute = get_field_and_pg_attr['pg_attribute']
    assert actual_pg_attribute == expected_pg_attribute


def test_get_pg_filter_simple(
        get_simple_filter_and_pg_filter, initiate_data_catalog_attrs
):
    actual_pg_filter = _get_pg_filter(get_simple_filter_and_pg_filter['filter'])
    expected_pg_filter = get_simple_filter_and_pg_filter['pg_filter']
    assert actual_pg_filter == expected_pg_filter


def test_get_pg_filter_boolean(
        get_boolean_filter_and_pg_filter, initiate_data_catalog_attrs
):
    actual_pg_filter = _get_pg_filter(get_boolean_filter_and_pg_filter['filter'])
    expected_pg_filter = get_boolean_filter_and_pg_filter['pg_filter']
    assert actual_pg_filter == expected_pg_filter


@pytest.mark.parametrize('key', ('select', 'group by'))
def test_build_attributes_clause(
        key, get_attrs_and_sql_statement, initiate_data_catalog_attrs
):
    actual_output = f"{key} {get_attrs_and_sql_statement['sql_attr_part']}"
    expected_output = _build_attributes_clause(get_attrs_and_sql_statement['attrs'], key=key)
    assert actual_output == expected_output


@pytest.mark.parametrize('key', ('select', 'group by'))
def test_build_attributes_clause_empty_attrs(key):
    assert _build_attributes_clause([], key=key) is None


def test_build_from_clause(get_from_clause_and_root_table, get_relations, initiate_data_catalog_attrs):
    expected_output = get_from_clause_and_root_table['from_clause']
    actual_output = _build_from_clause(get_from_clause_and_root_table['root_table'], get_relations)
    assert expected_output == actual_output


def test_build_from_clause_empty_relations():
    root_table = Table({'name': 'dv_raw.case_hub'})
    actual_output = _build_from_clause(root_table, [])
    expected_output = f'from {root_table.name} '
    assert actual_output == expected_output


@pytest.mark.parametrize(
    'filter_key, sql_key',
    (
            ('simple_filter', 'where'),
            ('boolean_filter', 'having')
    )
)
def test_build_filter_clause(
        filter_key, sql_key, get_filter_and_sql_statement,
        initiate_data_catalog_attrs
):
    expected_output = f"{get_filter_and_sql_statement[sql_key]}"
    actual_output = _build_filter_clause(get_filter_and_sql_statement[filter_key], key=sql_key)
    assert expected_output == actual_output


@pytest.mark.parametrize('key', ('where', 'having'))
def test_build_filter_clause_no_filter(key):
    assert _build_filter_clause(None, key) is None


def test_clear(get_aliases_record, get_attributes_record):
    _parse_aliases(get_aliases_record)
    _parse_attributes(get_attributes_record, key='attributes')
    _parse_attributes(get_attributes_record, key='group')
    clear()
    assert len(Alias.all_aliases) == 0
    assert len(Attribute.all_attributes) == 0


def test_piece_sql_statements_together_none_included():
    select = 'select dv_raw.case_hub._biz_key, dv_raw.case_sat.opendate dv_raw.person_name_sat.familyname, dv_raw.person_sat.birthdate'
    from_ = 'from dv_raw.case_hub ' \
            'join dv_raw.case_doctor_link ' \
                'on dv_raw.case_hub._hash_key = dv_raw.case_doctor_link.idcase_hash_fkey, ' \
            'join dv_raw.doctor_person_link ' \
                'on dv_raw.case_doctor_link.iddoctor_hash_fkey = dv_raw.doctor_person_link.iddoctor_hash_fkey, ' \
            'join dv_raw.person_name_sat ' \
                'on dv_raw.doctor_person_link.idperson_hash_fkey = dv_raw.person_name_sat._hash_fkey, ' \
            'join dv_raw.person_sat ' \
                'on dv_raw.doctor_person_link.idperson_hash_fkey = dv_raw.person_sat._hash_fkey, ' \
            'join dv_raw.case_sat ' \
                'on dv_raw.case_hub._hash_key = dv_raw.case_sat._hash_fkey'
    where = None
    group_by = None
    having = None

    expected_output = 'select dv_raw.case_hub._biz_key, dv_raw.case_sat.opendate dv_raw.person_name_sat.familyname, dv_raw.person_sat.birthdate ' \
                      'from dv_raw.case_hub ' \
                      'join dv_raw.case_doctor_link ' \
                        'on dv_raw.case_hub._hash_key = dv_raw.case_doctor_link.idcase_hash_fkey, ' \
                      'join dv_raw.doctor_person_link ' \
                        'on dv_raw.case_doctor_link.iddoctor_hash_fkey = dv_raw.doctor_person_link.iddoctor_hash_fkey, ' \
                      'join dv_raw.person_name_sat ' \
                        'on dv_raw.doctor_person_link.idperson_hash_fkey = dv_raw.person_name_sat._hash_fkey, ' \
                      'join dv_raw.person_sat ' \
                        'on dv_raw.doctor_person_link.idperson_hash_fkey = dv_raw.person_sat._hash_fkey, ' \
                      'join dv_raw.case_sat ' \
                        'on dv_raw.case_hub._hash_key = dv_raw.case_sat._hash_fkey'
    actual_output = _piece_sql_statements_together(select, from_, where, group_by, having)
    assert expected_output == actual_output


def test_piece_sql_statements_together_none_excluded():
    select = 'select patient.age, count(appointment.id)'
    from_ = 'from patient join appointment on patient.id = appointment.patient_id'
    where = 'where (patient.age < 35) AND (appointment.age > 2022-08-01)'
    group_by = 'group by appointment.id'
    having = 'having count(appointment.id) > 5'

    expected_output = 'select patient.age, count(appointment.id) ' \
                      'from patient join appointment on patient.id = appointment.patient_id ' \
                      'where (patient.age < 35) AND (appointment.age > 2022-08-01) ' \
                      'group by appointment.id ' \
                      'having count(appointment.id) > 5'
    actual_output = _piece_sql_statements_together(select, from_, where, group_by, having)
    assert expected_output == actual_output
