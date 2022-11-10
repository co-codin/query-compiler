import json
import pytest

from unittest.mock import patch, Mock

from query_compiler.schemas.filter import BooleanFilter, SimpleFilter
from query_compiler.schemas.sample_query import SAMPLE_QUERY_GRAPH
from query_compiler.schemas.table import Relation
from query_compiler.services.query_parse import _parse_aliases, \
    _parse_attributes, _parse_filter, _get_missing_attribute_names, \
    _load_missing_attribute_data, _build_join_hierarchy, _get_pg_attribute, \
    _get_pg_filter
from query_compiler.errors.schemas_errors import NoAttributesInInputQuery
from query_compiler.schemas.attribute import *


def test_parse_aliases_positive(
        clear_all_aliases, clear_all_attributes, get_aliases_record
):
    expected_aliases = {
        'appointments': Aggregate(
            get_aliases_record['aliases']['appointments']
        ),
        'appointment_time': Field(
            get_aliases_record['aliases']['appointment_time']
        )
    }
    expected_all_attributes = {
        expected_aliases['appointments'],
        expected_aliases['appointment_time'],
        expected_aliases['appointments'].field
    }
    _parse_aliases(get_aliases_record)
    assert expected_aliases == Alias.all_aliases
    assert Attribute.all_attributes == expected_all_attributes


def test_parse_aliases_no_aliases(clear_all_aliases, clear_all_attributes):
    _parse_aliases({})
    assert len(Alias.all_aliases) == 0
    assert len(Attribute.all_attributes) == 0


@pytest.mark.parametrize(
    'key, classes',
    (
        ('attributes', (Field, Alias)),
        ('group', (Field,))
    )
)
def test_parse_attributes_positive(
        clear_all_attributes, get_attributes_record, key, classes
):
    actual_attributes = _parse_attributes(get_attributes_record, key=key)
    expected_attributes = []
    for attr_record, class_ in zip(get_attributes_record[key], classes):
        expected_attributes.append(class_(attr_record))
    assert expected_attributes == actual_attributes
    assert Attribute.all_attributes == set(expected_attributes)


def test_parse_attributes_no_attributes(clear_all_attributes):
    with pytest.raises(NoAttributesInInputQuery):
        _parse_attributes({}, key='attributes')
    assert len(Attribute.all_attributes) == 0


def test_parse_attributes_no_group(clear_all_attributes):
    _parse_attributes({}, key='group')
    assert len(Attribute.all_attributes) == 0


def test_parse_filter_positive(
        clear_all_attributes,
        initiate_data_catalog_attrs,
        get_boolean_filter_record
):
    record = {
        'filter': get_boolean_filter_record
    }
    actual_filter = _parse_filter(record, key='filter')
    expected_filter = BooleanFilter(record['filter'])
    expected_attributes = {
        simple_filter.attr for simple_filter in expected_filter.values
    }
    assert actual_filter == expected_filter
    assert Attribute.all_attributes == expected_attributes


def test_parse_having_positive(
        add_appointments_alias,
        initiate_data_catalog_attrs,
        get_having_simple_record
):
    actual_filter = _parse_filter(get_having_simple_record, key='having')
    expected_filter = SimpleFilter(get_having_simple_record['having'])
    expected_alias_attr = expected_filter.attr
    expected_attributes = {
        expected_alias_attr,
        expected_alias_attr.attr,
        expected_alias_attr.attr.field
    }
    assert actual_filter == expected_filter
    assert Attribute.all_attributes == expected_attributes
    assert Alias.all_aliases == {
        expected_alias_attr.alias: expected_alias_attr.attr
    }


@pytest.mark.parametrize('key', ('filter', 'having'))
def test_parse_filter_no_filter(clear_all_aliases, clear_all_attributes, key):
    _parse_filter({}, key=key)
    assert len(Alias.all_aliases) == 0
    assert len(Attribute.all_attributes) == 0


def test_get_missing_attribute_names(
        get_all_attributes, add_not_missing_attrs_to_data_catalog
):
    actual_missing_attrs = _get_missing_attribute_names()
    expected_missing_attrs = {
        'missing_field', 'missing_field_from_aggregate'

    }
    assert set(actual_missing_attrs) == expected_missing_attrs


@patch(
    'query_compiler.schemas.data_catalog.DataCatalog.load_missing_attr_data'
)
@patch(
    'query_compiler.schemas.data_catalog.DataCatalog'
    '.load_missing_attr_data_list'
)
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


@patch(
    'query_compiler.schemas.data_catalog.DataCatalog.load_missing_attr_data'
)
@patch(
    'query_compiler.schemas.data_catalog.DataCatalog'
    '.load_missing_attr_data_list'
)
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


@patch(
    'query_compiler.schemas.data_catalog.DataCatalog.load_missing_attr_data'
)
@patch(
    'query_compiler.schemas.data_catalog.DataCatalog'
    '.load_missing_attr_data_list'
)
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


def test_build_join_hierarchy(clear_all_attributes, initiate_data_catalog_attrs):
    for field in json.loads(SAMPLE_QUERY_GRAPH)['attributes']:
        Attribute.get(field)
    root_table, relations = _build_join_hierarchy()
    expected_relations = (
        Relation(
            'dv_raw.case_sat',
            {
                'table': 'dv_raw.case_hub',
                'on': ('_hash_key', '_hash_fkey')
            }
        ),
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
            'dv_raw.person_name_sat',
            {
                'table': 'dv_raw.doctor_person_link',
                'on': ('idperson_hash_fkey', '_hash_fkey')
            }
        ),
        Relation(
            'dv_raw.person_sat',
            {
                'table': 'dv_raw.doctor_person_link',
                'on': ('idperson_hash_fkey', '_hash_fkey')
            }
        ),
    )
    assert root_table == 'dv_raw.case_hub'
    assert set(expected_relations) == set(relations)


def test_get_pg_attribute_aggregate(
        get_aggregate, initiate_data_catalog_attrs
):
    actual_pg_attribute = _get_pg_attribute(get_aggregate)
    table = get_aggregate.table
    db_name = DataCatalog.get_field(get_aggregate.field.id)
    expected_pg_attribute = f'{get_aggregate.func}({table.name}.{db_name})'
    assert actual_pg_attribute == expected_pg_attribute


def test_get_pg_attribute_field(
        get_field, initiate_data_catalog_attrs
):
    actual_pg_attribute = _get_pg_attribute(get_field)
    table = get_field.table
    db_name = DataCatalog.get_field(get_field.id)
    expected_pg_attribute = f'{table.name}.{db_name}'
    assert actual_pg_attribute == expected_pg_attribute


def test_get_pg_filter_simple(
        get_simple_filter_record, initiate_data_catalog_attrs
):
    simple_filter = SimpleFilter(get_simple_filter_record)
    actual_pg_filter = _get_pg_filter(simple_filter)
    expected_pg_filter = f'{_get_pg_attribute(simple_filter.attr)} ' \
                         f'{simple_filter.operator} {simple_filter.value}'
    assert actual_pg_filter == expected_pg_filter


def test_get_pg_filter_boolean(
        get_boolean_filter_record, initiate_data_catalog_attrs
):
    boolean_filter = BooleanFilter(get_boolean_filter_record)
    actual_pg_filter = _get_pg_filter(boolean_filter)
    parts = (f'({_get_pg_filter(part)})' for part in boolean_filter.values)
    expected_pg_filter = f' {boolean_filter.operator} '.join(parts)
    assert actual_pg_filter == expected_pg_filter
