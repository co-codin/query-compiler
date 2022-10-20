import pytest

from query_compiler.schemas.filter import BooleanFilter, SimpleFilter
from query_compiler.services.query_parse import _from_json_to_dict, \
    _parse_aliases, _parse_attributes, _parse_filter
from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError
from query_compiler.errors.schemas_errors import NoAttributesInInputQuery
from query_compiler.schemas.attribute import *


def test_from_json_to_dict_positive(
        get_sample_query_json,
        get_sample_query_dict
):
    actual_dict_query = _from_json_to_dict(get_sample_query_json)
    assert actual_dict_query == get_sample_query_dict


@pytest.mark.parametrize(
    'json_query_in',
    (b'', b'rubbish', b'[rubbish]' b'(rubbish)', b'10')
)
def test_from_json_to_dict_json_decode_error(
        get_sample_query_json, json_query_in
):
    with pytest.raises(DeserializeJSONQueryError):
        _from_json_to_dict(json_query_in)


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
        clear_all_attributes, get_boolean_filter_record
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
        add_appointments_alias, get_having_simple_record
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
