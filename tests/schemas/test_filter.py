import pytest

from datetime import date
from unittest.mock import patch, Mock

from query_compiler.schemas.attribute import *
from query_compiler.schemas.filter import SimpleFilter, BooleanFilter, Filter
from query_compiler.errors.schemas_errors import FilterValueCastError, \
    FilterConvertError


def test_boolean_filter_get_positive(
        clear_all_attributes, get_boolean_filter_record
):
    expected_filter = BooleanFilter(get_boolean_filter_record)
    actual_filter = Filter.get(get_boolean_filter_record)
    expected_all_attributes = {
        simple_filter.attr for simple_filter in expected_filter.values
    }
    assert actual_filter == expected_filter
    assert Attribute.all_attributes == expected_all_attributes


def test_simple_filter_get_positive(
        clear_all_attributes, get_simple_filter_record
):
    expected_filter = SimpleFilter(get_simple_filter_record)
    actual_filter = Filter.get(get_simple_filter_record)
    expected_all_attributes = {expected_filter.attr}

    assert actual_filter == expected_filter
    assert Attribute.all_attributes == expected_all_attributes


def test_filter_get_negative(clear_all_attributes):
    with pytest.raises(FilterConvertError):
        Filter.get({})
    assert len(Attribute.all_attributes) == 0


@pytest.mark.parametrize(
    'key, type_, index',
    (
        ('filter', 'int', 0),
        ('having', 'int', 0),
        ('having', 'float', 1),
        ('having', 'int', 2),
        ('having', 'int', 3),
        ('having', 'int', 4),
    ),
    ids=(
            'filter NO_AVG',
            'having COUNT',
            'having AVG',
            'having SUM',
            'having MIN',
            'having MAX'
    )
)
@patch('query_compiler.schemas.data_catalog.DataCatalog.get_type')
def test_get_type_name_positive(
        mock_get_type: Mock,
        get_filter_having_with_all_aggregate_funcs,
        key,
        type_,
        index
):
    mock_get_type.return_value = type_
    simple_filter = SimpleFilter(
        get_filter_having_with_all_aggregate_funcs[key][index]
    )
    actual_type = simple_filter._get_type_name()
    assert type_ == actual_type


@pytest.mark.parametrize(
    'class_, type_, index',
    (
            (int, 'int', 0),
            (float, 'float', 1),
            (str, 'str', 2),
            (bool, 'bool', 3),
            (date, 'date', 4)
    ),
)
@patch('query_compiler.schemas.filter.SimpleFilter._get_type_name')
def test_simple_filter_value_setter_positive(
        mock_get_type_name: Mock,
        get_simple_filters_with_fields_of_all_types,
        class_,
        type_,
        index
):
    mock_get_type_name.return_value = type_
    simple_filter = SimpleFilter(
        get_simple_filters_with_fields_of_all_types[index]
    )
    assert type(simple_filter.value) == class_


@pytest.mark.parametrize(
    'type_, index',
    (
            ('date', 0),
            ('date', 1),
            ('int', 2),
            ('date', 3),
            ('int', 4)
    ),
    ids=(
        'INT TO DATE',
        'FLOAT TO DATE',
        'STRING TO INT',
        'BOOL TO DATE',
        'DATE TO INT'
    )
)
@patch('query_compiler.schemas.filter.SimpleFilter._get_type_name')
def test_simple_filter_value_setter_cast_error(
        mock_get_type_name: Mock,
        get_simple_filters_with_fields_of_all_types,
        type_,
        index
):
    mock_get_type_name.return_value = type_
    with pytest.raises(FilterValueCastError):
        SimpleFilter(get_simple_filters_with_fields_of_all_types[index])
