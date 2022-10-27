import pytest

from datetime import date
from unittest import mock
from unittest.mock import patch, Mock

from query_compiler.schemas.attribute import *
from query_compiler.schemas.filter import SimpleFilter, BooleanFilter, Filter
from query_compiler.errors.schemas_errors import FilterValueCastError, \
    FilterConvertError


def test_boolean_filter_get_positive(
        clear_all_attributes,
        initiate_data_catalog_attrs,
        get_boolean_filter_record
):
    expected_filter = BooleanFilter(get_boolean_filter_record)
    actual_filter = Filter.get(get_boolean_filter_record)
    expected_all_attributes = {
        simple_filter.attr for simple_filter in expected_filter.values
    }
    assert actual_filter == expected_filter
    assert Attribute.all_attributes == expected_all_attributes


def test_simple_filter_get_positive(
        clear_all_attributes,
        initiate_data_catalog_attrs,
        get_simple_filter_record
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


@patch('query_compiler.schemas.data_catalog.DataCatalog.get_type')
def test_get_type_name_field(mock_get_type: Mock, get_simple_filter_record):
    expected_type = 'int'
    mock_get_type.return_value = expected_type
    simple_filter = SimpleFilter(get_simple_filter_record)
    actual_type = simple_filter._get_type_name()
    assert expected_type == actual_type
    assert mock_get_type.call_args_list == [
        mock.call(simple_filter.attr.id), mock.call(simple_filter.attr.id)
    ]


@pytest.mark.parametrize(
    'key, expected_type',
    (
            ('count', 'int'),
            ('avg', 'float')
    ),
)
@patch('query_compiler.schemas.data_catalog.DataCatalog.get_type')
def test_get_type_name_having_count_avg(
        mock_get_type: Mock,
        get_filter_having_with_all_aggregate_funcs,
        key,
        expected_type
):
    mock_get_type.return_value = expected_type
    simple_filter = SimpleFilter(
        get_filter_having_with_all_aggregate_funcs[key]
    )
    actual_type = simple_filter._get_type_name()
    assert actual_type == expected_type
    assert mock_get_type.call_count == 0


@pytest.mark.parametrize('key', ('sum', 'min', 'max'))
@patch('query_compiler.schemas.data_catalog.DataCatalog.get_type')
def test_get_type_name_having_positive(
        mock_get_type: Mock,
        get_filter_having_with_all_aggregate_funcs,
        key,
):
    expected_type = 'int'
    mock_get_type.return_value = expected_type
    simple_filter = SimpleFilter(
        get_filter_having_with_all_aggregate_funcs[key]
    )
    actual_type = simple_filter._get_type_name()
    assert expected_type == actual_type
    filter_alias = simple_filter.attr
    assert mock_get_type.call_args_list == [
        mock.call(filter_alias.attr.field.id),
        mock.call(filter_alias.attr.field.id)
    ]


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
    assert mock_get_type_name.call_count == 1


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
