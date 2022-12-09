import pytest

from datetime import date
from unittest import mock
from unittest.mock import patch, Mock

from query_compiler.schemas.filter import SimpleFilter, BooleanFilter, Filter
from query_compiler.errors.schemas_errors import (
    FilterValueCastError, FilterConvertError
)


def test_boolean_filter_get_positive(
        initiate_data_catalog_attrs,
        get_boolean_filter_record
):
    expected_filter = BooleanFilter(get_boolean_filter_record)
    actual_filter = Filter.get(get_boolean_filter_record)
    assert actual_filter == expected_filter


def test_simple_filter_get_positive(
        initiate_data_catalog_attrs,
        get_simple_filter_record
):
    expected_filter = SimpleFilter(get_simple_filter_record)
    actual_filter = Filter.get(get_simple_filter_record)
    assert actual_filter == expected_filter


def test_filter_get_negative():
    with pytest.raises(FilterConvertError):
        Filter.get({})


@patch('query_compiler.schemas.filter.DataCatalog.get_type')
def test_get_attr_type_name_field(mock_get_type: Mock, get_simple_filter_record):
    expected_type = 'int'
    mock_get_type.return_value = expected_type
    simple_filter = SimpleFilter(get_simple_filter_record)
    actual_type = simple_filter._get_attr_type_name()
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
def test_get_type_name_having_count_avg(
        get_filter_having_with_all_aggregate_funcs,
        key,
        expected_type
):
    simple_filter = SimpleFilter(
        get_filter_having_with_all_aggregate_funcs[key]
    )
    actual_type = simple_filter._get_attr_type_name()
    assert actual_type == expected_type


@pytest.mark.parametrize('key', ('sum', 'min', 'max'))
@patch('query_compiler.schemas.filter.DataCatalog.get_type')
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
    actual_type = simple_filter._get_attr_type_name()
    assert expected_type == actual_type
    filter_alias = simple_filter.attr
    assert mock_get_type.call_args_list == [
        mock.call(filter_alias.attr.field.id),
        mock.call(filter_alias.attr.field.id)
    ]


@pytest.mark.parametrize(
    'class_, type_',
    (
            (int, 'int'),
            (float, 'float'),
            (str, 'string',),
            (bool, 'bool'),
            (date, 'date')
    ),
)
@patch('query_compiler.schemas.filter.SimpleFilter._get_attr_type_name')
def test_simple_filter_value_setter_positive(
        mock_get_attr_type_name: Mock,
        get_simple_filters_with_fields_of_all_types,
        class_,
        type_
):
    mock_get_attr_type_name.return_value = type_
    simple_filter = SimpleFilter(
        get_simple_filters_with_fields_of_all_types[type_]
    )
    assert type(simple_filter.value) == class_
    assert mock_get_attr_type_name.call_count == 1


@pytest.mark.parametrize(
    'wrong_type, actual_type',
    (
            ('date', 'int'),
            ('date', 'float'),
            ('int', 'string'),
            ('date', 'bool'),
            ('int', 'date')
    ),
    ids=(
        'INT TO DATE',
        'FLOAT TO DATE',
        'STRING TO INT',
        'BOOL TO DATE',
        'DATE TO INT'
    )
)
@patch('query_compiler.schemas.filter.SimpleFilter._get_attr_type_name')
def test_simple_filter_value_setter_cast_error(
        mock_get_type_name: Mock,
        get_simple_filters_with_fields_of_all_types,
        wrong_type,
        actual_type
):
    mock_get_type_name.return_value = wrong_type
    with pytest.raises(FilterValueCastError):
        SimpleFilter(get_simple_filters_with_fields_of_all_types[actual_type])
