import pytest

from query_compiler.schemas.attribute import *


def test_attribute_get_positive(
        clear_all_attributes,
        get_field_alias_aggregate_records,
):
    expected_attrs = [
        class_(attr)
        for class_, attr in zip(
            (Field, Alias, Aggregate),
            get_field_alias_aggregate_records
        )
    ]
    actual_attrs = [
        Attribute.get(attr)
        for attr in get_field_alias_aggregate_records
    ]
    assert expected_attrs == actual_attrs
    expected_attrs.append(expected_attrs[-1].field)
    assert Attribute.all_attributes == set(expected_attrs)


def test_attribute_get_negative(clear_all_attributes):
    with pytest.raises(AttributeConvertError):
        Attribute.get({})
    assert len(Attribute.all_attributes) == 0
