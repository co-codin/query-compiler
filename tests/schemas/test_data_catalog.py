import pytest

from query_compiler.schemas.data_catalog import DataCatalog


@pytest.mark.parametrize(
    'field_name, is_in_attributes',
    (
        ('attribute1', True),
        ('attribute2', False)
    )
)
def test_is_field_in_attributes_dict(field_name, is_in_attributes):
    DataCatalog._attributes = {'attribute1': {}}
    actual_output = DataCatalog.is_field_in_attributes_dict(field_name)
    assert is_in_attributes == actual_output
