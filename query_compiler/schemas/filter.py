import logging

from abc import ABC
from datetime import date, datetime

from query_compiler.schemas.attribute import Attribute, Aggregate
from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.errors.schemas_errors import FilterConvertError, \
    FilterValueCastError

logger = logging.getLogger(__name__)


class Filter(ABC):
    @classmethod
    def get(cls, record):
        for class_ in (BooleanFilter, SimpleFilter):
            try:
                return class_(record)
            except ValueError:
                logger.warning(
                    f"Record {record} couldn't be converted to {class_}"
                )
        raise FilterConvertError(record)


class BooleanFilter(Filter):
    def __init__(self, record):
        self.operator = record['operator']
        if self.operator.lower() not in ('and', 'or'):
            raise ValueError()
        self.values = [Filter.get(r) for r in record['values']]


class SimpleFilter(Filter):
    _type_names_to_types = {
        'int': lambda val: val if isinstance(val, int) else int(val),
        'float': lambda val: val if isinstance(val, float) else float(val),
        'str': lambda val: val if isinstance(val, str) else str(val),
        'bool': lambda val: val if isinstance(val, bool) else bool(val),
        'date': lambda val:
        val if isinstance(val, date)
        else datetime.strptime(val, '%Y-%m-%d').date()
    }

    def __init__(self, record):
        self.operator = record['operator']
        self.attr = Attribute.get(record)
        self.value = record['value']

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        type_name = self._get_type_name()
        try:
            self._value = self._type_names_to_types[type_name](value)
        except (TypeError, ValueError) as exc:
            raise FilterValueCastError(type_name, value) from exc

    def _get_type_name(self) -> str:
        attr = self.attr.attr
        if isinstance(attr, Aggregate):
            """
            Check for an aggregate function
            if it's count then type_name is int
            if it's avg then type_name is float
            if it's max, min, sum then type_name is field's type name
            """
            if attr.func == 'count':
                type_name = 'int'
            elif attr.func == 'avg':
                type_name = 'float'
            else:
                """sum, min or max case"""
                type_name = DataCatalog.get_type(attr.field.id)
        else:
            """self.attr is a an instance of a class Field"""
            type_name = DataCatalog.get_type(self.attr.id)
        return type_name
