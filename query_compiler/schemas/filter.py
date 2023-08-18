import logging

from abc import ABC
from datetime import date, datetime
from typing import Any

from psycopg import sql

from query_compiler.configs.settings import settings
from query_compiler.schemas.attribute import Aggregate, Alias
from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.errors.schemas_errors import (
    FilterConvertError, FilterValueCastError, UnknownAggregationFunctionError,
    UnknownOperatorFunctionError
)

LOG = logging.getLogger(__name__)


class Filter(ABC):
    @classmethod
    def get(cls, record):
        for class_ in (BooleanFilter, SimpleFilter):
            try:
                return class_(record)
            except (KeyError, ValueError):
                LOG.info(f"Record {record} couldn't be converted to {class_.__name__}")
        raise FilterConvertError(record)


class BooleanFilter(Filter):
    def __init__(self, record):
        self.operator = record['operator']
        if self.operator.lower() not in ('and', 'or', 'not'):
            raise ValueError()
        self.values = [Filter.get(r) for r in record['values']]

    def __eq__(self, other):
        return self.operator == other.operator and self.values == other.values


class SimpleFilter(Filter):
    _type_names_to_types = {
        'int': lambda val: val if isinstance(val, int) else int(val),
        'float': lambda val: val if isinstance(val, float) else float(val),
        'str': lambda val: val if isinstance(val, str) else str(val),
        'bool': lambda val: val if isinstance(val, bool) else bool(val),
        'date': lambda val: val if isinstance(val, date) else datetime.strptime(val, '%Y-%m-%d').date(),
        'datetime': lambda val: val if isinstance(val, datetime) else datetime.strptime(val, '%Y-%m-%d %H:%M:%S'),
        'tuple': lambda val: SimpleFilter._convert_to_tuple(val)
    }

    @staticmethod
    def _convert_to_tuple(value: str) -> tuple[Any, ...]:
        value = value.strip()
        split_value = value[1:-1].split(',')
        return tuple((val.strip() for val in split_value))

    def __init__(self, record):
        self.operator = record['operator']
        self.attr = Alias(record)
        self.value = record['value']

    def __eq__(self, other):
        return self.operator == other.operator and self.attr == other.attr and self.value == other.value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        if self.operator == 'is null':
            self._value = None
        else:
            attr_type_name = self._get_attr_type_name()
            try:
                self._value = self._type_names_to_types.get(attr_type_name)(value)
                if attr_type_name != 'tuple':
                    self._value = sql.quote(self._value)
                match self.operator:
                    case 'in':
                        self._value = f"({','.join(map(sql.quote, self._value))})"
                    case 'between':
                        left, right = self._value
                        self._value = f"{sql.quote(left)} and {sql.quote(right)}"
                    case 'like':
                        if not isinstance(self._value, str):
                            raise TypeError
            except (TypeError, ValueError) as exc:
                raise FilterValueCastError(attr_type_name, value) from exc

    @property
    def operator(self):
        return self._operator

    @operator.setter
    def operator(self, operator: str):
        if operator.lower() not in settings.pg_operator_functions:
            raise UnknownOperatorFunctionError(operator)
        else:
            self._operator = operator

    def _get_attr_type_name(self) -> str:
        attr = self.attr.attr
        if self.operator in ('between', 'in'):
            type_name = 'tuple'
        elif isinstance(attr, Aggregate):
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
            elif attr.func in ('sum', 'min', 'max'):
                type_name = DataCatalog.get_type(attr.field.id)
            else:
                raise UnknownAggregationFunctionError(attr.func)
        else:
            """self.attr is a an instance of a class Field"""
            type_name = DataCatalog.get_type(self.attr.id)
        return type_name
