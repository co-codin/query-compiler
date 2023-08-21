import logging

from abc import ABC
from typing import Any

from psycopg import sql

from query_compiler.configs.settings import settings
from query_compiler.schemas.attribute import AliasStorage
from query_compiler.errors.schemas_errors import (FilterConvertError, UnknownOperatorFunctionError)

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
    def __init__(self, record):
        self.operator = record['operator']
        self.attr = AliasStorage.all_aliases[record['alias']]
        self.value = record['value']

    def __eq__(self, other):
        return self.operator == other.operator and self.attr == other.attr and self.value == other.value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        match self.operator:
            case 'is null':
                self._value = None
            case 'in' | 'between':
                self._value = self._convert_to_tuple(value)
                self._value = (
                    f"({','.join(map(sql.quote, self._value))})" if self._operator == 'in'
                    else f"{sql.quote(self._value[0])} and {sql.quote(self._value[1])}"
                )
            case _:
                self._value = sql.quote(value)

    @property
    def operator(self):
        return self._operator

    @operator.setter
    def operator(self, operator: str):
        if operator.lower() not in settings.pg_operator_functions:
            raise UnknownOperatorFunctionError(operator)
        else:
            self._operator = operator

    @staticmethod
    def _convert_to_tuple(value: str) -> tuple[Any, ...]:
        value = value.strip()
        split_value = value[1:-1].split(',')
        return tuple((val.strip() for val in split_value))
