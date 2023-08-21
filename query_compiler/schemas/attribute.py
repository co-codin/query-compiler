import logging

from abc import ABC, abstractmethod

from query_compiler.configs.settings import settings
from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.errors.schemas_errors import AttributeConvertError, UnknownAggregationFunctionError

LOG = logging.getLogger(__name__)


class Attribute(ABC):
    @classmethod
    def get(cls, record):
        for class_ in (Field, Aggregate):
            try:
                return class_(record)
            except KeyError:
                LOG.info(f"Record {record} couldn't be converted to {class_.__name__}")
        raise AttributeConvertError(record)

    @property
    @abstractmethod
    def table(self):
        ...

    @property
    @abstractmethod
    def field(self):
        ...

    @property
    @abstractmethod
    def display(self):
        ...


class Field(Attribute):
    def __init__(self, record):
        self._field = record['attr']['db_link']
        self._display = record['attr'].get('display', None)

    def __hash__(self):
        return hash(self._field)

    def __eq__(self, other):
        return self._field == other.field

    @property
    def table(self):
        return DataCatalog.get_table(self._field)

    @property
    def field(self):
        return self._field

    @property
    def display(self):
        return self._display


class Aggregate(Attribute):
    def __init__(self, record):
        self.func = record['aggregate']['function']
        self._field = Field(
            {
                'attr': {
                    'db_link': record['aggregate']['db_link']
                }
            }
        )
        self._display = record['aggregate']['display']

    def __hash__(self):
        return hash((self.func, self._field))

    def __eq__(self, other):
        return (self.func, self._field) == (other.func, other.field)

    @property
    def table(self):
        return self._field.table

    @property
    def func(self):
        return self._func

    @func.setter
    def func(self, func):
        if func in settings.pg_aggregation_functions:
            self._func = func
        else:
            raise UnknownAggregationFunctionError(func)

    @property
    def field(self):
        return self._field.field

    @property
    def display(self):
        return self._display


class AliasStorage:
    all_aliases = dict()

    @classmethod
    def clear(cls):
        cls.all_aliases.clear()
