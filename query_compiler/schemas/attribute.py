import logging

from abc import ABC

from query_compiler.configs.settings import settings
from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.errors.schemas_errors import (
    AttributeConvertError, UnknownAggregationFunctionError,
    NoAliasMappedValueError
)

LOG = logging.getLogger(__name__)


class Attribute(ABC):
    all_attributes = set()

    @property
    def attr(self):
        return self

    @classmethod
    def get(cls, record):
        for class_ in (Alias, Field, Aggregate):
            try:
                attr = class_(record)
                cls.all_attributes.add(attr)
                return attr
            except KeyError:
                LOG.info(
                    f"Record {record} couldn't be converted to "
                    f"{class_.__name__}"
                )
        raise AttributeConvertError(record)

    @classmethod
    def clear(cls):
        cls.all_attributes.clear()


class Field(Attribute):
    def __init__(self, record):
        self.field = record['field']

    def __hash__(self):
        return hash(self.field)

    def __eq__(self, other):
        return self.field == other.field

    @property
    def table(self):
        return DataCatalog.get_table(self.field)

    @property
    def id(self):
        return self.field


class Alias(Attribute):
    all_aliases = dict()

    def __init__(self, record):
        self.alias = record['alias']

    def __hash__(self):
        return hash(self.alias)

    def __eq__(self, other):
        return self.alias == other.alias

    @property
    def attr(self):
        try:
            return self.all_aliases[self.alias]
        except KeyError:
            raise NoAliasMappedValueError(self.alias)

    @property
    def table(self):
        return self.attr.table

    @property
    def id(self):
        return self.attr.id

    @classmethod
    def clear(cls):
        cls.all_aliases.clear()


class Aggregate(Attribute):
    def __init__(self, record):
        self.func = record['aggregate']['function']
        self.field = Attribute.get(record['aggregate'])

    def __hash__(self):
        return hash((self.func, self.field))

    def __eq__(self, other):
        return (self.func, self.field) == (other.func, other.field)

    @property
    def table(self):
        return self.field.attr.table

    @property
    def func(self):
        return self._func

    @func.setter
    def func(self, func):
        if func in settings.pg_aggregation_functions:
            self._func = func
        else:
            raise UnknownAggregationFunctionError(func)
