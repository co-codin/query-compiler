import logging

from abc import ABC

from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.errors.schemas_errors import AttributeConvertError


class Attribute(ABC):
    all_attributes = set()
    _logger: logging.Logger

    @property
    def attr(self):
        return self

    @classmethod
    def get(cls, record):
        cls._logger = logging.getLogger(__name__)
        for c in (Alias, Field, Aggregate):
            try:
                attr = c(record)
                cls.all_attributes.add(attr)
                return attr
            except KeyError:
                cls._logger.warning(
                    f"Record {record} couldn't be converted to {c}"
                )
        raise AttributeConvertError(record)


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
        return self.all_aliases[self.alias]

    @property
    def table(self):
        return self.attr.table

    @property
    def id(self):
        return self.attr.id


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
