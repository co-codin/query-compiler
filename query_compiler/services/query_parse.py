import json
import logging

from typing import Dict, Tuple, Set, Generator, cast

from query_compiler.schemas.attribute import Attribute, Alias, Aggregate
from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.schemas.filter import Filter, SimpleFilter, BooleanFilter
from query_compiler.schemas.table import Table
from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError


def generate_sql_query(query: bytes) -> str:
    """Function for generating sql query from json query"""
    logger = logging.getLogger(__name__)
    logger.info("Starting generating SQL query from the json query")

    dict_query = _from_json_to_dict(query, logger)
    attributes, filter_, groups, having = _parse_query(dict_query, logger)
    root_table, tables = _build_join_hierarchy(logger)
    sql_query = _build_sql_query(
        attributes, root_table, tables, filter_, groups, having, logger
    )

    logger.info("Generating SQL query from the json query successfully "
                "completed"
                )
    return sql_query


def _from_json_to_dict(query: bytes, logger: logging.Logger) -> Dict:
    logger.info(f"Deserializing the input json query {query}")
    try:
        dict_query = json.loads(query)
    except json.JSONDecodeError as json_decode_err:
        raise DeserializeJSONQueryError(query) from json_decode_err
    else:
        logger.info("Query deserialization successfully completed")
        return dict_query


def _parse_query(query: Dict, logger: logging.Logger) -> Tuple[
    Generator[Attribute, None, None],
    Filter, Generator[Attribute, None, None],
    Filter
]:
    """Function for parsing json query and creating schemas objects"""
    logger.info(f"Starting parsing the following query {query}")

    for alias, record in query['aliases'].items():
        Alias.all_aliases[alias] = Attribute.get(record)

    attributes = (Attribute.get(record) for record in query['attributes'])

    filter_ = Filter.get(query['filter'])
    groups = (Attribute.get(record) for record in query['group'])
    having = Filter.get(query['having'])

    logger.info(f"Query parsing successfully completed")
    return attributes, filter_, groups, having


def _build_join_hierarchy(logger: logging.Logger) -> Tuple[Table, Set[Table]]:
    """Function for building join hierarchy of the json query"""
    logger.info("Starting building join hierarchy")
    tables = set()
    root_table = None

    for attr in Attribute.all_attributes:
        if attr.table.related is None:
            root_table = attr.table
        else:
            tables.add(attr.table)
    logger.info("Join hierarchy building successfully completed")
    return root_table, tables


def _build_sql_query(
        attributes: Generator[Attribute, None, None],
        root_table: Table,
        tables: Set[Table],
        filter_: Filter,
        groups: Generator[Attribute, None, None],
        having: Filter,
        logger: logging.Logger
) -> str:
    """Function for building sql query for a particular database from
    schemas objects"""
    logger.info("Starting building SQL query")
    result = ['select']

    attributes_to_select = (_get_pg_attribute(attr) for attr in attributes)

    result.append(', '.join(attributes_to_select))
    result.append(f'from {root_table.name}')
    for join_table in tables:
        result.append(f'join {join_table.name} on '
                      f'{join_table.related}.{join_table.related_on[0]} = '
                      f'{join_table.name}.{join_table.related_on[1]}')

    result.append('where')
    result.append(_get_pg_filter(filter_))

    if groups:
        result.append(f'group by')
        for group in groups:
            result.append(f'{_get_pg_attribute(group)}')

    result.append('having')
    result.append(_get_pg_filter(having))

    result = ' '.join(result)
    logger.info("SQL query building successfully completed")
    return result


"""
In case of several types of databases create abstract class with these 
2 methods(_get_attribute and _get_filter). 
Each type of database - each class with these 2 overridden methods
"""


def _get_pg_attribute(attribute: Attribute) -> str:
    attribute = attribute.attr
    table = attribute.table

    if isinstance(attribute, Aggregate):
        db_name = DataCatalog.get_field(attribute.field.id)
        return f'{attribute.func}({table.name}.{db_name})'
    else:
        db_name = DataCatalog.get_field(attribute.id)
        return f'{table.name}.{db_name}'


def _get_pg_filter(filter_: Filter) -> str:
    if isinstance(filter_, SimpleFilter):
        return f'{_get_pg_attribute(filter_.attr)} ' \
               f'{filter_.operator} {filter_.value}'
    else:
        filter_ = cast(BooleanFilter, filter_)
        parts = (f'({_get_pg_filter(part)})' for part in filter_.values)
        return f' {filter_.operator} '.join(parts)


if __name__ == '__main__':
    """For debugging purposes"""
    from query_compiler.schemas.sample_query import SAMPLE_QUERY
    print(generate_sql_query(SAMPLE_QUERY))
