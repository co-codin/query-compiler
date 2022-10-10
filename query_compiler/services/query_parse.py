import json
import logging

from typing import Dict, Tuple, Set, cast, Union, Literal, List

from query_compiler.schemas.attribute import Attribute, Alias, Aggregate
from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.schemas.filter import Filter, SimpleFilter, BooleanFilter
from query_compiler.schemas.table import Table
from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError

_result = []


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
    List[Attribute],
    Filter,
    List[Attribute],
    Filter
]:
    """Function for parsing json query and creating schemas objects"""
    logger.info(f"Starting parsing the following query {query}")

    _parse_aliases(query, logger)
    attributes = _parse_attributes(query, logger, key='attributes')

    filter_ = _parse_filter(query, logger, key='filter')
    groups = _parse_attributes(query, logger, key='group')
    having = _parse_filter(query, logger, key='having')

    logger.info(f"Query parsing successfully completed")
    return attributes, filter_, groups, having


def _parse_aliases(query: Dict, logger: logging.Logger):
    try:
        for alias, record in query['aliases'].items():
            Alias.all_aliases[alias] = Attribute.get(record)
    except KeyError:
        logger.warning(f"There's no aliases in the query {query}")


def _parse_attributes(
        query: Dict,
        logger: logging.Logger,
        key: Literal['attributes', 'group']
) -> Union[List[Attribute], None]:
    """Parses attributes and group sections of the input query"""
    try:
        return [Attribute.get(record) for record in query[key]]
    except KeyError:
        logger.warning(f"There's no {key} in the query {query}")


def _parse_filter(
        query: Dict,
        logger: logging.Logger,
        key: Literal['filter', 'having']
) -> Union[Filter, None]:
    """Parses filter and having sections of the input query"""
    try:
        return Filter.get(query[key])
    except KeyError:
        logger.warning(f"There's no {key} in the query {query}")


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
        attributes: List[Attribute],
        root_table: Table,
        tables: Set[Table],
        filter_: Filter,
        groups: List[Attribute],
        having: Filter,
        logger: logging.Logger
) -> str:
    """Function for building sql query for a particular database from
    schemas objects"""
    logger.info("Starting building SQL query")

    _build_attributes_clause(attributes, key='select')
    _build_from_clause(root_table, tables)
    _build_filter_clause(filter_, key='where')
    _build_attributes_clause(groups, key='group by')
    _build_filter_clause(having, key='having')

    result = ' '.join(_result)
    logger.info("SQL query building successfully completed")
    return result


def _build_attributes_clause(
        attributes: List[Attribute],
        key: Literal['select', 'group by']
):
    """Builds select and group by clauses"""
    if attributes:
        _result.append(key)
        attributes_to_append = (_get_pg_attribute(attr) for attr in attributes)
        _result.append(', '.join(attributes_to_append))


def _build_from_clause(root_table: Table, tables: Set[Table]):
    """Builds from and join clauses"""
    _result.append(f'from {root_table.name}')
    for join_table in tables:
        _result.append(f'join {join_table.name} on '
                       f'{join_table.related}.{join_table.related_on[0]} = '
                       f'{join_table.name}.{join_table.related_on[1]}')


def _build_filter_clause(filter_: Filter, key: Literal['where', 'having']):
    """Builds where and having clauses"""
    if filter_:
        _result.append(key)
        _result.append(_get_pg_filter(filter_))


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
