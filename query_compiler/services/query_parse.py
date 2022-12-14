import logging

from typing import Dict, Tuple, cast, Union, Literal, List

from query_compiler.utils.parse_utils import deserialize_json_query
from query_compiler.schemas.attribute import Attribute, Alias, Aggregate, Field
from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.schemas.filter import Filter, SimpleFilter, BooleanFilter
from query_compiler.schemas.table import Relation
from query_compiler.services.access_control import check_access
from query_compiler.errors.schemas_errors import NoAttributesInInputQuery
from query_compiler.errors.query_parse_errors import (
    NoRootTable, NotOneRootTable, GroupByError
)

LOG = logging.getLogger(__name__)


def generate_sql_query(query: str, identity_id: str) -> str:
    try:
        return _generate_sql_query(query, identity_id)
    finally:
        _clear()


def _generate_sql_query(query: str, identity_id: str) -> str:
    """Function for generating sql query from json query"""
    LOG.info("Starting generating SQL query from the json query")

    dict_query = deserialize_json_query(query)
    dict_query['_identity_id'] = identity_id

    attributes, filter_, groups, having = _parse_query(dict_query)

    check_access(identity_id, attributes)
    root_table_name, tables = _build_join_hierarchy()
    sql_query = _build_sql_query(
        attributes, root_table_name, tables, filter_, groups, having
    )

    LOG.info("Generating SQL query from the json query successfully "
             "completed"
             )
    return sql_query


def _parse_query(query: Dict) -> Tuple[
    List[Attribute],
    Filter,
    List[Attribute],
    Filter
]:
    """Function for parsing json query and creating schemas objects"""
    LOG.info(f"Starting parsing the following query {query}")

    _parse_aliases(query)
    attributes = _parse_attributes(query)
    groups = _parse_group(query)

    _load_missing_attribute_data()

    filter_ = _parse_filter(query, key='filter')
    having = _parse_filter(query, key='having')

    LOG.info(f"Query parsing successfully completed")
    return attributes, filter_, groups, having


def _load_missing_attribute_data():
    missing_attrs = _get_missing_attribute_names()
    if missing_attrs:
        DataCatalog.load_missing_attr_data_list(missing_attrs)


def _get_missing_attribute_names() -> List[str]:
    return [
        attribute.id
        for attribute in Attribute.all_attributes
        if (isinstance(attribute, Field) and
            not DataCatalog.is_field_in_attributes_dict(attribute.id))
    ]


def _parse_aliases(query: Dict):
    try:
        for alias, record in query['aliases'].items():
            Alias.all_aliases[alias] = Attribute.get(record)
    except KeyError:
        LOG.info(f"There's no aliases in the query {query}")


def _parse_attributes(query: Dict) -> Union[List[Attribute], None]:
    """Parses attributes of the input query"""
    try:
        attr_list = []
        attr_set = set()
        for record in query['attributes']:
            attr = Attribute.get(record)
            if attr in attr_set:
                continue
            attr_set.add(attr)
            attr_list.append(attr)
        return attr_list
    except KeyError:
        raise NoAttributesInInputQuery(query)


def _parse_group(query: Dict) -> Union[List[Attribute], None]:
    """Parses group of the input query"""
    if 'group' in query:
        group_attrs = []
        for record in query['group']:
            if record['field'] not in map(_get_attr_field,
                                          Attribute.all_attributes):
                raise GroupByError()
            group_attrs.append(Attribute.get(record))
        return group_attrs
    else:
        LOG.info(f"There's no group in the query {query}")


def _get_attr_field(attribute):
    """
    Extracts field from Attribute object. The cases are as follows:
    1) Field: attribute.field
    2) Aggregate: attribute.field.field
    3) Alias -> Field: attribute.attr.field
    4) Alias -> Aggregate: attribute.attr.field.field
    :param attribute: Attribute
    :return: str
    """
    attr = attribute.attr  # in case attribute is Alias
    return attr.field if isinstance(attr, Field) else attr.field.field


def _parse_filter(
        query: Dict,
        key: Literal['filter', 'having']
) -> Union[Filter, None]:
    """Parses filter and having sections of the input query"""
    try:
        return Filter.get(query[key])
    except KeyError:
        LOG.info(f"There's no {key} in the query {query}")


def _build_join_hierarchy() -> Tuple[str, List[Relation]]:
    """Function for building join hierarchy of the json query"""
    LOG.info("Starting building join hierarchy")
    root_table_name = set()
    joined = set()
    relations = []
    for attr in Attribute.all_attributes:
        table = attr.table
        if not table.joins:
            root_table_name.add(table.name)
        else:
            root_table_name.add(table.joins[-1].related_table)

        for relation in reversed(table.joins):
            if relation in joined:
                continue
            relations.append(relation)
            joined.add(relation)
    if len(root_table_name) == 0:
        raise NoRootTable()
    elif len(root_table_name) > 1:
        raise NotOneRootTable(root_table_name)

    LOG.info("Join hierarchy building successfully completed")
    return root_table_name.pop(), relations


def _build_sql_query(
        attributes: List[Attribute],
        root_table_name: str,
        tables: List[Relation],
        filter_: Filter,
        groups: List[Attribute],
        having: Filter,
) -> str:
    """Function for building sql query for a particular database from
    schemas objects"""
    LOG.info("Starting building SQL query")

    select = _build_attributes_clause(attributes, key='select')
    tables = _build_from_clause(root_table_name, tables)
    where = _build_filter_clause(filter_, key='where')
    group_by = _build_attributes_clause(groups, key='group by')
    having = _build_filter_clause(having, key='having')
    sql_query = _piece_sql_statements_together(select, tables, where, group_by,
                                               having)

    LOG.info("SQL query building successfully completed")
    return sql_query


def _piece_sql_statements_together(*args):
    return ' '.join(filter(lambda item: item is not None, args))


def _build_attributes_clause(
        attributes: List[Attribute],
        key: Literal['select', 'group by']
):
    """Builds select and group by clauses"""
    if attributes:
        attributes_to_append = (_get_pg_attribute(attr) for attr in attributes)
        pg_attributes = ', '.join(attributes_to_append)
        return f"{key} {pg_attributes}"


def _build_from_clause(root_table_name: str, relations: List[Relation]):
    """Builds from and join clauses"""
    from_to_append = (
        f'join {rel.table} on '
        f'{rel.related_table}.{rel.related_key} = '
        f'{rel.table}.{rel.key}'
        for rel in relations
    )
    from_tables = ', '.join(from_to_append)
    return f'from {root_table_name} {from_tables}'


def _build_filter_clause(filter_: Filter, key: Literal['where', 'having']):
    """Builds where and having clauses"""
    if filter_:
        pg_filter = _get_pg_filter(filter_)
        return f"{key} {pg_filter}"


def _clear():
    Attribute.clear()
    Alias.clear()


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
