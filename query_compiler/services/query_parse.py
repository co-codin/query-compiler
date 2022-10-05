import json
import logging

from typing import Dict, List, Tuple

from query_compiler.schemas.attribute import Attribute
from query_compiler.schemas.filter import Filter
from query_compiler.schemas.table import Table
from query_compiler.errors.query_parse_errors import DeserializeJSONQueryError


def generate_sql_query(query: bytes) -> str:
    """Function for generating sql query from json query"""
    logger = logging.getLogger(__name__)

    dict_query = _from_json_to_dict(query, logger)
    attributes, filter_, groups, having = _parse_query(dict_query, logger)
    root_table, tables = _build_join_hierarchy(logger)
    sql_query = _build_sql_query(
        attributes, root_table, tables, filter_, groups, having, logger
    )
    return sql_query


def _from_json_to_dict(query: bytes, logger: logging.Logger) -> Dict:
    try:
        logger.info(f"Deserializing the input json query {query}")
        dict_query = json.loads(query)
    except json.JSONDecodeError as json_decode_err:
        raise DeserializeJSONQueryError(query) from json_decode_err
    else:
        return dict_query


def _parse_query(query: Dict, logger: logging.Logger) -> \
        Tuple[List[Attribute], Filter, List[Attribute], Filter]:
    """Function for parsing json query and creating schemas objects"""


def _build_join_hierarchy(logger: logging.Logger) -> Tuple[Table, List[Table]]:
    """Function for building join hierarchy of the json query"""


def _build_sql_query(
        attributes: List[Attribute],
        root_table: Table,
        tables: List[Table],
        filter_: Filter,
        groups: List[Attribute],
        having: Filter,
        logger: logging.Logger
) -> str:
    """Function for building sql query for a particular database from
    schemas objects"""


"""Separate classes for this 2 methods in case of different types of 
databases"""


def _get_pg_attribute() -> str:
    pass


def _get_pg_filter() -> str:
    pass
