from typing import Set

from query_compiler.errors import QueryCompilerError


class DeserializeJSONQueryError(QueryCompilerError):
    def __init__(self, json_query: str):
        super().__init__(
            f"Couldn't deserialize the input json query {json_query}"
        )


class NoRootTable(QueryCompilerError):
    def __init__(self):
        super().__init__("No root table was created")


class NotOneRootTable(QueryCompilerError):
    def __init__(self, root_table_name: Set[str]):
        super().__init__(
            f"More than one root table was built: {root_table_name}"
        )
