from query_compiler.errors import QueryCompilerError


class DeserializeJSONQueryError(QueryCompilerError):
    def __init__(self, json_query: str):
        super().__init__(
            f"Couldn't deserialize the following json query {json_query}"
        )


class NoRootTable(QueryCompilerError):
    def __init__(self):
        super().__init__("No root table was created")


class NotOneRootTable(QueryCompilerError):
    def __init__(self, root_table_name: set[str]):
        super().__init__(
            f"More than one root table was built: {root_table_name}"
        )


class GroupByError(QueryCompilerError):
    def __init__(self):
        super().__init__(
            "Group records are not matched up with attributes records"
        )


class AccessDeniedError(QueryCompilerError):
    def __init__(self, denied_fields):
        super().__init__(
            f"Access denied for {denied_fields}"
        )
        self.denied_fields = denied_fields
