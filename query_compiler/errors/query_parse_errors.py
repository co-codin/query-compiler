from query_compiler.errors import QueryCompilerError


class DeserializeJSONQueryError(QueryCompilerError):
    def __init__(self, json_query: bytes):
        super().__init__(
            f"Couldn't deserialize the input json query {json_query}"
        )


class AccessDeniedError(QueryCompilerError):
    def __init__(self, denied_fields):
        super().__init__(
            f"Access denied for {denied_fields}"
        )
        self.denied_fields = denied_fields
