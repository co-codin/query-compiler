from query_compiler.errors.base_error import QueryCompilerError


class DeserializeJSONQueryError(QueryCompilerError):
    def __init__(self, json_query: bytes):
        super().__init__(
            f"Couldn't deserialize the input json query {json_query}"
        )
