class QueryParseServiceError(Exception):
    def __init__(self, message: str = None):
        super().__init__(message)


class DeserializeJSONQueryError(QueryParseServiceError):
    def __init__(self, json_query: bytes):
        super().__init__(
            f"Couldn't deserialize the input json query {json_query}"
        )
