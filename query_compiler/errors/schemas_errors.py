from query_compiler.errors import QueryCompilerError


class AttributeConvertError(QueryCompilerError):
    def __init__(self, record):
        super().__init__(
            f"Couldn't convert the record {record} to one of the Attribute"
            f"child classes: Field, Alias, Aggregate"
        )


class FilterConvertError(QueryCompilerError):
    def __init__(self, record):
        super().__init__(
            f"Couldn't convert the record {record} to one of the Filter child"
            f"classes: BooleanFilter, SimpleFilter"
        )


class FilterValueCastError(QueryCompilerError):
    def __init__(self, type_name, value):
        super().__init__(
            f"Couldn't cast SimpleFilter value {value} to {type_name}"
        )
