from query_compiler.schemas.table import Table


class DataCatalog:
    _attributes = {
        "patient.age": {
            "db": "main",
            "table": {"name": "patient"},
            "field": "age",
            "type": "int",
        },
        "patient.appointment": {
            "db": "main",
            "table": {
                "name": "appointment",
                "relation": {
                    "table": "patient",
                    "on": ("id", "patient_id"),
                },
            },
            "field": "id",
            "type": "int",
        },
        "patient.appointment.at": {
            "db": "main",
            "table": {
                "name": "appointment",
                "relation": {
                    "table": "patient",
                    "on": ("id", "patient_id"),
                },
            },
            "field": "age",
            "type": "date",
        }
    }

    @classmethod
    def get_table(cls, name):
        return Table(cls._attributes[name]['table'])

    @classmethod
    def get_field(cls, name):
        return cls._attributes[name]['field']

    @classmethod
    def get_type(cls, name):
        return cls._attributes[name]['type']
