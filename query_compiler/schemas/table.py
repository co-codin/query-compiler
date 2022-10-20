class Table:
    def __init__(self, record):
        self.name = record['name']
        self.joins = []

        table = self.name
        for rel in reversed(record.get('relation', ())):
            self.joins.append(Relation(table, rel))
            table = rel['table']
        self.joins.reverse()

    def __hash__(self):
        return hash((self.name, self.joins))

    def __eq__(self, other):
        return self.name == other.name and self.joins == other.joins


class Relation:
    def __init__(self, table, record):
        self.table = table
        self.related_table = record['table']
        self.related_key, self.key = record['on']

    def __hash__(self):
        return hash(
            (self.table, self.related_table, self.related_key, self.key))

    def __eq__(self, other):
        return (
               self.table, self.related_table, self.related_key, self.key) == (
               other.table, other.related_table, other.related_key, other.key)
