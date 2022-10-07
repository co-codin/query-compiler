class Table:
    def __init__(self, record):
        self.name = record['name']
        self.related = None
        self.related_on = None
        if 'relation' in record:
            self.related = record['relation']['table']
            self.related_on = record['relation']['on']

    def __hash__(self):
        return hash((self.name, self.related, self.related_on))

    def __eq__(self, other):
        return self.name == other.name and \
               self.related == other.related and \
               self.related_on == other.related_on
