from abc import ABC


class Attribute(ABC):
    all_attributes = set()
    """"Implementation of class Attribute"""


class Field(Attribute):
    """Implementation of class Field"""


class Alias(Attribute):
    aliases = {}
    """Implementation of class Alias"""


class Aggregate(Attribute):
    """Implementation of class Aggregate"""
