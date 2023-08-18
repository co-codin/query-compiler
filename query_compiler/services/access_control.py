import requests

from typing import Iterable

from query_compiler.errors.query_parse_errors import AccessDeniedError, QueryCompilerError
from query_compiler.schemas.attribute import Attribute, Field
from query_compiler.schemas.data_catalog import DataCatalog
from query_compiler.configs.settings import settings


def check_access(identity_id: str, attributes: Iterable[Attribute]):
    fields = (attr.id for attr in attributes)
    resources = {}
    attr_mapping = {}
    for field in fields:
        table = DataCatalog.get_table(field).name
        name = DataCatalog.get_field(field)
        info = DataCatalog.get_field_attributes(field)

        db_attr = f'{table}.{name}'
        resources[db_attr] = info
        attr_mapping[db_attr] = field

    if not resources:
        return

    response = requests.get(f'{settings.iam_url}/rules/check', json={
        'identity_id': identity_id,
        'resources': resources
    })

    if response.status_code == 403:
        denied_resources = []
        for resource in response.json()['detail']['resources']:
            denied_resources.append(attr_mapping[resource])
        raise AccessDeniedError(denied_resources)
    if response.status_code != 200:
        raise QueryCompilerError()

    return True
