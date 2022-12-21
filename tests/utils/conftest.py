import pytest
import json


@pytest.fixture()
def get_sample_request_json():
    return json.dumps(
        {
            'guid': 1,
            'query': json.dumps({
                "attributes": [
                    {"field": "case.biz_key"},
                    {"field": "case.sat.open_date"},
                    {"field": "case.doctor.person.name_sat.family_name"},
                    {"field": "case.doctor.person.sat.birth_date"},
                ],
            })
        }
    )


@pytest.fixture()
def get_sample_request_dict():
    return {
        'guid': 1,
        'query': json.dumps({
            "attributes": [
                {"field": "case.biz_key"},
                {"field": "case.sat.open_date"},
                {"field": "case.doctor.person.name_sat.family_name"},
                {"field": "case.doctor.person.sat.birth_date"},
            ],
        })
    }
