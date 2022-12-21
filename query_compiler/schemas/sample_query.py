import json


SAMPLE_QUERY_GRAPH = json.dumps({
    "attributes": [
        {"field": "case.biz_key"},
        {"field": "case.sat.open_date"},
        {"field": "case.doctor.person.name_sat.family_name"},
        {"field": "case.doctor.person.sat.birth_date"},
    ],
})
