SAMPLE_QUERY = {
    "attributes": [
        {
            "field": "patient.age",
        },
        {
            "alias": "appointments"
        }
    ],
    "aliases": {
        "appointments": {
            "aggregate": {
                "function": "count",
                "field": "patient.appointment",
            }
        },
        "appointment_time": { "field": "patient.appointment.at" }
    },
    "group": [
        { "field": "patient.appointment", }
    ],
    "filter": {
        "operator": "AND",
        "values": [
            {
                "operator": "<",
                "field": "patient.age",
                "value": 35
            },
            {
                "field": "patient.appointment.at",
                "operator": ">",
                "value": "2022-08-01",
            }
        ]
    },
    "having": {
        "operator": ">",
        "alias": "appointments",
        "value": 5
    },
}


SAMPLE_QUERY_GRAPH = {
    "attributes": [
        {"field": "case.biz_key"},
        {"field": "case.sat.open_date"},
        {"field": "case.doctor.person.name_sat.family_name"},
        {"field": "case.doctor.person.sat.birth_date"},
    ],
}
