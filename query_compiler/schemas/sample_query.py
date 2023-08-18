test_dict1 = {
    "distinct": True,
    "aliases": {
        "dwh_i10_dev.dv_raw.diagnosisparam_hub._biz_key": {
            "attr": {
                "db_link": "dwh_i10_dev.dv_raw.diagnosisparam_hub._biz_key",
                "display": True
            }
        },
        "dwh_i10_dev.dv_raw.diagnosisparam_hub.diagnosisparam_diagnosis_link.diagnosis_hub._batchid": {
            "attr": {
                "db_link": "dwh_i10_dev.dv_raw.diagnosisparam_hub.diagnosisparam_diagnosis_link.diagnosis_hub._batchid",
                "display": True
            }
        },
        "sum.dwh_i10_dev.dv_raw.diagnosisparam_hub._biz_key": {
            "aggregate": {
                "function": "sum",
                "db_link": "dwh_i10_dev.dv_raw.diagnosisparam_hub._biz_key",
                "display": False
            }
        },
        "avg.dwh_i10_dev.dv_raw.diagnosisparam_hub.diagnosisparam_diagnosis_link.diagnosis_hub._batchid": {
            "aggregate": {
                "function": "avg",
                "db_link": "dwh_i10_dev.dv_raw.diagnosisparam_hub.diagnosisparam_diagnosis_link.diagnosis_hub._batchid",
                "display": False
            }
        }
    },
    "filter": {
        "key": "asdasd",
        "operator": "and",
        "values": [
            {
                "key": "asdasd",
                "operator": "not",
                "values": [
                    {
                        "alias": "dwh_i10_dev.dv_raw.diagnosisparam_hub._biz_key",
                        "value": 10,
                        "operator": "<"
                    }
                ]
            },
            {
                "key": "asdasd",
                "operator": "between",
                "alias": "dwh_i10_dev.dv_raw.diagnosisparam_hub.diagnosisparam_diagnosis_link.diagnosis_hub._batchid",
                "value": [1, 10]
            }
        ]
    },
    # 'group': [
    #     "dwh_i10_dev.dv_raw.diagnosisparam_hub._biz_key",
    #     "dwh_i10_dev.dv_raw.diagnosisparam_hub.diagnosisparam_diagnosis_link.diagnosis_hub._batchid"
    # ],
    'having': {
        "key": "asdasd",
        'operator': 'and',
        'values': [
            {
                "key": "asdasd",
                'operator': 'not',
                'values': [
                    {
                        "key": "asdasd",
                        'operator': 'in',
                        'alias': 'sum.dwh_i10_dev.dv_raw.diagnosisparam_hub._biz_key',
                        'value': [100, 200]
                    }
                ]
            },
            {
                "key": "asdasd",
                'operator': '>=',
                'alias': 'avg.dwh_i10_dev.dv_raw.diagnosisparam_hub.diagnosisparam_diagnosis_link.diagnosis_hub._batchid',
                'value': 5
            }
        ]
    }
}

test_dict2 = {
    'distinct': False,
    'aliases': {
        'dwh_i10_dev.dv_raw.doctor_sat.idposition': {
            'attr': {'db_link': 'dwh_i10_dev.dv_raw.doctor_sat.idposition', 'display': True}
        },
        'dwh_i10_dev.dv_raw.doctor_sat.doctor_hub.doctor_person_link.person_hub.person_sat.idsex': {
            'attr': {
                'db_link': 'dwh_i10_dev.dv_raw.doctor_sat.doctor_hub.doctor_person_link.person_hub.person_sat.idsex',
                'display': True
            }
        },
    }
}
