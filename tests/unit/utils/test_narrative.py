from server.explain.narrative import build_empty_result_guidance


def test_build_empty_result_guidance_includes_current_constraints():
    facts = {
        "row_count": 0,
        "table_name": "公开成交",
        "selected_tables": ["公开成交"],
        "filter_scope": [
            {"field": "成交年份", "operator": ">=", "value": 2016},
            {"field": "出让方式", "operator": "LIKE", "value": "%招拍挂%"},
            {
                "field": "行政区",
                "operator": "IN",
                "value_preview": ["江岸区", "江汉区", "硚口区"],
                "value_count": 5,
            },
        ],
        "permission_context": ["数据已按所属区域=武汉市进行权限过滤"],
    }

    guidance = build_empty_result_guidance(facts)

    assert guidance is not None
    assert guidance["query_state"] == "executed_but_no_rows"
    assert guidance["current_table"] == "公开成交"
    assert guidance["selected_tables"] == ["公开成交"]
    assert guidance["permission_limits"] == ["数据已按所属区域=武汉市进行权限过滤"]
    assert guidance["active_filters"] == [
        "成交年份 >= 2016",
        "出让方式 LIKE %招拍挂%",
        "行政区 IN 江岸区、江汉区、硚口区等5项",
    ]


def test_build_empty_result_guidance_returns_none_when_rows_exist():
    facts = {
        "row_count": 3,
        "filter_scope": [{"field": "成交年份", "operator": ">=", "value": 2020}],
    }

    assert build_empty_result_guidance(facts) is None
