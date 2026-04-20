from server.services.followup_context_service import FollowupContextService


def build_messages():
    return [
        {
            "message_id": "m-user-1",
            "role": "user",
            "content": "查询武汉土地成交情况",
            "status": "completed",
        },
        {
            "message_id": "m-assistant-1",
            "role": "assistant",
            "content": "武汉土地成交情况结果如下",
            "query_id": "q-result-1",
            "result_summary": "上一结果按区域展示了武汉土地成交总价。",
            "result_data": {
                "columns": [{"name": "区域"}, {"name": "成交总价"}],
                "rows": [["武昌", 100], ["江夏", 80]],
                "meta": {
                    "selected_table_ids": ["table_land_deal"],
                    "ir": {
                        "metrics": ["deal_amount"],
                        "dimensions": ["district"],
                    },
                },
            },
            "status": "completed",
        },
    ]


def test_resolve_followup_context_can_continue_on_result():
    result = FollowupContextService.resolve_followup_context("那按区域展开看一下呢？", build_messages())

    assert result["resolution"] == "continue_on_result"
    assert result["analysis_context"]["context_mode"] == "followup"
    assert result["analysis_context"]["inherit_from_query_id"] == "q-result-1"
    assert result["analysis_context"]["base_result_refs"][0]["table_ids"] == ["table_land_deal"]


def test_resolve_followup_context_can_compare_with_result():
    result = FollowupContextService.resolve_followup_context("和刚才的结果对比一下成都", build_messages())

    assert result["resolution"] == "compare_with_result"
    assert result["analysis_context"]["context_mode"] == "compare"
    assert result["analysis_context"]["comparison_result_refs"][0]["query_id"] == "q-result-1"


def test_resolve_followup_context_can_fall_back_to_new_query():
    result = FollowupContextService.resolve_followup_context("查询成都今年成交总价前十地块", build_messages())

    assert result["resolution"] == "resolved_to_new_query"
    assert "analysis_context" not in result


def test_resolve_followup_context_can_require_clarification():
    result = FollowupContextService.resolve_followup_context("嗯", build_messages())

    assert result["resolution"] == "need_clarification"
    assert "新问题" in result["message"]
