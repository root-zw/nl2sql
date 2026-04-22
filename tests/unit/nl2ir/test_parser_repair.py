import json
from types import SimpleNamespace

import pytest

from server.nl2ir.parser import NL2IRParser


class _FakeLLMClient:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = 0

    async def chat_completion(self, messages, tools, tool_choice):
        payload = self.payloads[min(self.calls, len(self.payloads) - 1)]
        self.calls += 1
        return {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "produce_ir",
                                    "arguments": json.dumps(payload, ensure_ascii=False),
                                }
                            }
                        ]
                    }
                }
            ]
        }

    def extract_function_call(self, response):
        raw = response["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]
        return json.loads(raw)


class _FakeRetriever:
    def __init__(self, result):
        self.result = result

    async def retrieve(self, **kwargs):
        return self.result

    def format_prompt_for_llm(self, hierarchical_result, question):
        return "fake prompt"


def _make_parser_shell():
    parser = object.__new__(NL2IRParser)
    parser.last_format_fix_notes = []
    return parser


def _make_hierarchical_result():
    table = SimpleNamespace(
        total_fields=1,
        table_id="table_1",
        dimensions=[],
        identifiers=[],
        measures=[],
    )
    return SimpleNamespace(
        domain_id=None,
        domain_name="demo",
        table_structures=[table],
        table_results=[],
        few_shot_direct_candidates=None,
        few_shot_examples=None,
        table_retrieval_info=None,
        table_retrieval_method=None,
    )


def _make_parser_with_fake_llm(payloads):
    parser = NL2IRParser(
        llm_client=_FakeLLMClient(payloads),
        semantic_model=None,
        domain_detector=None,
        global_rules_loader=None,
        hierarchical_retriever=_FakeRetriever(_make_hierarchical_result()),
        enum_retriever=None,
    )
    parser._build_retrieval_summary = lambda **kwargs: {}
    parser._calculate_confidence = lambda ir, hierarchical_result: 0.88
    return parser


def test_fix_common_format_errors_normalizes_quoted_scalar_literals():
    parser = _make_parser_shell()

    fixed = parser._fix_common_format_errors(
        {
            "query_type": '"aggregation"',
            "comparison_type": ' "YOY" ',
            "cross_partition_mode": "'compare'",
            "sort_order": " DESC ",
            "join_strategy": '"matched"',
        }
    )

    assert fixed["query_type"] == "aggregation"
    assert fixed["comparison_type"] == "yoy"
    assert fixed["cross_partition_mode"] == "compare"
    assert fixed["sort_order"] == "desc"
    assert fixed["join_strategy"] == "matched"
    assert {item["field"] for item in parser.last_format_fix_notes} == {
        "query_type",
        "comparison_type",
        "cross_partition_mode",
        "sort_order",
        "join_strategy",
    }


def test_fix_common_format_errors_does_not_guess_invalid_literal():
    parser = _make_parser_shell()

    fixed = parser._fix_common_format_errors({"query_type": "aggregate"})

    assert fixed["query_type"] == "aggregate"
    assert parser.last_format_fix_notes == []


def test_build_ir_from_payload_repairs_literal_error_locally():
    parser = _make_parser_shell()

    ir = parser._build_ir_from_payload(
        {
            "query_type": '"aggregation"',
            "comparison_type": '"yoy"',
            "metrics": [],
            "original_question": "今年比去年增长多少",
        }
    )

    assert ir.query_type == "aggregation"
    assert ir.comparison_type == "yoy"
    assert parser.last_format_fix_notes == [
        {
            "field": "query_type",
            "original": '"aggregation"',
            "fixed": "aggregation",
            "reason": "validation_literal_repair",
        },
        {
            "field": "comparison_type",
            "original": '"yoy"',
            "fixed": "yoy",
            "reason": "validation_literal_repair",
        },
    ]


@pytest.mark.asyncio
async def test_parse_handles_quoted_enums_without_retry():
    parser = _make_parser_with_fake_llm(
        [
            {
                "query_type": '"aggregation"',
                "comparison_type": '"yoy"',
                "metrics": [],
            }
        ]
    )

    ir, confidence = await parser.parse("今年比去年增长多少", retry_count=1)

    assert ir.query_type == "aggregation"
    assert ir.comparison_type == "yoy"
    assert confidence == 0.88
    assert parser.llm_client.calls == 1
    assert parser.last_validation_retry_feedback is None
    assert any(item["field"] == "query_type" for item in parser.last_format_fix_notes)
    assert any(item["field"] == "comparison_type" for item in parser.last_format_fix_notes)


@pytest.mark.asyncio
async def test_parse_uses_structured_retry_feedback_for_literal_errors():
    parser = _make_parser_with_fake_llm(
        [
            {
                "query_type": "aggregate",
                "metrics": [],
            },
            {
                "query_type": "aggregation",
                "metrics": [],
            },
        ]
    )

    ir, _ = await parser.parse("帮我统计一下", retry_count=1)

    assert ir.query_type == "aggregation"
    assert parser.llm_client.calls == 2
    assert parser.last_validation_retry_feedback is not None
    assert "query_type" in parser.last_validation_retry_feedback
    assert "aggregate" in parser.last_validation_retry_feedback
    assert "aggregation/detail/duplicate_detection/window_detail" in parser.last_validation_retry_feedback
