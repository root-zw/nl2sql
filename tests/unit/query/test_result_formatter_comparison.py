from types import SimpleNamespace

from server.formatter.result_formatter import ResultFormatter
from server.models.ir import IntermediateRepresentation


def _build_model():
    return SimpleNamespace(
        metrics={},
        measures={},
        fields={},
        dimensions={},
        field_enums={},
    )


def test_format_results_skips_total_row_for_comparison_queries():
    formatter = ResultFormatter(_build_model())
    ir = IntermediateRepresentation(
        query_type="aggregation",
        metrics=["metric_1"],
        comparison_type="yoy",
        show_growth_rate=True,
        original_question="近10年变化情况",
    )

    formatted = formatter.format_results(
        columns=[
            {"name": "成交年份", "type": "string"},
            {"name": "工业用地面积占比", "type": "number"},
            {"name": "工业用地面积占比_同比增长率", "type": "number"},
        ],
        rows=[
            ["2024", 52.75, None],
            ["2025", 71.05, 34.69],
        ],
        ir=ir,
        global_rules=[],
    )

    assert len(formatted) == 2
    assert [row["成交年份"] for row in formatted] == ["2024", "2025"]
