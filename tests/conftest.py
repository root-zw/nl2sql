import sys
from pathlib import Path

import pytest


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


def pytest_collection_modifyitems(items):
    marker_by_segment = {
        "unit": pytest.mark.unit,
        "schema": pytest.mark.schema,
        "integration": pytest.mark.integration,
    }

    for item in items:
        path_parts = Path(str(item.fspath)).parts
        for segment, marker in marker_by_segment.items():
            if segment in path_parts:
                item.add_marker(marker)
