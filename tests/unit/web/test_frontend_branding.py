from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[3]


def test_query_simple_view_is_removed():
    query_view = ROOT_DIR / "frontend" / "src" / "views" / "Query.vue"

    assert not query_view.exists()


def test_router_no_longer_registers_query_simple_route():
    router_file = ROOT_DIR / "frontend" / "src" / "router" / "index.js"
    router_source = router_file.read_text(encoding="utf-8")

    assert "/query-simple" not in router_source
    assert "path: '/query'" in router_source
    assert "redirect: '/chat'" in router_source


def test_frontend_branding_and_docs_use_smart_query_name():
    checked_files = [
        ROOT_DIR / "frontend" / "src" / "views" / "Chat.vue",
        ROOT_DIR / "frontend" / "src" / "views" / "Login.vue",
        ROOT_DIR / "frontend" / "src" / "views" / "admin" / "Login.vue",
        ROOT_DIR / "frontend" / "src" / "views" / "admin" / "components" / "MetadataManage.vue",
        ROOT_DIR / "README.md",
        ROOT_DIR / "env.template",
    ]

    for file_path in checked_files:
        content = file_path.read_text(encoding="utf-8")
        assert "智能问数" in content
        assert "湖北省城市体检更新AI智能问数" not in content
        assert "AI智能问数" not in content
        assert "城市体检" not in content
