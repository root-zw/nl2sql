from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.frontend_static import register_frontend_routes


def build_test_app(dist_dir, fallback_payload=None):
    app = FastAPI()

    @app.get("/api/health")
    async def health():
        return {"ok": True}

    register_frontend_routes(
        app,
        dist_dir=dist_dir,
        fallback_payload=fallback_payload or {"name": "nl2sql"},
    )
    return app


def test_frontend_root_falls_back_to_backend_payload_when_dist_missing(tmp_path):
    client = TestClient(build_test_app(tmp_path))

    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {"name": "nl2sql"}


def test_frontend_serves_index_for_spa_routes(tmp_path):
    (tmp_path / "index.html").write_text("<html><body>spa</body></html>", encoding="utf-8")
    client = TestClient(build_test_app(tmp_path))

    response = client.get("/admin/users")

    assert response.status_code == 200
    assert "spa" in response.text


def test_frontend_serves_static_files(tmp_path):
    (tmp_path / "index.html").write_text("<html><body>spa</body></html>", encoding="utf-8")
    (tmp_path / "favicon.ico").write_bytes(b"ico")
    client = TestClient(build_test_app(tmp_path))

    response = client.get("/favicon.ico")

    assert response.status_code == 200
    assert response.content == b"ico"


def test_frontend_does_not_override_api_routes(tmp_path):
    (tmp_path / "index.html").write_text("<html><body>spa</body></html>", encoding="utf-8")
    client = TestClient(build_test_app(tmp_path))

    health_response = client.get("/api/health")
    missing_api_response = client.get("/api/missing")

    assert health_response.status_code == 200
    assert health_response.json() == {"ok": True}
    assert missing_api_response.status_code == 404
