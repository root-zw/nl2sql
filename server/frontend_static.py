"""前端静态资源托管辅助。"""

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


RESERVED_FRONTEND_PREFIXES = ("api", "docs", "redoc", "openapi.json")


def get_frontend_dist_dir() -> Path:
    """返回前端构建产物目录。"""
    return Path(__file__).resolve().parent.parent / "frontend" / "dist"


def should_skip_frontend(path: str) -> bool:
    """保留后端专用路径，避免被 SPA 回退吞掉。"""
    normalized = path.strip("/")
    if not normalized:
        return False

    return any(
        normalized == prefix or normalized.startswith(f"{prefix}/")
        for prefix in RESERVED_FRONTEND_PREFIXES
    )


def resolve_frontend_file(path: str, dist_dir: Path) -> Path | None:
    """解析前端静态文件路径，并阻止目录穿越。"""
    normalized = path.strip("/")
    if not normalized:
        return None

    dist_path = dist_dir.resolve()
    candidate = (dist_path / normalized).resolve()
    try:
        candidate.relative_to(dist_path)
    except ValueError:
        return None

    if candidate.is_file():
        return candidate
    return None


def build_frontend_response(
    path: str,
    *,
    dist_dir: Path,
    fallback_payload: dict | None = None,
):
    """根据请求路径返回前端资源、SPA 入口或后端回退信息。"""
    if should_skip_frontend(path):
        raise HTTPException(status_code=404, detail="Not Found")

    static_file = resolve_frontend_file(path, dist_dir)
    if static_file is not None:
        return FileResponse(static_file)

    index_file = dist_dir / "index.html"
    if index_file.is_file():
        return FileResponse(index_file)

    if not path.strip("/") and fallback_payload is not None:
        return JSONResponse(fallback_payload)

    raise HTTPException(status_code=404, detail="Frontend assets not built")


def register_frontend_routes(
    app: FastAPI,
    *,
    fallback_payload: dict | None = None,
    dist_dir: Path | None = None,
) -> None:
    """为 FastAPI 注册前端静态资源和 SPA 回退路由。"""
    dist_path = (dist_dir or get_frontend_dist_dir()).resolve()
    assets_dir = dist_path / "assets"

    if assets_dir.is_dir():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="frontend-assets")

    @app.get("/", include_in_schema=False)
    async def frontend_index():
        return build_frontend_response(
            "",
            dist_dir=dist_path,
            fallback_payload=fallback_payload,
        )

    @app.get("/{full_path:path}", include_in_schema=False)
    async def frontend_entry(full_path: str):
        return build_frontend_response(full_path, dist_dir=dist_path)
