"""
监控指标API端点
"""

import structlog
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from server.middleware.auth import require_admin
from server.models.admin import User
from server.monitoring.metrics_collector import get_metrics_collector
from server.sync.error_handler import get_error_handler

logger = structlog.get_logger()
router = APIRouter()


@router.get("/metrics/summary")
async def get_metrics_summary(
    hours: int = Query(default=24, ge=1, le=168),  # 1小时到7天
    current_user: User = Depends(require_admin)
):
    """获取监控指标摘要"""
    try:
        metrics_collector = get_metrics_collector()
        if not metrics_collector:
            raise HTTPException(status_code=503, detail="监控服务未启动")

        summary = await metrics_collector.get_metrics_summary(hours)
        return {
            "success": True,
            "data": summary
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取监控指标摘要失败", error=str(e))
        raise HTTPException(status_code=500, detail=f"获取指标失败: {str(e)}")


@router.get("/metrics/health")
async def get_system_health(
    connection_id: Optional[str] = Query(None),
    current_user: User = Depends(require_admin)
):
    """获取系统健康状态"""
    try:
        metrics_collector = get_metrics_collector()
        if not metrics_collector:
            raise HTTPException(status_code=503, detail="监控服务未启动")

        # 获取最新的系统指标
        if metrics_collector.system_metrics_history:
            latest_system = metrics_collector.system_metrics_history[-1]
        else:
            latest_system = None

        # 获取连接健康状态
        connection_health = {}
        for metrics in metrics_collector.metrics_history[-10:]:  # 最近10个数据点
            if connection_id and metrics.connection_id != connection_id:
                continue

            if metrics.connection_id not in connection_health:
                connection_health[metrics.connection_id] = []

            connection_health[metrics.connection_id].append({
                "timestamp": metrics.timestamp.isoformat(),
                "health_score": metrics.health_score,
                "pending_changes": metrics.pending_changes_count,
                "running_syncs": metrics.running_syncs,
                "error_rate": metrics.error_rate
            })

        # 计算整体健康分数
        overall_health = 1.0
        if latest_system and metrics_collector.metrics_history:
            recent_connection_metrics = [m for m in metrics_collector.metrics_history[-20:]
                                      if not connection_id or m.connection_id == connection_id]
            overall_health = metrics_collector._calculate_overall_health(latest_system, recent_connection_metrics)

        return {
            "success": True,
            "data": {
                "overall_health": overall_health,
                "system_metrics": latest_system.__dict__ if latest_system else None,
                "connection_health": connection_health,
                "timestamp": datetime.utcnow().isoformat()
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取系统健康状态失败", error=str(e))
        raise HTTPException(status_code=500, detail=f"获取健康状态失败: {str(e)}")


@router.get("/metrics/errors")
async def get_error_metrics(
    connection_id: Optional[str] = Query(None),
    days: int = Query(default=7, ge=1, le=30),
    current_user: User = Depends(require_admin)
):
    """获取错误统计信息"""
    try:
        error_handler = get_error_handler()
        if not error_handler:
            raise HTTPException(status_code=503, detail="错误处理服务未启动")

        error_stats = await error_handler.get_error_statistics(connection_id, days)

        return {
            "success": True,
            "data": error_stats
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取错误统计失败", error=str(e))
        raise HTTPException(status_code=500, detail=f"获取错误统计失败: {str(e)}")


@router.get("/metrics/performance")
async def get_performance_metrics(
    connection_id: Optional[str] = Query(None),
    hours: int = Query(default=24, ge=1, le=168),
    current_user: User = Depends(require_admin)
):
    """获取性能指标"""
    try:
        metrics_collector = get_metrics_collector()
        if not metrics_collector:
            raise HTTPException(status_code=503, detail="监控服务未启动")

        since = datetime.utcnow() - timedelta(hours=hours)

        # 过滤指定时间范围的指标
        filtered_metrics = [
            m for m in metrics_collector.metrics_history
            if m.timestamp >= since and (not connection_id or m.connection_id == connection_id)
        ]

        if not filtered_metrics:
            return {
                "success": True,
                "data": {
                    "message": "指定时间范围内无数据",
                    "time_range_hours": hours
                }
            }

        # 按连接分组
        connection_performance = {}
        for metrics in filtered_metrics:
            if metrics.connection_id not in connection_performance:
                connection_performance[metrics.connection_id] = {
                    "sync_durations": [],
                    "entity_counts": [],
                    "health_scores": [],
                    "error_rates": [],
                    "pending_changes": []
                }

            perf = connection_performance[metrics.connection_id]
            if metrics.avg_sync_duration > 0:
                perf["sync_durations"].append(metrics.avg_sync_duration)
            perf["entity_counts"].append(metrics.total_entities_synced)
            perf["health_scores"].append(metrics.health_score)
            perf["error_rates"].append(metrics.error_rate)
            perf["pending_changes"].append(metrics.pending_changes_count)

        # 计算统计信息
        performance_summary = {}
        for conn_id, perf in connection_performance.items():
            performance_summary[conn_id] = {
                "avg_sync_duration": sum(perf["sync_durations"]) / len(perf["sync_durations"]) if perf["sync_durations"] else 0,
                "max_sync_duration": max(perf["sync_durations"]) if perf["sync_durations"] else 0,
                "min_sync_duration": min(perf["sync_durations"]) if perf["sync_durations"] else 0,
                "total_entities_synced": sum(perf["entity_counts"]),
                "avg_health_score": sum(perf["health_scores"]) / len(perf["health_scores"]) if perf["health_scores"] else 1.0,
                "avg_error_rate": sum(perf["error_rates"]) / len(perf["error_rates"]) if perf["error_rates"] else 0,
                "current_pending_changes": perf["pending_changes"][-1] if perf["pending_changes"] else 0,
                "data_points": len([m for m in filtered_metrics if m.connection_id == conn_id])
            }

        return {
            "success": True,
            "data": {
                "time_range_hours": hours,
                "connection_performance": performance_summary,
                "total_data_points": len(filtered_metrics)
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("获取性能指标失败", error=str(e))
        raise HTTPException(status_code=500, detail=f"获取性能指标失败: {str(e)}")


@router.get("/metrics/alerts")
async def get_active_alerts(
    current_user: User = Depends(require_admin)
):
    """获取活跃告警"""
    try:
        alerts = []

        metrics_collector = get_metrics_collector()
        error_handler = get_error_handler()

        # 检查系统健康状态告警
        if metrics_collector and metrics_collector.system_metrics_history:
            latest_system = metrics_collector.system_metrics_history[-1]

            # 错误率告警
            if latest_system.error_rate_last_hour > 0.1:  # 错误率超过10%
                alerts.append({
                    "id": "high_error_rate",
                    "type": "error",
                    "severity": "high",
                    "title": "系统错误率过高",
                    "message": f"最近1小时错误率: {latest_system.error_rate_last_hour:.1%}",
                    "timestamp": datetime.utcnow().isoformat()
                })

            # 队列积压告警
            if latest_system.pending_sync_queue_size > 100:
                alerts.append({
                    "id": "queue_backlog",
                    "type": "performance",
                    "severity": "medium",
                    "title": "同步队列积压",
                    "message": f"待同步变更数量: {latest_system.pending_sync_queue_size}",
                    "timestamp": datetime.utcnow().isoformat()
                })

        # 检查连接健康状态告警
        if metrics_collector:
            for metrics in metrics_collector.metrics_history[-10:]:  # 最近10个数据点
                if metrics.health_score < 0.5:  # 健康分数低于50%
                    alerts.append({
                        "id": f"connection_health_{metrics.connection_id}",
                        "type": "health",
                        "severity": "medium" if metrics.health_score > 0.2 else "high",
                        "title": f"连接健康状态异常",
                        "message": f"连接 {metrics.connection_id} 健康分数: {metrics.health_score:.1%}",
                        "connection_id": metrics.connection_id,
                        "timestamp": metrics.timestamp.isoformat()
                    })

        # 检查严重错误告警
        if error_handler:
            # 这里可以查询最近的严重错误
            # 暂时使用占位符
            pass

        # 按严重程度排序
        severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        alerts.sort(key=lambda x: severity_order.get(x["severity"], 3))

        return {
            "success": True,
            "data": {
                "alerts": alerts,
                "total_count": len(alerts),
                "critical_count": len([a for a in alerts if a["severity"] == "critical"]),
                "high_count": len([a for a in alerts if a["severity"] == "high"]),
                "medium_count": len([a for a in alerts if a["severity"] == "medium"])
            }
        }

    except Exception as e:
        logger.error("获取活跃告警失败", error=str(e))
        raise HTTPException(status_code=500, detail=f"获取告警失败: {str(e)}")