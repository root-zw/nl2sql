"""
LLM 表选择模块

负责统一的 LLM 表选择逻辑，支持：
1. 跨连接模式：当未指定 connection_id 时，从所有可访问连接中选表
2. 单连接模式：当已指定 connection_id 时，从该连接的表中选择
"""

from typing import Dict, Any, Optional, List
from uuid import UUID
import structlog

from server.models.api import QueryResponse, TableSelectionCard, TableCandidate
from server.config import settings
from .stream_emitter import QueryStreamEmitter, stream_progress, stream_table_selection, stream_thinking

logger = structlog.get_logger()




async def llm_select_table(
    question: str,
    user_id: str,
    user_role: str,
    connection_id: Optional[str],
    domain_id: Optional[str],
    selected_table_id: Optional[str],
    tracer,
    stream: Optional[QueryStreamEmitter],
    query_id: str,
    timestamp: str
) -> Dict[str, Any]:
    """
    统一的 LLM 表选择入口
    
    直接从数据库查询表元数据，不依赖 semantic_model。
    
    Args:
        question: 用户问题
        user_id: 用户ID
        user_role: 用户角色
        connection_id: 数据库连接ID（可选，为空时进入跨连接模式）
        domain_id: 业务域ID（可选）
        selected_table_id: 用户已选择的表ID（可选，用于确认流程）
        tracer: 追踪器
        stream: WebSocket 流
        query_id: 查询ID
        timestamp: 时间戳
    
    Returns:
        {
            "status": "success" | "need_confirmation" | "error" | "skipped",
            "connection_id": str,      # 成功时
            "selected_table_id": str,  # 成功时
            "selected_table": str,     # 成功时（表名）
            "response": QueryResponse, # 需要确认时
            "error": dict              # 失败时
        }
    """
    from server.utils.db_pool import get_metadata_pool
    from server.nl2ir.llm_table_selector import (
        LLMTableSelector,
        load_all_tables_meta,
        TableMeta,
    )
    from server.nl2ir.table_structure_loader import TableStructureLoader
    from server.dependencies import get_table_selection_llm_client
    from server.models.semantic import SemanticModel
    
    # 如果未启用 LLM 表选择，跳过
    if not settings.llm_table_selection_enabled:
        return {"status": "skipped", "reason": "llm_table_selection_disabled"}
    
    try:
        pool = await get_metadata_pool()
        
        # 1. 如果用户已确认表选择，直接返回
        if selected_table_id:
            async with pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT connection_id, display_name 
                    FROM db_tables 
                    WHERE table_id = $1
                """, UUID(selected_table_id))
                
                if row:
                    return {
                        "status": "success",
                        "connection_id": str(row["connection_id"]),
                        "selected_table": row["display_name"],
                        "selected_table_id": selected_table_id
                    }
                else:
                    return {
                        "status": "error",
                        "error": {
                            "code": "TABLE_NOT_FOUND",
                            "message": f"未找到表 {selected_table_id}"
                        }
                    }
        
        # 2. 确定要查询的连接范围
        if connection_id:
            # 单连接模式
            connection_ids = [connection_id]
            step_name = "LLM表选择"
        else:
            # 跨连接模式：获取用户可访问的连接列表
            connection_ids = await _get_accessible_connections(pool, user_id, user_role)
            if not connection_ids:
                return {
                    "status": "error",
                    "error": {
                        "code": "NO_ACCESS",
                        "message": "您没有任何可访问的数据源"
                    }
                }
            step_name = "LLM表选择"
        
        logger.debug(f"LLM表选择模式", mode="single" if connection_id else "cross", connections=len(connection_ids))
        
        # 3. 创建轻量级的结构加载器（只用于格式化，不加载完整语义模型）
        empty_model = SemanticModel(version="2.0")
        structure_loader = TableStructureLoader(empty_model)
        
        # 4. 从数据库加载表元数据（带用户权限过滤）
        all_tables_meta = await load_all_tables_meta(
            connection_id=connection_id,
            structure_loader=structure_loader,
            connection_ids=connection_ids if not connection_id else None,
            user_id=user_id,
            user_role=user_role
        )
        
        if not all_tables_meta:
            return {
                "status": "error",
                "error": {
                    "code": "NO_TABLES",
                    "message": "未找到任何可查询的数据表"
                }
            }
        
        logger.debug(f"加载了 {len(all_tables_meta)} 张表的元数据")
        
        # 5. 开始 LLM 表选择的 trace 步骤
        step_desc = "使用LLM智能选择最相关的数据表"
        table_selection_step = tracer.start_step(step_name, "table_selection", step_desc)
        await stream_progress(stream, step_name, "started", step_desc, {
            "connection_count": len(connection_ids),
            "table_count": len(all_tables_meta)
        })
        table_selection_step.set_input({
            "question": question,
            "connection_count": len(connection_ids),
            "table_count": len(all_tables_meta)
        })
        
        # 思考过程：开始分析
        question_preview = question[:50] + '...' if len(question) > 50 else question
        await stream_thinking(stream, "table_selection", f"正在分析问题：「{question_preview}」\n正在通过AI选择最匹配的数据表...", done=False, step_status="started")
        
        # 6. 调用 LLM 选表（使用表选择场景的客户端，支持独立配置模型）
        llm_client = get_table_selection_llm_client()
        selector = LLMTableSelector(llm_client, structure_loader)
        
        selection_result = await selector.select_tables(
            question=question,
            all_tables_meta=all_tables_meta
        )
        
        # 添加 LLM 提示词到 metadata
        if selector.last_system_prompt or selector.last_user_prompt:
            table_selection_step.add_metadata("llm_prompts", [
                {
                    "__type__": "markdown",
                    "title": "Table Selection System Prompt",
                    "content": selector.last_system_prompt or ""
                },
                {
                    "__type__": "markdown",
                    "title": "Table Selection User Prompt",
                    "content": selector.last_user_prompt or ""
                }
            ])
        if selector.last_result_json:
            table_selection_step.add_metadata("llm_response", selector.last_result_json)
        
        # 7. 根据结果决定下一步
        if selection_result.action == "confirm":
            # 需要用户确认
            table_selection_step.set_output({
                "action": "confirm",
                "candidates": [
                    {"table_name": c.table_name, "confidence": c.confidence}
                    for c in selection_result.candidates
                ]
            })
            tracer.end_step()
            await stream_progress(stream, step_name, "pending", "等待用户确认表选择", None)
            
            # 获取配置：总候选数和每页大小
            # 注意：page_size 使用 max_candidates 配置，表示每批显示的表数量
            total_candidates_limit = settings.llm_table_selection_total_candidates
            page_size = settings.llm_table_selection_max_candidates
            
            # 获取所有有效候选（置信度大于0即可，LLM已过滤低于0.3的）
            all_candidates = [c for c in selection_result.candidates if c.confidence > 0][:total_candidates_limit]
            
            # 使用 LLM 返回的多表查询判断（完全依赖 LLM）
            is_multi_table = selection_result.is_multi_table_query
            multi_table_mode = selection_result.multi_table_mode
            multi_table_hint = selection_result.multi_table_hint
            recommended_table_ids = selection_result.recommended_table_ids

            # 根据 LLM 判断决定是否允许多选
            # 支持的多选模式：compare（跨年对比）、union（跨年合并）、multi_join（跨表关联）
            # 以及旧版兼容：cross_year、cross_partition、cross_year_compare、cross_year_union
            allow_multi = is_multi_table and multi_table_mode in (
                "compare", "union", "multi_join",  # 新版
                "cross_year", "cross_partition", "cross_year_compare", "cross_year_union"  # 旧版兼容
            )

            # 构建确认卡消息和原因
            if allow_multi and multi_table_hint:
                message = multi_table_hint
                if multi_table_mode == "multi_join":
                    confirmation_reason = "跨表关联查询，请确认需要关联的数据表"
                elif multi_table_mode in ("compare", "union", "cross_year", "cross_year_compare", "cross_year_union"):
                    confirmation_reason = "跨年度查询，请确认需要查询的年份"
                else:
                    confirmation_reason = "多表查询，请确认需要查询的数据表"
            else:
                message = "系统找到了多个可能相关的表，请确认您要查询的是哪张表："
                # 根据置信度情况生成确认原因
                top_confidence = all_candidates[0].confidence if all_candidates else 0
                if top_confidence < 0.7:
                    confirmation_reason = f"AI 置信度较低（{top_confidence:.0%}），请帮助确认"
                elif len(all_candidates) > 1:
                    gap = all_candidates[0].confidence - all_candidates[1].confidence if len(all_candidates) > 1 else 1
                    if gap < 0.15:
                        confirmation_reason = "存在多个相似度接近的表，请选择最合适的"
                    else:
                        confirmation_reason = "找到多个可能相关的表"
                else:
                    confirmation_reason = "请确认数据表选择"

            # 构建确认卡
            card = TableSelectionCard(
                candidates=all_candidates,  # 返回所有候选表，前端分批展示
                question=question,
                message=message,
                confirmation_reason=confirmation_reason,
                allow_multi_select=allow_multi,
                multi_table_mode=multi_table_mode if allow_multi else None,  # 传递多表模式给前端
                page_size=page_size,
                total_candidates=len(all_candidates),
                is_cross_year_query=(multi_table_mode in ("compare", "union", "cross_year", "cross_year_compare", "cross_year_union")),
                cross_year_hint=multi_table_hint,
                recommended_table_ids=recommended_table_ids  # LLM 推荐的表，前端用于预选
            )
            
            await stream_table_selection(stream, card, query_id)
            
            return {
                "status": "need_confirmation",
                "response": QueryResponse(
                    status="table_selection_needed",
                    table_selection=card,
                    query_id=query_id,
                    timestamp=timestamp
                )
            }
        
        if selection_result.action == "clarify":
            # 置信度过低
            table_selection_step.set_output({
                "action": "clarify",
                "reason": "置信度过低"
            })
            tracer.end_step()
            await stream_progress(stream, step_name, "failed", "未能匹配到相关数据表", None)
            
            return {
                "status": "error",
                "error": {
                    "code": "TABLE_NOT_FOUND",
                    "message": "未能匹配到相关数据表，请尝试更具体地描述您的问题。"
                }
            }
        
        # 8. 选表成功
        primary_table_id = selection_result.primary_table_id
        if not primary_table_id and selection_result.candidates:
            primary_table_id = selection_result.candidates[0].table_id

        # 获取推荐表列表（跨年查询时可能有多个表）
        recommended_table_ids = selection_result.recommended_table_ids or [primary_table_id]
        is_multi_table = selection_result.is_multi_table_query
        multi_table_mode = selection_result.multi_table_mode

        # 从元数据中获取 connection_id
        selected_table_meta = next(
            (t for t in all_tables_meta if t.table_id == primary_table_id),
            None
        )

        if not selected_table_meta:
            return {
                "status": "error",
                "error": {
                    "code": "TABLE_META_NOT_FOUND",
                    "message": "无法获取选中表的元数据"
                }
            }

        detected_connection_id = selected_table_meta.connection_id or connection_id

        # 构建输出信息
        # 当是多表查询时（跨年/跨分区/多表关联），显示所有推荐表的名称
        # 支持新版模式值（compare/union/multi_join）和旧版兼容值
        if is_multi_table and multi_table_mode in (
            "compare", "union", "multi_join",  # 新版
            "cross_year", "cross_partition", "cross_year_compare", "cross_year_union"  # 旧版兼容
        ) and len(recommended_table_ids) > 1:
            # 多表查询：获取所有推荐表的名称
            selected_table_names = []
            for tid in recommended_table_ids:
                meta = next((t for t in all_tables_meta if t.table_id == tid), None)
                if meta:
                    selected_table_names.append(meta.display_name)
            selected_table_display = ", ".join(selected_table_names)
        else:
            selected_table_display = selected_table_meta.display_name

        table_selection_step.set_output({
            "action": "execute",
            "selected_table": selected_table_display,
            "table_id": primary_table_id,
            "selected_table_ids": recommended_table_ids,
            "connection_id": detected_connection_id,
            "confidence": selection_result.candidates[0].confidence if selection_result.candidates else None,
            "is_multi_table_query": is_multi_table,
            "multi_table_mode": multi_table_mode
        })
        tracer.end_step()
        await stream_progress(stream, step_name, "success", f"已选择表: {selected_table_display}", {
            "selected_table": selected_table_display,
            "connection_id": detected_connection_id
        })

        # 思考过程：选表完成
        confidence = selection_result.candidates[0].confidence if selection_result.candidates else 0
        if is_multi_table and len(recommended_table_ids) > 1:
            # 根据 multi_table_mode 显示正确的模式名称
            mode_display = {
                # 新版模式值
                "compare": "跨年对比分析",
                "union": "跨年合并",
                "multi_join": "多表关联",
                # 旧版兼容
                "cross_year": "跨年对比",
                "cross_year_compare": "跨年对比分析",
                "cross_year_union": "跨年合并",
                "cross_partition": "跨分区查询",
            }.get(multi_table_mode, "多表查询")
            
            thinking_lines = [
                f"选定表: **{selected_table_display}**",
                f"置信度: {confidence:.1%}",
                f"查询模式: {mode_display} ({len(recommended_table_ids)}张表)",
                "→ 所有表置信度都较高，直接执行查询"
            ]
        else:
            thinking_lines = [
                f"选定表: **{selected_table_meta.display_name}**",
                f"置信度: {confidence:.1%}",
                "→ 置信度高，直接执行查询"
            ]
        await stream_thinking(stream, "table_selection", "\n".join(thinking_lines), done=True, step_status="success")

        return {
            "status": "success",
            "connection_id": detected_connection_id,
            "selected_table": selected_table_display,
            "selected_table_id": primary_table_id,
            "selected_table_ids": recommended_table_ids,  # 新增：返回多个表ID
            "is_multi_table_query": is_multi_table,
            "multi_table_mode": multi_table_mode
        }
        
    except Exception as e:
        logger.exception("LLM 表选择失败", error=str(e))
        return {
            "status": "error",
            "error": {
                "code": "LLM_SELECTION_ERROR",
                "message": f"LLM 表选择失败: {str(e)}"
            }
        }


async def _get_accessible_connections(pool, user_id: str, user_role: str) -> List[str]:
    """获取用户可访问的连接ID列表"""
    user_uuid = UUID(user_id) if user_id else None
    
    async with pool.acquire() as conn:
        if user_role == 'admin':
            connections = await conn.fetch("""
                SELECT connection_id 
                FROM database_connections 
                WHERE is_active = TRUE
            """)
        else:
            # 检查用户是否有 scope_type='all' 的角色
            has_all = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM user_data_roles udr
                    JOIN data_roles dr ON udr.role_id = dr.role_id
                    WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                    AND dr.is_active = TRUE AND dr.scope_type = 'all'
                )
            """, user_uuid)
            
            if has_all:
                connections = await conn.fetch("""
                    SELECT connection_id 
                    FROM database_connections 
                    WHERE is_active = TRUE
                """)
            else:
                connections = await conn.fetch("""
                    SELECT DISTINCT dc.connection_id
                    FROM user_data_roles udr
                    JOIN data_roles dr ON udr.role_id = dr.role_id
                    JOIN role_table_permissions rtp ON dr.role_id = rtp.role_id
                    JOIN db_tables t ON rtp.table_id = t.table_id
                    JOIN database_connections dc ON t.connection_id = dc.connection_id
                    WHERE udr.user_id = $1 AND udr.is_active = TRUE 
                    AND dr.is_active = TRUE AND dc.is_active = TRUE
                """, user_uuid)
    
    return [str(c["connection_id"]) for c in connections]


# ============================================================
# 保留旧函数名作为别名，保持向后兼容
# ============================================================
async def llm_select_table_cross_connections(
    question: str,
    user_id: str,
    user_role: str,
    domain_id: Optional[str],
    selected_table_id: Optional[str],
    tracer,
    stream: Optional[QueryStreamEmitter],
    query_id: str,
    timestamp: str
) -> Dict[str, Any]:
    """
    跨连接的 LLM 表选择（向后兼容别名）
    
    实际调用 llm_select_table，connection_id=None 表示跨连接模式
    """
    return await llm_select_table(
        question=question,
        user_id=user_id,
        user_role=user_role,
        connection_id=None,  # 跨连接模式
        domain_id=domain_id,
        selected_table_id=selected_table_id,
        tracer=tracer,
        stream=stream,
        query_id=query_id,
        timestamp=timestamp
    )

