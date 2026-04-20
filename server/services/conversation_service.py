"""
会话管理服务
提供多轮对话会话的 CRUD 操作和消息管理
"""

import json
from typing import Dict, Any, List, Optional
from uuid import UUID
from datetime import datetime
import structlog
import asyncpg

from server.config import settings
from server.utils.db_pool import get_metadata_pool

logger = structlog.get_logger()


class ConversationService:
    """会话管理服务"""
    
    def __init__(self, db_conn: asyncpg.Connection):
        self.db = db_conn
    
    # ==================== 会话 CRUD ====================
    
    async def create_conversation(
        self,
        user_id: UUID,
        title: Optional[str] = None,
        connection_id: Optional[UUID] = None,
        domain_id: Optional[UUID] = None
    ) -> Dict[str, Any]:
        """创建新会话"""
        row = await self.db.fetchrow("""
            INSERT INTO conversations (user_id, title, connection_id, domain_id)
            VALUES ($1, $2, $3, $4)
            RETURNING conversation_id, user_id, title, connection_id, domain_id,
                      is_active, is_pinned, created_at, updated_at, last_message_at
        """, user_id, title, connection_id, domain_id)
        
        logger.info("会话已创建", conversation_id=str(row['conversation_id']), user_id=str(user_id))
        return self._row_to_dict(row)
    
    async def get_conversation(
        self,
        conversation_id: UUID,
        user_id: Optional[UUID] = None
    ) -> Optional[Dict[str, Any]]:
        """获取会话详情"""
        if user_id:
            row = await self.db.fetchrow("""
                SELECT c.*, dc.connection_name, bd.domain_name
                FROM conversations c
                LEFT JOIN database_connections dc ON c.connection_id = dc.connection_id
                LEFT JOIN business_domains bd ON c.domain_id = bd.domain_id
                WHERE c.conversation_id = $1 AND c.user_id = $2 AND c.is_active = TRUE
            """, conversation_id, user_id)
        else:
            row = await self.db.fetchrow("""
                SELECT c.*, dc.connection_name, bd.domain_name
                FROM conversations c
                LEFT JOIN database_connections dc ON c.connection_id = dc.connection_id
                LEFT JOIN business_domains bd ON c.domain_id = bd.domain_id
                WHERE c.conversation_id = $1 AND c.is_active = TRUE
            """, conversation_id)
        
        return self._row_to_dict(row) if row else None
    
    async def list_conversations(
        self,
        user_id: UUID,
        limit: int = 50,
        offset: int = 0,
        include_inactive: bool = False
    ) -> List[Dict[str, Any]]:
        """获取用户的会话列表"""
        if include_inactive:
            rows = await self.db.fetch("""
                SELECT c.*, dc.connection_name, bd.domain_name,
                       (SELECT COUNT(*) FROM conversation_messages cm WHERE cm.conversation_id = c.conversation_id) as message_count
                FROM conversations c
                LEFT JOIN database_connections dc ON c.connection_id = dc.connection_id
                LEFT JOIN business_domains bd ON c.domain_id = bd.domain_id
                WHERE c.user_id = $1
                ORDER BY c.is_pinned DESC, c.last_message_at DESC NULLS LAST, c.created_at DESC
                LIMIT $2 OFFSET $3
            """, user_id, limit, offset)
        else:
            rows = await self.db.fetch("""
                SELECT c.*, dc.connection_name, bd.domain_name,
                       (SELECT COUNT(*) FROM conversation_messages cm WHERE cm.conversation_id = c.conversation_id) as message_count
                FROM conversations c
                LEFT JOIN database_connections dc ON c.connection_id = dc.connection_id
                LEFT JOIN business_domains bd ON c.domain_id = bd.domain_id
                WHERE c.user_id = $1 AND c.is_active = TRUE
                ORDER BY c.is_pinned DESC, c.last_message_at DESC NULLS LAST, c.created_at DESC
                LIMIT $2 OFFSET $3
            """, user_id, limit, offset)
        
        return [self._row_to_dict(row) for row in rows]
    
    async def update_conversation(
        self,
        conversation_id: UUID,
        user_id: UUID,
        title: Optional[str] = None,
        connection_id: Optional[UUID] = None,
        domain_id: Optional[UUID] = None,
        is_pinned: Optional[bool] = None
    ) -> Optional[Dict[str, Any]]:
        """更新会话"""
        # 构建动态更新语句
        updates = []
        params = [conversation_id, user_id]
        param_idx = 3
        
        if title is not None:
            updates.append(f"title = ${param_idx}")
            params.append(title)
            param_idx += 1
        
        if connection_id is not None:
            updates.append(f"connection_id = ${param_idx}")
            params.append(connection_id)
            param_idx += 1
        
        if domain_id is not None:
            updates.append(f"domain_id = ${param_idx}")
            params.append(domain_id)
            param_idx += 1
        
        if is_pinned is not None:
            updates.append(f"is_pinned = ${param_idx}")
            params.append(is_pinned)
            param_idx += 1
        
        if not updates:
            return await self.get_conversation(conversation_id, user_id)
        
        query = f"""
            UPDATE conversations
            SET {', '.join(updates)}
            WHERE conversation_id = $1 AND user_id = $2 AND is_active = TRUE
            RETURNING conversation_id, user_id, title, connection_id, domain_id,
                      is_active, is_pinned, created_at, updated_at, last_message_at
        """
        
        row = await self.db.fetchrow(query, *params)
        
        if row:
            logger.info("会话已更新", conversation_id=str(conversation_id))
        return self._row_to_dict(row) if row else None
    
    async def delete_conversation(
        self,
        conversation_id: UUID,
        user_id: UUID,
        hard_delete: bool = False
    ) -> bool:
        """删除会话（软删除或硬删除）"""
        if hard_delete:
            result = await self.db.execute("""
                DELETE FROM conversations
                WHERE conversation_id = $1 AND user_id = $2
            """, conversation_id, user_id)
        else:
            result = await self.db.execute("""
                UPDATE conversations
                SET is_active = FALSE
                WHERE conversation_id = $1 AND user_id = $2
            """, conversation_id, user_id)
        
        deleted = result.split()[-1] != '0'
        if deleted:
            logger.info("会话已删除", conversation_id=str(conversation_id), hard_delete=hard_delete)
        return deleted
    
    async def cleanup_old_conversations(self, user_id: UUID) -> int:
        """清理超出限制的旧会话（软删除最旧的非置顶会话）"""
        max_per_user = settings.conversation_max_per_user
        if max_per_user <= 0:
            return 0
        
        # 获取当前活跃会话数量
        count = await self.db.fetchval("""
            SELECT COUNT(*) FROM conversations
            WHERE user_id = $1 AND is_active = TRUE
        """, user_id)
        
        if count <= max_per_user:
            return 0
        
        # 删除超出的非置顶会话
        to_delete = count - max_per_user
        result = await self.db.execute("""
            UPDATE conversations
            SET is_active = FALSE
            WHERE conversation_id IN (
                SELECT conversation_id FROM conversations
                WHERE user_id = $1 AND is_active = TRUE AND is_pinned = FALSE
                ORDER BY last_message_at ASC NULLS FIRST, created_at ASC
                LIMIT $2
            )
        """, user_id, to_delete)
        
        deleted_count = int(result.split()[-1])
        if deleted_count > 0:
            logger.info("已清理旧会话", user_id=str(user_id), deleted_count=deleted_count)
        return deleted_count
    
    # ==================== 消息操作 ====================
    
    async def create_placeholder_message(
        self,
        conversation_id: UUID,
        role: str,
        query_id: Optional[UUID] = None,
        query_params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        创建占位消息（先占位后执行模式）
        
        Args:
            conversation_id: 会话ID
            role: 消息角色 ('user' 或 'assistant')
            query_id: 查询ID（可选）
            query_params: 查询参数（可选）
            metadata: 元数据（可选）
            
        Returns:
            Dict: 创建的消息记录，包含 message_id
        """
        # 创建占位消息，状态为 'running'（assistant）或 'completed'（user）
        status = 'running' if role == 'assistant' else 'completed'
        # assistant 占位时内容为空，user 消息应该已经有内容（但这里作为占位，也允许为空）
        content = ''
        
        row = await self.db.fetchrow("""
            INSERT INTO conversation_messages (
                conversation_id, role, content, query_id, status,
                query_params, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
        """,
            conversation_id, role, content, query_id, status,
            json.dumps(query_params, ensure_ascii=False) if query_params else None,
            json.dumps(metadata, ensure_ascii=False) if metadata else '{}'
        )
        
        logger.debug("占位消息已创建", message_id=str(row['message_id']), conversation_id=str(conversation_id), role=role)
        return self._message_row_to_dict(row)
    
    async def update_message(
        self,
        message_id: UUID,
        content: Optional[str] = None,
        sql_text: Optional[str] = None,
        result_summary: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
        status: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        更新消息内容（用于流式更新）
        
        Args:
            message_id: 消息ID
            content: 内容（可选）
            sql_text: SQL文本（可选）
            result_summary: 结果摘要（可选）
            result_data: 结果数据（可选）
            status: 状态（可选）
            error_message: 错误信息（可选）
            metadata: 元数据（可选）
            
        Returns:
            bool: 是否更新成功
        """
        updates = []
        params = []
        param_idx = 1
        
        if content is not None:
            updates.append(f"content = ${param_idx}")
            params.append(content)
            param_idx += 1
        
        if sql_text is not None:
            updates.append(f"sql_text = ${param_idx}")
            params.append(sql_text)
            param_idx += 1
        
        if result_summary is not None:
            updates.append(f"result_summary = ${param_idx}")
            params.append(result_summary)
            param_idx += 1
        
        if result_data is not None:
            # 处理结果数据（限制行数）
            processed_result_data = None
            if result_data and settings.conversation_save_full_result:
                processed_result_data = self._limit_result_rows(result_data)
            updates.append(f"result_data = ${param_idx}::jsonb")
            params.append(json.dumps(processed_result_data, ensure_ascii=False, default=str) if processed_result_data else None)
            param_idx += 1
        
        if status is not None:
            updates.append(f"status = ${param_idx}")
            params.append(status)
            param_idx += 1
        
        if error_message is not None:
            updates.append(f"error_message = ${param_idx}")
            params.append(error_message)
            param_idx += 1
        
        if metadata is not None:
            updates.append(f"metadata = ${param_idx}::jsonb")
            params.append(json.dumps(metadata, ensure_ascii=False) if metadata else '{}')
            param_idx += 1
        
        if not updates:
            return False
        
        params.append(message_id)
        query = f"""
            UPDATE conversation_messages
            SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
            WHERE message_id = ${param_idx}
        """
        
        result = await self.db.execute(query, *params)
        updated = result.split()[-1] != '0'
        
        if updated:
            logger.debug("消息已更新", message_id=str(message_id))
        
        return updated

    async def add_message(
        self,
        conversation_id: UUID,
        role: str,
        content: str,
        query_id: Optional[UUID] = None,
        sql_text: Optional[str] = None,
        result_summary: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
        status: str = 'completed',
        error_message: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
        context_message_ids: Optional[List[UUID]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """添加消息到会话"""
        # 确保 content 不为 None（数据库约束要求 NOT NULL）
        if content is None:
            content = ''
        
        # 处理结果数据（限制行数）
        processed_result_data = None
        if result_data and settings.conversation_save_full_result:
            processed_result_data = self._limit_result_rows(result_data)
        
        row = await self.db.fetchrow("""
            INSERT INTO conversation_messages (
                conversation_id, role, content, query_id, sql_text,
                result_summary, result_data, status, error_message,
                query_params, context_message_ids, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING *
        """,
            conversation_id, role, content, query_id, sql_text,
            result_summary,
            json.dumps(processed_result_data, ensure_ascii=False, default=str) if processed_result_data else None,
            status, error_message,
            json.dumps(query_params, ensure_ascii=False) if query_params else None,
            context_message_ids,
            json.dumps(metadata, ensure_ascii=False) if metadata else '{}'
        )
        
        logger.debug("消息已添加", message_id=str(row['message_id']), conversation_id=str(conversation_id), role=role)
        return self._message_row_to_dict(row)
    
    async def get_messages(
        self,
        conversation_id: UUID,
        limit: int = 100,
        offset: int = 0,
        include_result_data: bool = True
    ) -> List[Dict[str, Any]]:
        """获取会话的消息列表"""
        if include_result_data:
            rows = await self.db.fetch("""
                SELECT * FROM conversation_messages
                WHERE conversation_id = $1
                ORDER BY created_at ASC
                LIMIT $2 OFFSET $3
            """, conversation_id, limit, offset)
        else:
            rows = await self.db.fetch("""
                SELECT message_id, conversation_id, role, content, query_id, sql_text,
                       result_summary, status, error_message, query_params,
                       context_message_ids, metadata, created_at, updated_at
                FROM conversation_messages
                WHERE conversation_id = $1
                ORDER BY created_at ASC
                LIMIT $2 OFFSET $3
            """, conversation_id, limit, offset)
        
        return [self._message_row_to_dict(row) for row in rows]
    
    async def get_recent_context(
        self,
        conversation_id: UUID,
        depth: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """获取最近的对话上下文（用于多轮对话）"""
        context_depth = depth if depth is not None else settings.conversation_context_depth
        if context_depth <= 0:
            return []
        
        # 获取最近的 N 轮对话（每轮包含 user + assistant）
        rows = await self.db.fetch("""
            SELECT message_id, role, content, query_id, sql_text, result_summary, result_data, status
            FROM conversation_messages
            WHERE conversation_id = $1 AND status = 'completed'
            ORDER BY created_at DESC
            LIMIT $2
        """, conversation_id, context_depth * 2)
        
        # 反转顺序，使其按时间正序排列
        return [self._message_row_to_dict(row) for row in reversed(rows)]
    
    async def update_message_status(
        self,
        message_id: UUID,
        status: str,
        error_message: Optional[str] = None,
        result_summary: Optional[str] = None,
        result_data: Optional[Dict[str, Any]] = None,
        sql_text: Optional[str] = None
    ) -> bool:
        """更新消息状态"""
        updates = ["status = $2"]
        params = [message_id, status]
        param_idx = 3
        
        if error_message is not None:
            updates.append(f"error_message = ${param_idx}")
            params.append(error_message)
            param_idx += 1
        
        if result_summary is not None:
            updates.append(f"result_summary = ${param_idx}")
            params.append(result_summary)
            param_idx += 1
        
        if result_data is not None:
            processed_result_data = self._limit_result_rows(result_data) if settings.conversation_save_full_result else None
            updates.append(f"result_data = ${param_idx}")
            params.append(json.dumps(processed_result_data, ensure_ascii=False, default=str) if processed_result_data else None)
            param_idx += 1
        
        if sql_text is not None:
            updates.append(f"sql_text = ${param_idx}")
            params.append(sql_text)
            param_idx += 1
        
        query = f"""
            UPDATE conversation_messages
            SET {', '.join(updates)}
            WHERE message_id = $1
        """
        
        result = await self.db.execute(query, *params)
        return result.split()[-1] != '0'
    
    async def get_message(self, message_id: UUID) -> Optional[Dict[str, Any]]:
        """获取单条消息"""
        row = await self.db.fetchrow("""
            SELECT * FROM conversation_messages WHERE message_id = $1
        """, message_id)
        return self._message_row_to_dict(row) if row else None
    
    # ==================== 会话标题自动生成 ====================
    
    async def auto_generate_title(
        self,
        conversation_id: UUID,
        first_message: str
    ) -> str:
        """根据第一条消息自动生成会话标题"""
        max_length = settings.conversation_title_max_length
        
        # 截取前 N 个字符作为标题
        title = first_message.strip()
        if len(title) > max_length:
            title = title[:max_length - 3] + "..."
        
        # 更新会话标题
        await self.db.execute("""
            UPDATE conversations SET title = $2 WHERE conversation_id = $1 AND title IS NULL
        """, conversation_id, title)
        
        return title
    
    # ==================== 辅助方法 ====================
    
    def _row_to_dict(self, row: asyncpg.Record) -> Dict[str, Any]:
        """将数据库行转换为字典"""
        if not row:
            return {}
        result = dict(row)
        # 转换 UUID 为字符串
        for key in ['conversation_id', 'user_id', 'connection_id', 'domain_id']:
            if key in result and result[key]:
                result[key] = str(result[key])
        # 转换时间戳为 ISO 格式
        for key in ['created_at', 'updated_at', 'last_message_at']:
            if key in result and result[key]:
                result[key] = result[key].isoformat()
        return result
    
    def _message_row_to_dict(self, row: asyncpg.Record) -> Dict[str, Any]:
        """将消息行转换为字典"""
        if not row:
            return {}
        result = dict(row)
        # 转换 UUID 为字符串
        for key in ['message_id', 'conversation_id', 'query_id']:
            if key in result and result[key]:
                result[key] = str(result[key])
        # 转换 UUID 数组
        if 'context_message_ids' in result and result['context_message_ids']:
            result['context_message_ids'] = [str(uid) for uid in result['context_message_ids']]
        # 解析 JSON 字段
        for key in ['result_data', 'query_params', 'metadata']:
            if key in result and result[key]:
                if isinstance(result[key], str):
                    try:
                        result[key] = json.loads(result[key])
                    except json.JSONDecodeError:
                        pass
        # 转换时间戳为 ISO 格式
        for key in ['created_at', 'updated_at']:
            if key in result and result[key]:
                result[key] = result[key].isoformat()
        return result
    
    def _limit_result_rows(self, result_data: Dict[str, Any]) -> Dict[str, Any]:
        """限制结果数据的行数"""
        if not result_data:
            return result_data
        
        max_rows = settings.conversation_max_result_rows
        if max_rows <= 0:
            return result_data
        
        result = dict(result_data)
        if 'rows' in result and isinstance(result['rows'], list):
            original_count = len(result['rows'])
            if original_count > max_rows:
                result['rows'] = result['rows'][:max_rows]
                result['_truncated'] = True
                result['_original_row_count'] = original_count
        
        return result


class ActiveQueryRegistry:
    """活跃查询注册表 - 用于追踪和取消正在执行的查询"""
    
    def __init__(self, db_conn: asyncpg.Connection):
        self.db = db_conn
    
    async def register_query(
        self,
        query_id: UUID,
        user_id: UUID,
        query_text: str,
        conversation_id: Optional[UUID] = None,
        message_id: Optional[UUID] = None,
        connection_id: Optional[UUID] = None,
        ws_connection_id: Optional[str] = None
    ) -> bool:
        """注册活跃查询"""
        try:
            await self.db.execute("""
                INSERT INTO active_queries (
                    query_id, conversation_id, message_id, user_id,
                    query_text, connection_id, status, ws_connection_id
                ) VALUES ($1, $2, $3, $4, $5, $6, 'running', $7)
                ON CONFLICT (query_id) DO UPDATE SET
                    status = 'running',
                    started_at = CURRENT_TIMESTAMP,
                    cancelled_at = NULL,
                    completed_at = NULL
            """, query_id, conversation_id, message_id, user_id, query_text, connection_id, ws_connection_id)
            logger.debug("查询已注册", query_id=str(query_id))
            return True
        except Exception as e:
            logger.warning("注册查询失败", query_id=str(query_id), error=str(e))
            return False
    
    async def mark_cancelling(self, query_id: UUID, user_id: UUID) -> bool:
        """标记查询为取消中状态"""
        result = await self.db.execute("""
            UPDATE active_queries
            SET status = 'cancelling', cancelled_at = CURRENT_TIMESTAMP
            WHERE query_id = $1 AND user_id = $2 AND status = 'running'
        """, query_id, user_id)
        
        updated = result.split()[-1] != '0'
        if updated:
            logger.info("查询已标记为取消中", query_id=str(query_id))
        return updated
    
    async def mark_cancelled(self, query_id: UUID) -> bool:
        """标记查询为已取消"""
        result = await self.db.execute("""
            UPDATE active_queries
            SET status = 'cancelled'
            WHERE query_id = $1 AND status IN ('running', 'cancelling')
        """, query_id)
        return result.split()[-1] != '0'
    
    async def mark_completed(self, query_id: UUID) -> bool:
        """标记查询为已完成"""
        result = await self.db.execute("""
            UPDATE active_queries
            SET status = 'completed', completed_at = CURRENT_TIMESTAMP
            WHERE query_id = $1 AND status = 'running'
        """, query_id)
        return result.split()[-1] != '0'
    
    async def is_cancelled(self, query_id: UUID) -> bool:
        """检查查询是否被取消"""
        status = await self.db.fetchval("""
            SELECT status FROM active_queries WHERE query_id = $1
        """, query_id)
        return status in ('cancelling', 'cancelled')
    
    async def get_query_status(self, query_id: UUID) -> Optional[str]:
        """获取查询状态"""
        return await self.db.fetchval("""
            SELECT status FROM active_queries WHERE query_id = $1
        """, query_id)
    
    async def get_user_running_queries(self, user_id: UUID) -> List[Dict[str, Any]]:
        """获取用户正在执行的查询"""
        rows = await self.db.fetch("""
            SELECT query_id, conversation_id, message_id, query_text, started_at
            FROM active_queries
            WHERE user_id = $1 AND status = 'running'
            ORDER BY started_at DESC
        """, user_id)
        return [dict(row) for row in rows]
    
    async def cleanup_stale_queries(self, timeout_seconds: int = 3600) -> int:
        """清理过期的查询记录"""
        result = await self.db.execute("""
            DELETE FROM active_queries
            WHERE status IN ('completed', 'cancelled')
              AND (completed_at < NOW() - INTERVAL '%s seconds'
                   OR cancelled_at < NOW() - INTERVAL '%s seconds')
        """ % (timeout_seconds, timeout_seconds))
        return int(result.split()[-1])
