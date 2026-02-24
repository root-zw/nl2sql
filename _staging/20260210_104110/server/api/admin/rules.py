"""
全局规则管理API
派生指标、单位转换、校验规则、同义词映射
"""

from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Optional, Dict, Any
import json
from uuid import UUID
import asyncpg
import structlog
logger = structlog.get_logger()

from server.models.database import (
    GlobalRuleCreate,
    GlobalRuleUpdate,
    GlobalRuleResponse,
    RuleType
)
from server.middleware.auth import require_data_admin
from server.models.admin import User as AdminUser

router = APIRouter()


async def get_db_pool():
    """获取数据库连接池"""
    from server.config import settings
    conn = await asyncpg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        database=settings.postgres_db
    )
    try:
        yield conn
    finally:
        await conn.close()


# ============================================================================
# 全局规则CRUD
# ============================================================================

@router.get("/rules", response_model=List[GlobalRuleResponse])
async def list_rules(
    connection_id: Optional[UUID] = None,
    rule_type: Optional[RuleType] = None,
    is_active: Optional[bool] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """获取全局规则列表"""
    try:
        where_clause = "WHERE 1=1"
        params = []
        
        if connection_id:
            where_clause += f" AND connection_id = ${len(params) + 1}"
            params.append(connection_id)
        
        if rule_type:
            where_clause += f" AND rule_type = ${len(params) + 1}"
            params.append(rule_type)
        
        if is_active is not None:
            where_clause += f" AND is_active = ${len(params) + 1}"
            params.append(is_active)
        
        query = f"""
            SELECT 
                gr.rule_id, gr.connection_id, gr.rule_type, gr.rule_name,
                gr.description, gr.rule_definition, gr.scope, gr.domain_id, gr.domain_ids,
                gr.priority, gr.is_active, gr.created_at,
                dc.connection_name
            FROM global_rules gr
            LEFT JOIN database_connections dc ON gr.connection_id = dc.connection_id
            {where_clause.replace('connection_id', 'gr.connection_id').replace('rule_type', 'gr.rule_type').replace('is_active', 'gr.is_active')}
            ORDER BY gr.rule_type, gr.priority DESC, gr.rule_name
        """
        
        rows = await db.fetch(query, *params)
        
        def _parse_rule_def(val):
            # 兼容不同驱动返回：可能是dict或json字符串
            if isinstance(val, (dict, list)):
                return val
            try:
                return json.loads(val)
            except Exception:
                return {}

        return [
            GlobalRuleResponse(
                rule_id=row['rule_id'],
                connection_id=row['connection_id'],
                connection_name=row['connection_name'],  # 数据源名称
                rule_type=row['rule_type'],
                rule_name=row['rule_name'],
                description=row['description'],
                rule_definition=_parse_rule_def(row['rule_definition']),
                scope=row['scope'] or 'global',
                domain_id=row['domain_id'],
                domain_ids=row['domain_ids'],
                priority=row['priority'],
                is_active=row['is_active'],
                created_at=row['created_at']
            )
            for row in rows
        ]
    
    except Exception as e:
        logger.exception("获取全局规则列表失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取全局规则列表失败: {str(e)}"
        )


@router.get("/rules/{rule_id}", response_model=GlobalRuleResponse)
async def get_rule(
    rule_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """获取单个全局规则详情"""
    try:
        row = await db.fetchrow("""
            SELECT 
                rule_id, connection_id, rule_type, rule_name,
                description, rule_definition, scope, domain_id, domain_ids,
                priority, is_active, created_at
            FROM global_rules
            WHERE rule_id = $1
        """, rule_id)
        
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"全局规则 {rule_id} 不存在"
            )
        
        def _parse_rule_def(val):
            if isinstance(val, (dict, list)):
                return val
            try:
                return json.loads(val)
            except Exception:
                return {}

        return GlobalRuleResponse(
            rule_id=row['rule_id'],
            connection_id=row['connection_id'],
            rule_type=row['rule_type'],
            rule_name=row['rule_name'],
            description=row['description'],
            rule_definition=_parse_rule_def(row['rule_definition']),
            scope=row['scope'] or 'global',
            domain_id=row['domain_id'],
            domain_ids=row['domain_ids'],
            priority=row['priority'],
            is_active=row['is_active'],
            created_at=row['created_at']
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("获取全局规则详情失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取全局规则详情失败: {str(e)}"
        )


@router.post("/rules", response_model=GlobalRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_rule(
    rule: GlobalRuleCreate,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """创建全局规则"""
    try:
        # 验证rule_definition格式
        validate_rule_definition(rule.rule_type, rule.rule_definition)
        
        # 处理业务域：支持新的 domain_ids 或兼容旧的 domain_id
        domain_ids_value = rule.domain_ids
        domain_id_value = rule.domain_id
        
        # 如果只提供了 domain_id，转换为 domain_ids 数组
        if domain_id_value and not domain_ids_value:
            domain_ids_value = [domain_id_value]
        
        # 插入规则
        # 兼容旧版驱动：将rule_definition显式序列化为JSON字符串
        rule_definition_json = json.dumps(rule.rule_definition, ensure_ascii=False)
        row = await db.fetchrow("""
            INSERT INTO global_rules (
                connection_id, rule_type, rule_name, description,
                rule_definition, scope, domain_id, domain_ids, priority, is_active
            )
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, TRUE)
            RETURNING rule_id, connection_id, rule_type, rule_name,
                      description, rule_definition, scope, domain_id, domain_ids,
                      priority, is_active, created_at
        """,
            rule.connection_id,
            rule.rule_type,
            rule.rule_name,
            rule.description,
            rule_definition_json,
            rule.scope,
            domain_id_value,
            domain_ids_value,
            rule.priority
        )
        
        logger.info(f"创建全局规则成功: {rule.rule_name} ({rule.rule_type}), 业务域: {domain_ids_value}")
        
        def _parse_rule_def(val):
            if isinstance(val, (dict, list)):
                return val
            try:
                return json.loads(val)
            except Exception:
                return {}

        return GlobalRuleResponse(
            rule_id=row['rule_id'],
            connection_id=row['connection_id'],
            rule_type=row['rule_type'],
            rule_name=row['rule_name'],
            description=row['description'],
            rule_definition=_parse_rule_def(row['rule_definition']),
            scope=row['scope'] or 'global',
            domain_id=row['domain_id'],
            domain_ids=row['domain_ids'],
            priority=row['priority'],
            is_active=row['is_active'],
            created_at=row['created_at']
        )
    
    except asyncpg.UniqueViolationError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"规则名称 '{rule.rule_name}' 在该类型下已存在"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("创建全局规则失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建全局规则失败: {str(e)}"
        )


@router.put("/rules/{rule_id}", response_model=GlobalRuleResponse)
async def update_rule(
    rule_id: UUID,
    rule: GlobalRuleUpdate,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """更新全局规则"""
    try:
        # 检查是否存在
        existing = await db.fetchrow(
            "SELECT rule_type FROM global_rules WHERE rule_id = $1",
            rule_id
        )
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"全局规则 {rule_id} 不存在"
            )
        
        # 构建更新语句
        updates = []
        params = []
        param_index = 1
        
        update_fields = rule.dict(exclude_unset=True)
        
        # 如果更新rule_definition，需要验证
        if 'rule_definition' in update_fields:
            validate_rule_definition(
                existing['rule_type'],
                update_fields['rule_definition']
            )
        
        for field, value in update_fields.items():
            if field == 'rule_definition':
                # 显式序列化并强制为jsonb
                value = json.dumps(value, ensure_ascii=False)
                updates.append(f"{field} = ${param_index}::jsonb")
            else:
                updates.append(f"{field} = ${param_index}")
            params.append(value)
            param_index += 1
        
        if not updates:
            # 没有更新，返回当前数据
            return await get_rule(rule_id, db)
        
        params.append(rule_id)
        
        # 如果更新了 domain_id，同时更新 domain_ids
        if 'domain_id' in update_fields and update_fields['domain_id']:
            if 'domain_ids' not in update_fields:
                updates.append(f"domain_ids = ${param_index}")
                params.append([update_fields['domain_id']])
                param_index += 1
        
        query = f"""
            UPDATE global_rules
            SET {', '.join(updates)}, updated_at = NOW()
            WHERE rule_id = ${param_index}
            RETURNING rule_id, connection_id, rule_type, rule_name,
                      description, rule_definition, scope, domain_id, domain_ids,
                      priority, is_active, created_at
        """
        
        row = await db.fetchrow(query, *params)
        
        logger.info(f"更新全局规则成功: {rule_id}")
        
        def _parse_rule_def(val):
            if isinstance(val, (dict, list)):
                return val
            try:
                return json.loads(val)
            except Exception:
                return {}

        return GlobalRuleResponse(
            rule_id=row['rule_id'],
            connection_id=row['connection_id'],
            rule_type=row['rule_type'],
            rule_name=row['rule_name'],
            description=row['description'],
            rule_definition=_parse_rule_def(row['rule_definition']),
            scope=row['scope'] or 'global',
            domain_id=row['domain_id'],
            domain_ids=row['domain_ids'],
            priority=row['priority'],
            is_active=row['is_active'],
            created_at=row['created_at']
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("更新全局规则失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新全局规则失败: {str(e)}"
        )


@router.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_rule(
    rule_id: UUID,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """删除全局规则"""
    try:
        result = await db.execute(
            "DELETE FROM global_rules WHERE rule_id = $1",
            rule_id
        )
        
        if result == "DELETE 0":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"全局规则 {rule_id} 不存在"
            )
        
        logger.info(f"删除全局规则成功: {rule_id}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("删除全局规则失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除全局规则失败: {str(e)}"
        )


# ============================================================================
# 规则模板与示例
# ============================================================================

@router.get("/rules/templates")
async def get_rule_templates():
    """
    获取规则模板
    """
    templates = {
        "derived_metric": {
            "name": "派生指标",
            "description": "基于基础指标计算得出的指标",
            "example": {
                "formula": "SUM(收入) - SUM(成本)",
                "dependencies": ["field_id_1", "field_id_2"],
                "display_name": "利润",
                "unit": "元",
                "description": "收入减去成本"
            },
            "required_fields": ["formula", "dependencies", "display_name"]
        },
        "unit_conversion": {
            "name": "单位转换",
            "description": "在不同单位之间转换",
            "example": {
                "from_unit": "元",
                "to_unit": "万元",
                "conversion_factor": 0.0001,
                "description": "元转万元"
            },
            "required_fields": ["from_unit", "to_unit", "conversion_factor"]
        },
        "validation": {
            "name": "校验规则",
            "description": "数据有效性校验",
            "example": {
                "field_id": "field_xxx",
                "rule_expression": "value >= 0",
                "error_message": "金额不能为负",
                "severity": "error"
            },
            "required_fields": ["field_id", "rule_expression", "error_message"]
        },
        "synonym_mapping": {
            "name": "同义词映射",
            "description": "字段或值的同义词映射",
            "example": {
                "field_id": "field_xxx",
                "synonyms": {
                    "收入": ["营收", "销售额", "revenue"],
                    "成本": ["花费", "开支", "cost"]
                }
            },
            "required_fields": ["field_id", "synonyms"]
        }
    }
    
    return {
        "success": True,
        "templates": templates
    }


@router.post("/rules/validate")
async def validate_rule(
    rule_type: RuleType,
    rule_definition: Dict[str, Any]
):
    """
    验证规则定义是否合法
    """
    try:
        validate_rule_definition(rule_type, rule_definition)
        
        return {
            "success": True,
            "message": "规则定义合法"
        }
    
    except HTTPException as e:
        return {
            "success": False,
            "message": e.detail
        }
    except Exception as e:
        return {
            "success": False,
            "message": f"验证失败: {str(e)}"
        }


# ============================================================================
# 辅助函数
# ============================================================================

def validate_rule_definition(rule_type: str, rule_definition: Dict[str, Any]):
    """
    验证规则定义格式
    
    Args:
        rule_type: 规则类型
        rule_definition: 规则定义（JSON）
    
    Raises:
        HTTPException: 如果定义不合法
    """
    if rule_type == "derived_metric":
        # 派生指标
        required_fields = ["display_name", "formula", "field_dependencies"]
        for field in required_fields:
            if field not in rule_definition:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"派生指标缺少必填字段: {field}"
                )
        
        # 验证field_dependencies是列表
        if not isinstance(rule_definition["field_dependencies"], list):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="field_dependencies必须是列表"
            )
    
    elif rule_type == "default_filter":
        # 默认过滤
        required_fields = ["table_id", "filter_field", "filter_operator", "filter_value"]
        for field in required_fields:
            if field not in rule_definition:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"默认过滤缺少必填字段: {field}"
                )
        
        # 验证filter_operator
        valid_operators = ["=", "!=", ">", "<", ">=", "<=", "IN", "NOT IN", "LIKE", "IS NULL", "IS NOT NULL"]
        if rule_definition["filter_operator"] not in valid_operators:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"filter_operator必须是以下之一: {', '.join(valid_operators)}"
            )
    
    elif rule_type == "custom_instruction":
        # 自定义规则
        required_fields = ["instruction"]
        for field in required_fields:
            if field not in rule_definition:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"自定义规则缺少必填字段: {field}"
                )
        
        # 验证instruction非空
        if not rule_definition["instruction"].strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="instruction不能为空"
            )


@router.get("/rules/apply/{connection_id}")
async def get_applied_rules(
    connection_id: UUID,
    query: Optional[str] = None,
    current_user: AdminUser = Depends(require_data_admin),
    db = Depends(get_db_pool)
):
    """
    获取应用到查询的规则
    
    Args:
        connection_id: 数据库连接ID
        query: 用户查询（可选）
    
    Returns:
        应用的规则列表
    """
    try:
        # 获取所有激活的规则
        rules = await db.fetch("""
            SELECT 
                rule_id, rule_type, rule_name, rule_definition, priority
            FROM global_rules
            WHERE connection_id = $1 AND is_active = TRUE
            ORDER BY priority DESC, rule_name
        """, connection_id)
        
        applied_rules = []
        
        for rule in rules:
            # 根据规则类型应用逻辑
            # 这里简化处理，实际应用中需要更复杂的匹配逻辑
            
            if query and rule['rule_type'] == 'synonym_mapping':
                # 检查查询中是否包含同义词
                synonyms = rule['rule_definition'].get('synonyms', {})
                for key, syn_list in synonyms.items():
                    if any(syn.lower() in query.lower() for syn in syn_list):
                        applied_rules.append({
                            "rule_id": str(rule['rule_id']),
                            "rule_name": rule['rule_name'],
                            "rule_type": rule['rule_type'],
                            "matched": True,
                            "matched_term": key
                        })
                        break
            else:
                applied_rules.append({
                    "rule_id": str(rule['rule_id']),
                    "rule_name": rule['rule_name'],
                    "rule_type": rule['rule_type'],
                    "matched": False
                })
        
        return {
            "success": True,
            "total_rules": len(rules),
            "applied_rules": applied_rules
        }
    
    except Exception as e:
        logger.exception("获取应用规则失败")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取应用规则失败: {str(e)}"
        )

