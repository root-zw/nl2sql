"""SHOWPLAN 成本预估"""

from typing import Dict, Any, List
import xml.etree.ElementTree as ET
import structlog
from sqlalchemy.ext.asyncio import AsyncConnection

from server.config import settings

logger = structlog.get_logger()


class ShowPlanGuard:
    """SHOWPLAN 守护 - 在执行前评估查询成本（异步版本）"""
    
    def __init__(
        self,
        connection: AsyncConnection,
        max_rows: int = None,
        max_cost: float = None
    ):
        self.conn = connection
        self.max_rows = max_rows or settings.showplan_rows_max
        self.max_cost = max_cost or settings.showplan_cost_max
    
    async def estimate_cost(self, sql: str) -> Dict[str, Any]:
        """
        评估 SQL 成本
        
        Args:
            sql: 要执行的 SQL
        
        Returns:
            {
                "estimated_rows": int,
                "estimated_cost": float,
                "plan_xml": str,
                "safe_to_execute": bool,
                "warnings": List[str]
            }
        
        Raises:
            无
        """
        if not settings.showplan_enabled:
            # 跳过成本检查
            return {
                "estimated_rows": 0,
                "estimated_cost": 0.0,
                "plan_xml": "",
                "safe_to_execute": True,
                "warnings": []
            }
        
        logger.debug("开始 SHOWPLAN 成本评估")
        
        try:
            from sqlalchemy import text
            
            # 1. 开启 SHOWPLAN_XML
            await self.conn.execute(text("SET SHOWPLAN_XML ON"))
            
            # 2. 获取执行计划（不会真正执行）
            result = await self.conn.execute(text(sql))
            plan_xml_row = result.fetchone()
            plan_xml = plan_xml_row[0] if plan_xml_row else ""
            
        except Exception as e:
            logger.error("获取 SHOWPLAN 失败", error=str(e))
            # 失败时返回安全估算
            return {
                "estimated_rows": 0,
                "estimated_cost": 0.0,
                "plan_xml": "",
                "safe_to_execute": True,
                "warnings": [f"无法获取执行计划: {str(e)}"]
            }
        
        finally:
            # 3. 关闭 SHOWPLAN_XML
            try:
                from sqlalchemy import text
                await self.conn.execute(text("SET SHOWPLAN_XML OFF"))
            except Exception:
                pass
        
        # 4. 解析 XML
        try:
            root = ET.fromstring(plan_xml)
            estimated_rows, estimated_cost = self._parse_plan_xml(root)
        except Exception as e:
            logger.error("解析 SHOWPLAN XML 失败", error=str(e))
            return {
                "estimated_rows": 0,
                "estimated_cost": 0.0,
                "plan_xml": plan_xml,
                "safe_to_execute": True,
                "warnings": [f"无法解析执行计划: {str(e)}"]
            }
        
        # 5. 判断是否安全
        warnings = []
        safe = True
        
        if estimated_rows > self.max_rows:
            warnings.append(
                f"预估扫描 {estimated_rows:,.0f} 行，超过阈值 {self.max_rows:,}"
            )
            safe = False
        
        if estimated_cost > self.max_cost:
            warnings.append(
                f"预估成本 {estimated_cost:.2f}，超过阈值 {self.max_cost}"
            )
            safe = False
        
        # 检查是否有表扫描
        table_scans = self._detect_table_scans(root)
        if table_scans:
            warnings.append(f"检测到 {len(table_scans)} 个全表扫描: {', '.join(table_scans)}")
        
        logger.debug(
            "成本评估完成",
            estimated_rows=estimated_rows,
            estimated_cost=estimated_cost,
            safe=safe
        )
        
        return {
            "estimated_rows": int(estimated_rows),
            "estimated_cost": estimated_cost,
            "plan_xml": plan_xml,
            "safe_to_execute": safe,
            "warnings": warnings
        }
    
    def _parse_plan_xml(self, root: ET.Element) -> tuple:
        """
        解析 SHOWPLAN XML，提取预估行数和成本
        
        Returns:
            (estimated_rows, estimated_cost)
        """
        # 查找 StmtSimple 节点
        stmt = root.find(".//{*}StmtSimple")
        
        if stmt is not None:
            estimated_rows = float(stmt.get("StatementEstRows", 0))
            estimated_cost = float(stmt.get("StatementSubTreeCost", 0))
            return estimated_rows, estimated_cost
        
        return 0.0, 0.0
    
    def _detect_table_scans(self, root: ET.Element) -> List[str]:
        """检测全表扫描"""
        table_scans = []
        
        # 查找 PhysicalOp="Table Scan" 的节点
        for relop in root.findall(".//{*}RelOp[@PhysicalOp='Table Scan']"):
            # 尝试获取表名
            table_node = relop.find(".//{*}Object")
            if table_node is not None:
                table_name = table_node.get("Table", "unknown")
                table_scans.append(table_name)
        
        return table_scans

