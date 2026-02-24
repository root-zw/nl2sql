"""
表关系自动识别器
支持多种检测方法：外键约束、名称相似度、数据分析
"""

import re
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from difflib import SequenceMatcher
import structlog
logger = structlog.get_logger()


@dataclass
class RelationshipSuggestion:
    """表关系建议"""
    left_table_id: str
    left_column_id: str
    right_table_id: str
    right_column_id: str
    relationship_type: str  # 'one_to_one', 'one_to_many', 'many_to_many'
    detection_method: str  # 'foreign_key', 'name_similarity', 'data_analysis'
    confidence_score: float  # 0-1
    join_type: str = 'INNER'

    # 额外信息
    left_table_name: str = ""
    left_column_name: str = ""
    right_table_name: str = ""
    right_column_name: str = ""


class RelationshipDetector:
    """表关系检测器"""

    @staticmethod
    def calculate_name_similarity(name1: str, name2: str) -> float:
        """计算两个名称的相似度 (0-1)"""
        return SequenceMatcher(None, name1.lower(), name2.lower()).ratio()

    @staticmethod
    def is_potential_foreign_key(column_name: str) -> bool:
        """判断列名是否像外键"""
        column_lower = column_name.lower()

        # 常见外键模式
        patterns = [
            r'.*_id$',           # user_id, order_id
            r'.*id$',            # userid, orderid
            r'fk_.*',            # fk_user, fk_order
            r'.*_key$',          # user_key
            r'ref_.*',           # ref_user
        ]

        for pattern in patterns:
            if re.match(pattern, column_lower):
                return True

        return False

    @staticmethod
    def extract_table_name_from_column(column_name: str) -> Optional[str]:
        """从列名提取可能的表名"""
        column_lower = column_name.lower()

        # 移除常见后缀
        for suffix in ['_id', 'id', '_key', 'key', 'fk_', 'ref_']:
            if column_lower.endswith(suffix):
                extracted = column_lower[:-len(suffix)]
                if extracted:
                    return extracted
            if column_lower.startswith(suffix):
                extracted = column_lower[len(suffix):]
                if extracted:
                    return extracted

        return None

    @staticmethod
    def detect_by_foreign_keys(
        tables: List[Dict],
        columns: List[Dict]
    ) -> List[RelationshipSuggestion]:
        """
        方法1：基于外键约束检测

        Args:
            tables: 表列表
            columns: 列列表（包含is_foreign_key, referenced_table_id等）

        Returns:
            List of RelationshipSuggestion
        """
        suggestions = []

        # 创建表ID到表信息的映射
        table_map = {str(t['table_id']): t for t in tables}

        for col in columns:
            if not col.get('is_foreign_key'):
                continue

            # 已经有外键信息
            referenced_table_id = col.get('referenced_table_id')
            referenced_column_id = col.get('referenced_column_id')

            if not referenced_table_id or not referenced_column_id:
                continue

            left_table = table_map.get(str(col['table_id']))
            right_table = table_map.get(str(referenced_table_id))

            if not left_table or not right_table:
                continue

            suggestions.append(RelationshipSuggestion(
                left_table_id=str(col['table_id']),
                left_column_id=str(col['column_id']),
                right_table_id=str(referenced_table_id),
                right_column_id=str(referenced_column_id),
                relationship_type='one_to_many',  # 默认，可以后续分析
                detection_method='foreign_key',
                confidence_score=0.95,  # 外键约束置信度最高
                join_type='INNER',
                left_table_name=left_table['table_name'],
                left_column_name=col['column_name'],
                right_table_name=right_table['table_name'],
                right_column_name='id'  # 通常引用主键
            ))

        return suggestions

    @staticmethod
    def detect_by_name_similarity(
        tables: List[Dict],
        columns: List[Dict]
    ) -> List[RelationshipSuggestion]:
        """
        方法2：基于名称相似度检测

        逻辑：
        1. 找出所有可能是外键的列（名称包含_id等）
        2. 提取可能引用的表名
        3. 在其他表中查找主键或同名列
        4. 计算相似度
        """
        suggestions = []

        # 创建表ID到表信息的映射
        table_map = {str(t['table_id']): t for t in tables}

        # 按表分组列
        columns_by_table = {}
        for col in columns:
            table_id = str(col['table_id'])
            if table_id not in columns_by_table:
                columns_by_table[table_id] = []
            columns_by_table[table_id].append(col)

        # 找出所有主键
        primary_keys = {}
        for col in columns:
            if col.get('is_primary_key'):
                primary_keys[str(col['table_id'])] = col

        # 遍历所有列，查找潜在外键
        for col in columns:
            # 跳过已经识别为外键的
            if col.get('is_foreign_key'):
                continue

            # 跳过主键
            if col.get('is_primary_key'):
                continue

            # 判断是否像外键
            if not RelationshipDetector.is_potential_foreign_key(col['column_name']):
                continue

            # 提取可能的表名
            potential_table_name = RelationshipDetector.extract_table_name_from_column(
                col['column_name']
            )

            if not potential_table_name:
                continue

            # 在其他表中查找匹配
            left_table_id = str(col['table_id'])

            for other_table in tables:
                other_table_id = str(other_table['table_id'])

                # 跳过同一张表
                if other_table_id == left_table_id:
                    continue

                # 计算表名相似度
                table_name_similarity = RelationshipDetector.calculate_name_similarity(
                    potential_table_name,
                    other_table['table_name']
                )

                # 相似度阈值
                if table_name_similarity < 0.6:
                    continue

                # 查找该表的主键
                pk_col = primary_keys.get(other_table_id)

                if not pk_col:
                    # 没有主键，尝试找名为id的列
                    other_cols = columns_by_table.get(other_table_id, [])
                    pk_col = next(
                        (c for c in other_cols if c['column_name'].lower() in ['id', 'pk']),
                        None
                    )

                if not pk_col:
                    continue

                # 计算置信度
                confidence = table_name_similarity

                # 如果列名也相似，提高置信度
                col_name_similarity = RelationshipDetector.calculate_name_similarity(
                    col['column_name'],
                    f"{other_table['table_name']}_{pk_col['column_name']}"
                )

                confidence = (confidence + col_name_similarity) / 2

                # 置信度阈值
                if confidence < 0.5:
                    continue

                left_table = table_map.get(left_table_id)

                suggestions.append(RelationshipSuggestion(
                    left_table_id=left_table_id,
                    left_column_id=str(col['column_id']),
                    right_table_id=other_table_id,
                    right_column_id=str(pk_col['column_id']),
                    relationship_type='one_to_many',
                    detection_method='name_similarity',
                    confidence_score=min(confidence, 0.85),  # 名称相似度最高0.85
                    join_type='INNER',
                    left_table_name=left_table['table_name'],
                    left_column_name=col['column_name'],
                    right_table_name=other_table['table_name'],
                    right_column_name=pk_col['column_name']
                ))

        return suggestions

    @staticmethod
    def merge_suggestions(
        suggestions: List[RelationshipSuggestion]
    ) -> List[RelationshipSuggestion]:
        """
        合并重复的建议（同一对表列，多种方法检测到）
        保留置信度最高的
        """
        # 按 (left_table_id, left_column_id, right_table_id, right_column_id) 分组
        suggestion_map = {}

        for suggestion in suggestions:
            key = (
                suggestion.left_table_id,
                suggestion.left_column_id,
                suggestion.right_table_id,
                suggestion.right_column_id
            )

            if key not in suggestion_map:
                suggestion_map[key] = suggestion
            else:
                # 保留置信度更高的
                if suggestion.confidence_score > suggestion_map[key].confidence_score:
                    suggestion_map[key] = suggestion

        return list(suggestion_map.values())

    @staticmethod
    def detect_all(
        tables: List[Dict],
        columns: List[Dict]
    ) -> List[RelationshipSuggestion]:
        """
        综合检测（外键 + 名称相似度）
        """
        all_suggestions = []

        # 方法1：外键约束
        fk_suggestions = RelationshipDetector.detect_by_foreign_keys(tables, columns)
        all_suggestions.extend(fk_suggestions)

        logger.debug("外键约束检测完成", relationships=len(fk_suggestions))

        # 方法2：名称相似度
        name_suggestions = RelationshipDetector.detect_by_name_similarity(tables, columns)
        all_suggestions.extend(name_suggestions)

        logger.debug("名称相似度检测完成", relationships=len(name_suggestions))

        # 合并去重
        merged_suggestions = RelationshipDetector.merge_suggestions(all_suggestions)

        logger.debug("关系检测合并完成", unique_relationships=len(merged_suggestions))

        return merged_suggestions


def test_detector():
    """测试关系检测器"""
    # 模拟数据
    tables = [
        {'table_id': '1', 'table_name': 'users'},
        {'table_id': '2', 'table_name': 'orders'},
        {'table_id': '3', 'table_name': 'products'},
    ]

    columns = [
        # users表
        {'column_id': '1', 'table_id': '1', 'column_name': 'id', 'is_primary_key': True, 'is_foreign_key': False},
        {'column_id': '2', 'table_id': '1', 'column_name': 'username', 'is_primary_key': False, 'is_foreign_key': False},

        # orders表
        {'column_id': '3', 'table_id': '2', 'column_name': 'id', 'is_primary_key': True, 'is_foreign_key': False},
        {'column_id': '4', 'table_id': '2', 'column_name': 'user_id', 'is_primary_key': False, 'is_foreign_key': False},
        {'column_id': '5', 'table_id': '2', 'column_name': 'product_id', 'is_primary_key': False, 'is_foreign_key': False},

        # products表
        {'column_id': '6', 'table_id': '3', 'column_name': 'id', 'is_primary_key': True, 'is_foreign_key': False},
        {'column_id': '7', 'table_id': '3', 'column_name': 'product_name', 'is_primary_key': False, 'is_foreign_key': False},
    ]

    detector = RelationshipDetector()
    suggestions = detector.detect_all(tables, columns)

    print("\n 表关系检测结果\n" + "=" * 60)

    for s in suggestions:
        print(f" {s.left_table_name}.{s.left_column_name} → {s.right_table_name}.{s.right_column_name}")
        print(f"   方法: {s.detection_method}, 置信度: {s.confidence_score:.2f}, 类型: {s.relationship_type}")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    test_detector()

