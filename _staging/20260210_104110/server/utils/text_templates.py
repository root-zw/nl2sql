"""文本模板和配置加载器"""

import json
from pathlib import Path
from typing import Dict, List, Any
import structlog

from server.config import settings
from server.utils.prompt_loader import resolve_path

logger = structlog.get_logger()


# 配置文件路径
_DEFAULT_TEMPLATES_FILE = Path(__file__).parent.parent.parent / "prompts" / "common" / "text_templates.json"
TEMPLATES_FILE = resolve_path(settings.text_templates_file, _DEFAULT_TEMPLATES_FILE)


class TextTemplates:
    """文本模板单例管理器"""
    
    _instance = None
    _templates = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_templates()
        return cls._instance
    
    def _load_templates(self):
        """加载文本模板配置"""
        try:
            if TEMPLATES_FILE.exists():
                with open(TEMPLATES_FILE, 'r', encoding='utf-8') as f:
                    self._templates = json.load(f)
                logger.debug("文本模板加载成功", file=str(TEMPLATES_FILE))
            else:
                logger.warning("文本模板文件不存在，使用默认配置", file=str(TEMPLATES_FILE))
                self._templates = self._get_default_templates()
        except Exception as e:
            logger.error("加载文本模板失败，使用默认配置", error=str(e))
            self._templates = self._get_default_templates()
    
    def _get_default_templates(self) -> Dict[str, Any]:
        """获取默认模板（作为后备）"""
        return {
            "time_units": {
                "day": "天",
                "week": "周",
                "month": "月",
                "quarter": "季度",
                "year": "年"
            },
            "stopwords": [
                "的", "了", "是", "有", "在", "和", "与", "或", "等"
            ],
            "total_keywords": [
                "合计", "总计", "汇总", "小计", "总数"
            ],
            "explanation_templates": {},
            "narrative_default_prompt": ""
        }
    
    @property
    def time_units(self) -> Dict[str, str]:
        """时间单位映射"""
        return self._templates.get("time_units", {})
    
    @property
    def stopwords(self) -> List[str]:
        """停用词列表"""
        return self._templates.get("stopwords", [])
    
    @property
    def total_keywords(self) -> List[str]:
        """合计关键词列表"""
        return self._templates.get("total_keywords", [])
    
    @property
    def explanation_templates(self) -> Dict[str, str]:
        """说明文本模板"""
        return self._templates.get("explanation_templates", {})
    
    @property
    def narrative_default_prompt(self) -> str:
        """叙述生成的默认提示词"""
        return self._templates.get("narrative_default_prompt", "")
    
    def reload(self):
        """重新加载配置（用于热更新）"""
        self._load_templates()
        logger.debug("文本模板已重新加载")


# 全局实例
text_templates = TextTemplates()


# 便捷函数
def get_time_units() -> Dict[str, str]:
    """获取时间单位映射"""
    return text_templates.time_units


def get_stopwords() -> List[str]:
    """获取停用词列表"""
    return text_templates.stopwords


def get_total_keywords() -> List[str]:
    """获取合计关键词列表"""
    return text_templates.total_keywords


def get_explanation_templates() -> Dict[str, str]:
    """获取说明文本模板"""
    return text_templates.explanation_templates


def get_narrative_default_prompt() -> str:
    """获取叙述生成的默认提示词"""
    return text_templates.narrative_default_prompt
