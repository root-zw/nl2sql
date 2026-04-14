"""LLM 客户端封装（OpenAI 兼容）

支持多场景配置，每个场景可独立指定模型：
- table_selection: 表选择
- nl2ir: NL2IR 解析
- direct_sql: 直接 SQL 生成
- narrative: 叙述生成
- vector_selector: 向量表选择（LLM3）
- default: 默认配置

配置优先级：
1. 数据库中的场景模型配置（如果启用了 USE_METADATA_DB）
2. 环境变量配置
3. 默认配置
"""

from typing import Dict, List, Any, Optional, AsyncIterator, Literal
import json
import re
import asyncio
import structlog
from openai import AsyncOpenAI, APIConnectionError, APITimeoutError
import httpx

from server.config import settings
from server.exceptions import ParseError

logger = structlog.get_logger()

# LLM 使用场景
LLMScenario = Literal["default", "table_selection", "nl2ir", "direct_sql", "narrative", "vector_selector"]

# 缓存数据库配置（避免每次请求都查询数据库）
_db_config_cache: Dict[str, Dict[str, Any]] = {}
_db_config_cache_time: float = 0.0
_DB_CONFIG_CACHE_TTL = 60.0  # 缓存60秒


async def get_llm_config_for_scenario_from_db(scenario: LLMScenario) -> Optional[Dict[str, Any]]:
    """
    从数据库获取场景的 LLM 配置
    
    Args:
        scenario: 场景名称
        
    Returns:
        配置字典，如果数据库未配置则返回 None
    """
    import time
    global _db_config_cache, _db_config_cache_time
    
    # 检查缓存是否有效
    current_time = time.time()
    if current_time - _db_config_cache_time < _DB_CONFIG_CACHE_TTL and scenario in _db_config_cache:
        return _db_config_cache.get(scenario)
    
    try:
        from server.services.model_provider_service import get_model_provider_service
        from server.models.model_provider import LLMScenario as LLMScenarioEnum
        
        service = get_model_provider_service()
        
        # 将字符串转换为枚举
        scenario_enum = LLMScenarioEnum(scenario)
        
        # 从数据库获取配置
        config = await service.get_llm_config_for_scenario(scenario_enum)
        
        if config:
            _db_config_cache[scenario] = config
            _db_config_cache_time = current_time
            logger.debug("从数据库加载LLM配置", scenario=scenario, model=config.get("model"))
            return config
        
        return None
        
    except Exception as e:
        logger.warning("从数据库获取LLM配置失败，将使用环境变量配置", 
                      scenario=scenario, error=str(e))
        return None


def clear_llm_config_cache():
    """清除LLM配置缓存（配置更新后调用）"""
    global _db_config_cache, _db_config_cache_time
    _db_config_cache = {}
    _db_config_cache_time = 0.0
    logger.info("已清除LLM配置缓存")


def get_llm_config_for_scenario_from_env(scenario: LLMScenario = "default") -> Dict[str, Any]:
    """
    从环境变量获取指定场景的 LLM 配置
    
    优先使用场景特定配置，未配置时回退到默认配置
    
    Args:
        scenario: 场景名称
        
    Returns:
        配置字典，包含 base_url, api_key, model, temperature, max_tokens, timeout
    """
    # 默认配置
    config = {
        "base_url": settings.nl2sql_base_url,
        "api_key": settings.nl2sql_api_key,
        "model": settings.llm_model,
        "temperature": settings.llm_temperature,
        "max_tokens": settings.llm_max_tokens,
        "timeout": settings.llm_timeout,
        "max_retries": settings.llm_max_retries,
    }
    
    if scenario == "table_selection":
        # 表选择场景
        if settings.llm_table_selection_base_url:
            config["base_url"] = settings.llm_table_selection_base_url
        if settings.llm_table_selection_api_key:
            config["api_key"] = settings.llm_table_selection_api_key
        if settings.llm_table_selection_model:
            config["model"] = settings.llm_table_selection_model
        if settings.llm_table_selection_temperature is not None:
            config["temperature"] = settings.llm_table_selection_temperature
        if settings.llm_table_selection_max_tokens is not None:
            config["max_tokens"] = settings.llm_table_selection_max_tokens
        if settings.llm_table_selection_timeout is not None:
            config["timeout"] = settings.llm_table_selection_timeout
            
    elif scenario == "nl2ir":
        # NL2IR 解析场景
        if settings.llm_nl2ir_base_url:
            config["base_url"] = settings.llm_nl2ir_base_url
        if settings.llm_nl2ir_api_key:
            config["api_key"] = settings.llm_nl2ir_api_key
        if settings.llm_nl2ir_model:
            config["model"] = settings.llm_nl2ir_model
        if settings.llm_nl2ir_temperature is not None:
            config["temperature"] = settings.llm_nl2ir_temperature
        if settings.llm_nl2ir_max_tokens is not None:
            config["max_tokens"] = settings.llm_nl2ir_max_tokens
        if settings.llm_nl2ir_timeout is not None:
            config["timeout"] = settings.llm_nl2ir_timeout
            
    elif scenario == "narrative":
        # 叙述生成场景
        if settings.llm_narrative_base_url:
            config["base_url"] = settings.llm_narrative_base_url
        if settings.llm_narrative_api_key:
            config["api_key"] = settings.llm_narrative_api_key
        if settings.llm_narrative_model:
            config["model"] = settings.llm_narrative_model
        # 叙述场景使用 narrative_temperature
        config["temperature"] = settings.narrative_temperature
        if settings.llm_narrative_max_tokens is not None:
            config["max_tokens"] = settings.llm_narrative_max_tokens
        if settings.llm_narrative_timeout is not None:
            config["timeout"] = settings.llm_narrative_timeout

    elif scenario == "direct_sql":
        # 直接 SQL 生成场景
        if settings.llm_direct_sql_base_url:
            config["base_url"] = settings.llm_direct_sql_base_url
        if settings.llm_direct_sql_api_key:
            config["api_key"] = settings.llm_direct_sql_api_key
        if settings.llm_direct_sql_model:
            config["model"] = settings.llm_direct_sql_model
        if settings.llm_direct_sql_temperature is not None:
            config["temperature"] = settings.llm_direct_sql_temperature
        if settings.llm_direct_sql_max_tokens is not None:
            config["max_tokens"] = settings.llm_direct_sql_max_tokens
        if settings.llm_direct_sql_timeout is not None:
            config["timeout"] = settings.llm_direct_sql_timeout
            
    elif scenario == "vector_selector":
        # 向量表选择场景（LLM3）
        if settings.llm_vector_selector_base_url:
            config["base_url"] = settings.llm_vector_selector_base_url
        if settings.llm_vector_selector_api_key:
            config["api_key"] = settings.llm_vector_selector_api_key
        if settings.llm_vector_selector_model:
            config["model"] = settings.llm_vector_selector_model
        if settings.llm_vector_selector_temperature is not None:
            config["temperature"] = settings.llm_vector_selector_temperature
        if settings.llm_vector_selector_max_tokens is not None:
            config["max_tokens"] = settings.llm_vector_selector_max_tokens
        if settings.llm_vector_selector_timeout is not None:
            config["timeout"] = settings.llm_vector_selector_timeout
    
    return config


def get_llm_config_for_scenario(scenario: LLMScenario = "default") -> Dict[str, Any]:
    """
    获取指定场景的 LLM 配置（同步版本，用于初始化）
    
    此函数仅从环境变量获取配置，用于同步初始化场景。
    如需从数据库获取配置，请使用 get_llm_config_for_scenario_from_db 异步函数。
    
    Args:
        scenario: 场景名称
        
    Returns:
        配置字典
    """
    return get_llm_config_for_scenario_from_env(scenario)


class LLMClient:
    """LLM 客户端，支持 OpenAI 兼容接口
    
    支持多场景配置：
    - 通过 scenario 参数指定场景，自动加载对应配置
    - 也可以直接传入参数覆盖
    - 支持从数据库动态加载配置（需要调用 load_config_from_db 方法）
    
    配置优先级：
    1. 直接传入的参数
    2. 数据库场景配置（如果调用了 load_config_from_db）
    3. 环境变量场景配置
    4. 环境变量默认配置
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        scenario: LLMScenario = "default"
    ):
        """
        初始化 LLM 客户端
        
        Args:
            base_url: API 基础 URL（可选，覆盖配置）
            api_key: API 密钥（可选，覆盖配置）
            model: 模型名称（可选，覆盖配置）
            timeout: 超时时间（可选，覆盖配置）
            max_retries: 最大重试次数（可选，覆盖配置）
            scenario: 使用场景，自动加载对应配置
        """
        # 获取场景配置（从环境变量）
        config = get_llm_config_for_scenario_from_env(scenario)
        self.scenario = scenario
        
        # 保存传入的参数（用于覆盖）
        self._override_base_url = base_url
        self._override_api_key = api_key
        self._override_model = model
        self._override_timeout = timeout
        self._override_max_retries = max_retries
        
        # 是否已从数据库加载配置
        self._db_config_loaded = False
        self._db_config: Optional[Dict[str, Any]] = None
        
        # 参数优先级：传入参数 > 场景配置 > 默认配置
        self.primary_base_url = base_url or config["base_url"]
        self.alt_base_url = self._infer_dashscope_alt(self.primary_base_url)
        self.active_base_url = self.primary_base_url
        self.api_key = api_key or config["api_key"]
        self.model = model or config["model"]
        self.timeout = timeout if timeout is not None else config["timeout"]
        self.max_retries = max_retries if max_retries is not None else config["max_retries"]
        self._scenario_temperature = config["temperature"]
        self._scenario_max_tokens = config["max_tokens"]
        
        self.client = None
        self.client_base_url = None
        self._ensure_client(self.active_base_url)
        
        logger.debug(
            "LLM 客户端初始化",
            scenario=scenario,
            model=self.model,
            base_url=self.primary_base_url[:50] + "..." if self.primary_base_url and len(self.primary_base_url) > 50 else self.primary_base_url
        )
    
    async def load_config_from_db(self) -> bool:
        """
        从数据库加载配置（如果可用）
        
        此方法会尝试从数据库加载场景配置，如果成功则更新客户端参数。
        如果数据库未启用或未配置，则保持使用环境变量配置。
        
        Returns:
            是否成功从数据库加载了配置
        """
        if not settings.use_metadata_db:
            return False
        
        db_config = await get_llm_config_for_scenario_from_db(self.scenario)
        
        if not db_config:
            return False
        
        self._db_config = db_config
        self._db_config_loaded = True
        
        # 只在没有覆盖参数时使用数据库配置
        if not self._override_base_url and db_config.get("base_url"):
            self.primary_base_url = db_config["base_url"]
            self.alt_base_url = self._infer_dashscope_alt(self.primary_base_url)
            self.active_base_url = self.primary_base_url
        
        if not self._override_api_key and db_config.get("api_key"):
            self.api_key = db_config["api_key"]
        
        if not self._override_model and db_config.get("model"):
            self.model = db_config["model"]
        
        if self._override_timeout is None and db_config.get("timeout") is not None:
            self.timeout = db_config["timeout"]
        
        if self._override_max_retries is None and db_config.get("max_retries") is not None:
            self.max_retries = db_config["max_retries"]
        
        if db_config.get("temperature") is not None:
            self._scenario_temperature = db_config["temperature"]
        
        if db_config.get("max_tokens") is not None:
            self._scenario_max_tokens = db_config["max_tokens"]
        
        # 重新初始化客户端
        self.client = None
        self._ensure_client(self.active_base_url)
        
        logger.info(
            "从数据库加载LLM配置",
            scenario=self.scenario,
            model=self.model,
            base_url=self.primary_base_url[:50] + "..." if self.primary_base_url and len(self.primary_base_url) > 50 else self.primary_base_url
        )
        
        return True

    def _infer_dashscope_alt(self, url: Optional[str]) -> Optional[str]:
        """推断 DashScope 备用线路（已禁用）
        
        注意：阿里云国内版和国际版的 API Key 不互通，
        国内版 API Key 无法在 dashscope-intl.aliyuncs.com 上使用，
        因此禁用自动切换到国际版的逻辑。
        
        如果需要使用国际版，请直接配置 NL2SQL_BASE_URL 为国际版地址，
        并使用对应的国际版 API Key。
        """
        # 禁用国际版备用线路：国内/国际版 API Key 不互通，切换后会导致 401 错误
        return None

    def _ensure_client(self, base_url: Optional[str]) -> None:
        target_url = base_url or self.primary_base_url
        if self.client is None or self.client_base_url != target_url:
            self.client = AsyncOpenAI(
                base_url=target_url,
                api_key=self.api_key,
                timeout=self.timeout
            )
            self.client_base_url = target_url
            logger.info(
                "LLM 客户端初始化",
                base_url=target_url,
                model=self.model,
                use_tools=settings.llm_use_tools
            )

    @staticmethod
    def _is_transient_network_error(error: Exception) -> bool:
        transient_types = (APIConnectionError, APITimeoutError, httpx.TransportError)
        if isinstance(error, transient_types):
            return True
        message = str(error).lower()
        return any(
            hint in message
            for hint in [
                "timed out",
                "timeout",
                "connection reset",
                "connection aborted",
                "temporary failure in name resolution",
            ]
        )

    @staticmethod
    def _is_enable_thinking_rejected(error: Exception) -> bool:
        """识别网关/模型不支持 enable_thinking 的 400 错误。"""
        message = str(error).lower()
        return (
            "enable_thinking" in message
            and ("extra_forbidden" in message or "extra inputs are not permitted" in message)
        )

    @staticmethod
    def _remove_enable_thinking(request_params: Dict[str, Any]) -> bool:
        """从请求参数中剔除 enable_thinking，返回是否有实际移除。"""
        removed = False
        extra_body = request_params.get("extra_body")
        if isinstance(extra_body, dict):
            if "enable_thinking" in extra_body:
                extra_body.pop("enable_thinking", None)
                removed = True
            template_kwargs = extra_body.get("chat_template_kwargs")
            if isinstance(template_kwargs, dict) and "enable_thinking" in template_kwargs:
                template_kwargs.pop("enable_thinking", None)
                removed = True
                if not template_kwargs:
                    extra_body.pop("chat_template_kwargs", None)
            if not extra_body:
                request_params.pop("extra_body", None)
        return removed

    @staticmethod
    def _is_server_error(error: Exception) -> bool:
        """识别 5xx 服务端错误。"""
        message = str(error).lower()
        return bool(re.search(r"error code:\s*5\d\d", message))

    @staticmethod
    def _remove_tooling(request_params: Dict[str, Any]) -> bool:
        """移除 tools/tool_choice，返回是否有实际移除。"""
        removed = False
        if "tool_choice" in request_params:
            request_params.pop("tool_choice", None)
            removed = True
        if "tools" in request_params:
            request_params.pop("tools", None)
            removed = True
        return removed
    
    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        tool_choice: Optional[Dict] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> Dict[str, Any]:
        # 优先使用传入参数，其次使用场景配置，最后使用全局配置
        if temperature is None:
            temperature = self._scenario_temperature if self._scenario_temperature is not None else settings.llm_temperature

        request_params = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            **kwargs
        }

        if tools and settings.llm_use_tools:
            request_params["tools"] = tools
            if tool_choice:
                request_params["tool_choice"] = tool_choice

        # 优先使用场景配置的 max_tokens
        effective_max_tokens = self._scenario_max_tokens if self._scenario_max_tokens is not None else settings.llm_max_tokens
        if effective_max_tokens is not None:
            request_params["max_tokens"] = effective_max_tokens

        if settings.llm_top_p is not None:
            request_params["top_p"] = settings.llm_top_p

        extra_body = kwargs.pop("extra_body", None) or {}
        chat_template_kwargs = kwargs.pop("chat_template_kwargs", None) or {}

        existing_extra_body = request_params.pop("extra_body", None) or {}
        if existing_extra_body:
            extra_body = {**existing_extra_body, **extra_body}

        existing_template_kwargs = request_params.pop("chat_template_kwargs", None) or {}
        if existing_template_kwargs:
            chat_template_kwargs = {**existing_template_kwargs, **chat_template_kwargs}

        if settings.llm_enable_thinking is not None:
            chat_template_kwargs.setdefault("enable_thinking", settings.llm_enable_thinking)

        if chat_template_kwargs:
            existing_template_body = extra_body.get("chat_template_kwargs", {})
            if isinstance(existing_template_body, dict):
                extra_body["chat_template_kwargs"] = {
                    **existing_template_body,
                    **chat_template_kwargs,
                }
            else:
                extra_body["chat_template_kwargs"] = chat_template_kwargs
            logger.debug("已添加聊天模板参数", template_kwargs=extra_body["chat_template_kwargs"])

        if extra_body:
            request_params["extra_body"] = extra_body
            logger.debug("已添加额外参数", extra_params=extra_body)

        max_attempts = max(1, self.max_retries)
        last_error: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                self._ensure_client(self.active_base_url)
                logger.info(
                    "调用 LLM",
                    scenario=self.scenario,
                    model=self.model,
                    has_tools=bool(tools),
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
                response = await self.client.chat.completions.create(**request_params)

                usage = response.usage
                if usage:
                    logger.info(
                        "LLM 调用完成",
                        prompt_tokens=usage.prompt_tokens,
                        completion_tokens=usage.completion_tokens,
                        total_tokens=usage.total_tokens,
                    )

                return response.model_dump()
            except Exception as e:
                last_error = e
                if self._is_enable_thinking_rejected(e):
                    if self._remove_enable_thinking(request_params):
                        logger.warning(
                            "模型不支持 enable_thinking，已自动降级重试",
                            scenario=self.scenario,
                            model=self.model,
                        )
                        continue
                if self._is_server_error(e):
                    if self._remove_tooling(request_params):
                        logger.warning(
                            "检测到5xx且工具调用失败，已自动降级为无tools重试",
                            scenario=self.scenario,
                            model=self.model,
                        )
                        continue
                if (
                    self.alt_base_url
                    and self.active_base_url != self.alt_base_url
                    and self._is_transient_network_error(e)
                ):
                    logger.warning(
                        "DashScope 主域名调用失败，切换至 intl 备用线路",
                        previous_base_url=self.active_base_url,
                        alt_base_url=self.alt_base_url,
                        error=str(e),
                    )
                    self.active_base_url = self.alt_base_url
                    continue
                logger.warning(
                    "LLM 调用失败",
                    error=str(e),
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
                if attempt >= max_attempts:
                    break
                backoff = min(2 ** attempt, 10)
                await asyncio.sleep(backoff)

        logger.error("LLM 调用失败", error=str(last_error) if last_error else None)
        raise ParseError(f"LLM 调用失败: {str(last_error) if last_error else 'unknown error'}")

    async def chat_completion_stream(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
        tool_choice: Optional[Dict] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        以流式方式调用 LLM Chat Completion。
        """
        # 优先使用传入参数，其次使用场景配置，最后使用全局配置
        if temperature is None:
            temperature = self._scenario_temperature if self._scenario_temperature is not None else settings.llm_temperature

        request_params = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            **kwargs
        }

        if tools and settings.llm_use_tools:
            request_params["tools"] = tools
            if tool_choice:
                request_params["tool_choice"] = tool_choice

        # 优先使用场景配置的 max_tokens
        effective_max_tokens = self._scenario_max_tokens if self._scenario_max_tokens is not None else settings.llm_max_tokens
        if effective_max_tokens is not None:
            request_params["max_tokens"] = effective_max_tokens

        if settings.llm_top_p is not None:
            request_params["top_p"] = settings.llm_top_p

        extra_body = kwargs.pop("extra_body", None) or {}
        chat_template_kwargs = kwargs.pop("chat_template_kwargs", None) or {}

        existing_extra_body = request_params.pop("extra_body", None) or {}
        if existing_extra_body:
            extra_body = {**existing_extra_body, **extra_body}

        existing_template_kwargs = request_params.pop("chat_template_kwargs", None) or {}
        if existing_template_kwargs:
            chat_template_kwargs = {**existing_template_kwargs, **chat_template_kwargs}

        if settings.llm_enable_thinking is not None:
            chat_template_kwargs.setdefault("enable_thinking", settings.llm_enable_thinking)

        if chat_template_kwargs:
            existing_template_body = extra_body.get("chat_template_kwargs", {})
            if isinstance(existing_template_body, dict):
                extra_body["chat_template_kwargs"] = {
                    **existing_template_body,
                    **chat_template_kwargs,
                }
            else:
                extra_body["chat_template_kwargs"] = chat_template_kwargs

        if extra_body:
            request_params["extra_body"] = extra_body

        max_attempts = max(1, self.max_retries)
        last_error: Optional[Exception] = None

        for attempt in range(1, max_attempts + 1):
            try:
                self._ensure_client(self.active_base_url)
                logger.info(
                    "调用 LLM（流式）",
                    scenario=self.scenario,
                    model=self.model,
                    has_tools=bool(tools),
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
                stream = await self.client.chat.completions.create(stream=True, **request_params)

                async def _iterator():
                    async for chunk in stream:
                        yield chunk.model_dump()

                return _iterator()
            except Exception as e:
                last_error = e
                if self._is_enable_thinking_rejected(e):
                    if self._remove_enable_thinking(request_params):
                        logger.warning(
                            "模型不支持 enable_thinking（流式），已自动降级重试",
                            scenario=self.scenario,
                            model=self.model,
                        )
                        continue
                if self._is_server_error(e):
                    if self._remove_tooling(request_params):
                        logger.warning(
                            "检测到5xx且工具调用失败（流式），已自动降级为无tools重试",
                            scenario=self.scenario,
                            model=self.model,
                        )
                        continue
                if (
                    self.alt_base_url
                    and self.active_base_url != self.alt_base_url
                    and self._is_transient_network_error(e)
                ):
                    logger.warning(
                        "DashScope 主域名流式调用失败，切换至 intl 备用线路",
                        previous_base_url=self.active_base_url,
                        alt_base_url=self.alt_base_url,
                        error=str(e),
                    )
                    self.active_base_url = self.alt_base_url
                    continue
                logger.warning(
                    "LLM 流式调用失败",
                    error=str(e),
                    attempt=attempt,
                    max_attempts=max_attempts,
                )
                if attempt >= max_attempts:
                    break
                backoff = min(2 ** attempt, 10)
                await asyncio.sleep(backoff)

        logger.error("LLM 流式调用失败", error=str(last_error) if last_error else None)
        raise ParseError(f"LLM 调用失败: {str(last_error) if last_error else 'unknown error'}")
    
    def extract_function_call(self, response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        从响应中提取 Function Calling 参数
        
        Args:
            response: LLM 响应
        
        Returns:
            函数参数字典，如果没有则返回 None
        """
        try:
            message = response["choices"][0]["message"]
            
            # 检查是否有 tool_calls
            if "tool_calls" in message and message["tool_calls"]:
                tool_call = message["tool_calls"][0]
                function_args = tool_call["function"]["arguments"]

                # 兼容部分模型返回 dict（非字符串）
                if isinstance(function_args, dict):
                    if "arguments" in function_args:
                        arguments = function_args["arguments"]
                        logger.debug(
                            "函数调用参数解析成功 (dict)",
                            function_name=tool_call["function"].get("name"),
                            arguments_preview=str(arguments)[:500]
                        )
                        return arguments
                    return function_args

                if not isinstance(function_args, str):
                    logger.error("提取函数调用失败: 参数类型异常", arg_type=type(function_args).__name__)
                    return None

                cleaned = self._normalize_function_args(function_args)

                try:
                    parsed_args = json.loads(cleaned)
                    if isinstance(parsed_args, dict) and "arguments" in parsed_args:
                        arguments = parsed_args["arguments"]
                        logger.debug(
                            "函数调用参数解析成功",
                            function_name=tool_call["function"].get("name"),
                            arguments_preview=str(arguments)[:500]
                        )
                        return arguments
                    logger.debug(
                        "函数调用参数解析成功",
                        function_name=tool_call["function"].get("name"),
                        arguments_preview=str(parsed_args)[:500]
                    )
                    return parsed_args
                except json.JSONDecodeError:
                    decoder = json.JSONDecoder()
                    try:
                        parsed, end = decoder.raw_decode(cleaned)
                        trailing = cleaned[end:].strip()
                        if trailing:
                            logger.warning(
                                "函数调用参数包含多余内容，已忽略",
                                trailing_preview=trailing[:200]
                            )
                        if isinstance(parsed, dict) and "arguments" in parsed:
                            arguments = parsed["arguments"]
                            logger.debug(
                                "函数调用参数解析成功 (宽松模式)",
                                function_name=tool_call["function"].get("name"),
                                arguments_preview=str(arguments)[:500]
                            )
                            return arguments
                        logger.debug(
                            "函数调用参数解析成功 (宽松模式)",
                            function_name=tool_call["function"].get("name"),
                            arguments_preview=str(parsed)[:500]
                        )
                        return parsed
                    except json.JSONDecodeError as e:
                        logger.error("提取函数调用失败", error=str(e), preview=cleaned[:200])
                        return None
            
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                cleaned = self._normalize_function_args(content)
                try:
                    parsed = json.loads(cleaned)
                except json.JSONDecodeError:
                    decoder = json.JSONDecoder()
                    try:
                        parsed, _ = decoder.raw_decode(cleaned)
                    except json.JSONDecodeError:
                        return None

                if isinstance(parsed, dict):
                    if "arguments" in parsed and isinstance(parsed["arguments"], dict):
                        return parsed["arguments"]
                    if "function" in parsed and isinstance(parsed["function"], dict):
                        func_args = parsed["function"].get("arguments")
                        if isinstance(func_args, dict):
                            return func_args
                        if isinstance(func_args, str):
                            try:
                                obj = json.loads(self._normalize_function_args(func_args))
                                if isinstance(obj, dict):
                                    return obj
                            except Exception:
                                pass
                    return parsed

            return None
            
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            logger.error("提取函数调用失败", error=str(e))
            return None

    @staticmethod
    def _normalize_function_args(raw: str) -> str:
        """清洗 LLM 返回的函数参数字符串"""
        cleaned = raw.strip()

        # 去除代码块包裹 ```json ... ```
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?", "", cleaned, count=1, flags=re.IGNORECASE).strip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()

        # 去除前缀噪声（如"Sure, here is ...{"）
        first_brace = cleaned.find("{")
        if first_brace > 0:
            cleaned = cleaned[first_brace:]

        # 去除末尾噪声（如"}```"、解释说明等）
        last_brace = cleaned.rfind("}")
        if last_brace != -1:
            cleaned = cleaned[:last_brace + 1]

        return cleaned.strip()
    
    def get_text_content(self, response: Dict[str, Any]) -> str:
        """获取文本内容"""
        try:
            return response["choices"][0]["message"]["content"] or ""
        except (KeyError, IndexError):
            return ""

    @staticmethod
    def extract_stream_text(chunk: Dict[str, Any]) -> str:
        """解析流式返回中的文本内容。"""
        try:
            choice = chunk["choices"][0]
            delta = choice.get("delta", {})
            content = delta.get("content")

            if isinstance(content, list):
                parts = []
                for item in content:
                    if isinstance(item, dict):
                        parts.append(item.get("text") or "")
                return "".join(parts)

            if isinstance(content, str):
                return content

            return ""
        except (KeyError, IndexError, TypeError):
            return ""


async def create_llm_client(
    scenario: LLMScenario = "default",
    use_db_config: bool = True,
    **kwargs
) -> LLMClient:
    """
    创建 LLM 客户端的工厂函数
    
    此函数会自动尝试从数据库加载配置（如果启用）。
    
    Args:
        scenario: 使用场景
        use_db_config: 是否尝试从数据库加载配置（默认True）
        **kwargs: 传递给 LLMClient 的其他参数
        
    Returns:
        配置好的 LLMClient 实例
        
    Example:
        ```python
        # 使用数据库配置（如果可用）
        client = await create_llm_client("nl2ir")
        
        # 仅使用环境变量配置
        client = await create_llm_client("nl2ir", use_db_config=False)
        
        # 覆盖特定参数
        client = await create_llm_client("nl2ir", model="gpt-4o")
        ```
    """
    client = LLMClient(scenario=scenario, **kwargs)
    
    if use_db_config and settings.use_metadata_db:
        try:
            await client.load_config_from_db()
        except Exception as e:
            logger.warning("从数据库加载LLM配置失败，使用环境变量配置", 
                          scenario=scenario, error=str(e))
    
    return client
