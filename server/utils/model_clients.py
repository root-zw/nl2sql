"""
统一的模型客户端封装

提供 Embedding 和 Reranker 的统一接口，支持：
- vLLM/OpenAI 兼容接口
- Ollama 接口（仅 Embedding）
- 批量处理和并发控制
- 连接池管理
"""

from __future__ import annotations

import asyncio
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
import structlog
import httpx

import math
from server.config import settings, RetrievalConfig

logger = structlog.get_logger()


def _sigmoid(x: float) -> float:
    """Sigmoid 函数，将任意实数映射到 (0, 1)"""
    if x >= 700:
        return 1.0
    if x <= -700:
        return 0.0
    return 1.0 / (1.0 + math.exp(-x))


def _normalize_reranker_score(score: float, use_sigmoid: bool = True) -> float:
    """
    归一化 Reranker 分数到 [0, 1]
    
    Args:
        score: 原始分数
        use_sigmoid: 是否使用 sigmoid 归一化
    
    Returns:
        归一化后的分数
    """
    if score is None:
        return 0.0
    if use_sigmoid:
        return _sigmoid(score)
    return max(0.0, min(1.0, score))


# ============================================================================
# Embedding 客户端
# ============================================================================

class EmbeddingClient:
    """
    统一的 Embedding 客户端
    
    支持的后端：
    - vLLM/OpenAI 兼容接口（/v1/embeddings 或 /embeddings）
    - Ollama 接口（/api/embeddings）
    
    特性：
    - 自动检测服务类型
    - 批量处理（vLLM支持真批量，Ollama使用并发模拟）
    - 并发控制
    - 连接复用
    """
    
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: Optional[int] = None,
        max_concurrent: Optional[int] = None,
        batch_size: Optional[int] = None,
    ):
        """
        初始化 Embedding 客户端
        
        Args:
            base_url: API 基础 URL，默认从配置读取
            api_key: API 密钥，默认从配置读取
            model: 模型名称，默认从配置读取
            timeout: 请求超时时间（秒）
            max_concurrent: 最大并发数（用于 Ollama 模式）
            batch_size: 批量处理大小
        """
        self.base_url = (base_url or settings.embedding_base_url or "").rstrip("/")
        self.api_key = api_key or settings.embedding_api_key or ""
        self.model = model or settings.embedding_model or "bge-m3"
        self.timeout = timeout or settings.embedding_timeout or 30
        self.max_concurrent = max_concurrent or getattr(settings, 'embedding_max_concurrent', 10)
        self.batch_size = batch_size or getattr(settings, 'embedding_batch_size', 16)
        
        # 自动检测服务类型
        self.is_ollama = self._detect_ollama(self.base_url)
        
        # 并发控制信号量
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # 复用的 HTTP 客户端
        self._client: Optional[httpx.AsyncClient] = None
    
    @staticmethod
    def _detect_ollama(base_url: str) -> bool:
        """
        检测是否为 Ollama 服务。

        优先使用显式配置 `EMBEDDING_API_STYLE=ollama|openai|auto`：
        - ollama: 强制走 /api/embeddings
        - openai: 强制走 OpenAI/vLLM 兼容路径
        - auto: 尽量从 URL 推断（仅作为兜底）
        """
        api_style = (getattr(settings, "embedding_api_style", "auto") or "auto").strip().lower()
        if api_style == "ollama":
            return True
        if api_style == "openai":
            return False

        if not base_url:
            return False
        url_lower = base_url.lower()
        if "ollama" in url_lower:
            return True
        if ":11434" in url_lower:
            logger.warning(
                "Embedding API style=auto 且 URL 含 11434 端口，将推断为 Ollama；建议显式设置 EMBEDDING_API_STYLE",
                base_url=base_url,
            )
            return True
        return False
    
    async def _get_client(self) -> httpx.AsyncClient:
        """获取复用的 HTTP 客户端"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client
    
    def _build_headers(self) -> Dict[str, str]:
        """构建请求头"""
        headers = {"Content-Type": "application/json"}
        if not self.is_ollama and self.api_key and self.api_key != "dummy":
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers
    
    def _get_endpoint(self) -> str:
        """获取 API 端点"""
        if self.is_ollama:
            return f"{self.base_url}/api/embeddings"
        # vLLM/OpenAI 格式
        if "/v1" in self.base_url:
            return f"{self.base_url}/embeddings"
        return f"{self.base_url}/v1/embeddings"
    
    async def embed_single(self, text: str) -> List[float]:
        """
        生成单个文本的向量
        
        Args:
            text: 要向量化的文本
            
        Returns:
            向量列表
        """
        if not text:
            return []
        
        client = await self._get_client()
        url = self._get_endpoint()
        headers = self._build_headers()
        
        # 构建请求体
        if self.is_ollama:
            payload = {"model": self.model, "prompt": text}
        else:
            payload = {"model": self.model, "input": text}
        
        try:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            
            # 解析响应
            if self.is_ollama:
                if "embedding" in data:
                    return data["embedding"]
            else:
                if "data" in data and data["data"]:
                    return data["data"][0].get("embedding", [])
            
            # 兜底尝试
            if "embedding" in data:
                return data["embedding"]
            if isinstance(data, list):
                return data
            
            raise ValueError(f"无法解析 embedding 响应: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            
        except httpx.HTTPStatusError as e:
            logger.error("Embedding API 请求失败", status=e.response.status_code, url=url)
            raise RuntimeError(f"Embedding API 请求失败 (HTTP {e.response.status_code})") from e
        except Exception as e:
            logger.error("Embedding 请求异常", error=str(e), url=url)
            raise RuntimeError(f"Embedding 请求异常: {e}") from e
    
    async def _embed_single_with_semaphore(self, text: str) -> List[float]:
        """带信号量控制的单条向量生成"""
        async with self._semaphore:
            return await self.embed_single(text)
    
    async def embed_batch(
        self,
        texts: List[str],
        batch_size: Optional[int] = None,
    ) -> List[List[float]]:
        """
        批量生成向量
        
        - vLLM/OpenAI：使用真批量接口
        - Ollama：使用并发控制的并行请求
        
        Args:
            texts: 文本列表
            batch_size: 批量大小，默认使用初始化时的配置
            
        Returns:
            向量列表（与输入顺序一致）
        """
        if not texts:
            return []
        
        # 过滤空文本
        valid_texts = [t for t in texts if t]
        if not valid_texts:
            return []
        
        batch_size = batch_size or self.batch_size
        
        # Ollama 模式：使用并发控制
        if self.is_ollama:
            return await self._embed_batch_ollama(valid_texts)
        
        # vLLM/OpenAI 模式：使用真批量接口
        return await self._embed_batch_openai(valid_texts, batch_size)
    
    async def _embed_batch_ollama(self, texts: List[str]) -> List[List[float]]:
        """Ollama 模式：并发请求"""
        tasks = [self._embed_single_with_semaphore(text) for text in texts]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        vectors = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning("Ollama embedding 失败，使用空向量", index=i, error=str(result))
                vectors.append([])
            else:
                vectors.append(result)
        
        return vectors
    
    async def _embed_batch_openai(self, texts: List[str], batch_size: int) -> List[List[float]]:
        """vLLM/OpenAI 模式：真批量请求"""
        client = await self._get_client()
        url = self._get_endpoint()
        headers = self._build_headers()
        
        all_vectors: List[List[float]] = []
        
        for start in range(0, len(texts), batch_size):
            chunk = texts[start:start + batch_size]
            payload = {"model": self.model, "input": chunk}
            
            try:
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                
                if "data" in data and isinstance(data["data"], list):
                    # 按 index 排序确保顺序正确
                    sorted_items = sorted(data["data"], key=lambda x: x.get("index", 0))
                    chunk_vectors = [item.get("embedding", []) for item in sorted_items]
                else:
                    raise ValueError(f"批量响应格式异常: {list(data.keys())}")
                
                if len(chunk_vectors) != len(chunk):
                    raise ValueError(f"批量返回数量不匹配: 期望 {len(chunk)}, 实际 {len(chunk_vectors)}")
                
                all_vectors.extend(chunk_vectors)
                
            except Exception as e:
                logger.error("批量 embedding 失败", start=start, error=str(e))
                # 回退到单条处理
                for text in chunk:
                    try:
                        vec = await self.embed_single(text)
                        all_vectors.append(vec)
                    except Exception:
                        all_vectors.append([])
        
        return all_vectors
    
    async def aclose(self):
        """关闭客户端"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None


# ============================================================================
# Reranker 客户端
# ============================================================================

class RerankerClient:
    """
    统一的 Reranker 客户端
    
    支持的后端：
    - vLLM/OpenAI 兼容的 rerank 接口（/v1/rerank 或 /rerank）
    - Cohere 风格接口
    - sentence-transformers CrossEncoder 服务
    
    特性：
    - 批量处理
    - 并发控制
    - 自动归一化分数
    """
    
    def __init__(
        self,
        endpoint: Optional[str] = None,
        model: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        max_concurrent: Optional[int] = None,
        normalize_output: Optional[bool] = None,
    ):
        """
        初始化 Reranker 客户端
        
        Args:
            endpoint: API 端点，默认从配置读取
            model: 模型名称，默认从配置读取
            api_key: API 密钥（可选）
            timeout: 请求超时时间
            max_concurrent: 最大并发数
            normalize_output: 是否归一化输出分数到 [0, 1]
        """
        self.endpoint = (endpoint or settings.reranker_endpoint or "").rstrip("/")
        self.model = model or settings.reranker_model or ""
        # 支持从配置或参数传入 api_key
        self.api_key = api_key or getattr(settings, 'reranker_api_key', None) or ""
        self.timeout = timeout or getattr(settings, 'reranker_timeout', 30)
        self.max_concurrent = max_concurrent or getattr(settings, 'reranker_max_concurrent', 5)
        # 是否归一化输出，默认从配置读取
        self.normalize_output = (
            normalize_output if normalize_output is not None
            else RetrievalConfig.reranker_normalize()
        )
        
        # 并发控制
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # HTTP 客户端
        self._client: Optional[httpx.AsyncClient] = None
        
        # 是否已验证
        self._validated = False
        self._is_valid = False
        self._api_style: Optional[str] = None  # "batch" or "pair"
    
    def is_enabled(self) -> bool:
        """检查 Reranker 是否启用（配置开关 + 端点配置）"""
        from server.config import RetrievalConfig
        # 先检查配置开关，再检查端点配置
        return RetrievalConfig.reranker_enabled() and bool(self.endpoint and self.model)
    
    async def _get_client(self) -> httpx.AsyncClient:
        """获取复用的 HTTP 客户端"""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client
    
    def _build_headers(self) -> Dict[str, str]:
        """构建请求头"""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers
    
    def _get_rerank_url(self) -> str:
        """获取 rerank API URL"""
        # 如果端点已经包含 /rerank，直接使用
        if "/rerank" in self.endpoint:
            return self.endpoint
        # 如果包含 /v1，添加 /rerank
        if "/v1" in self.endpoint:
            base = self.endpoint.rstrip("/")
            if not base.endswith("/rerank"):
                return f"{base}/rerank"
            return base
        # 默认添加 /v1/rerank
        return f"{self.endpoint}/v1/rerank"
    
    async def _validate_and_detect_api(self) -> bool:
        """
        验证 Reranker 端点并检测 API 风格
        
        Returns:
            是否是有效的 Reranker 服务
        """
        if self._validated:
            return self._is_valid
        
        self._validated = True
        
        if not self.is_enabled():
            self._is_valid = False
            return False
        
        client = await self._get_client()
        url = self._get_rerank_url()
        headers = self._build_headers()
        
        # 尝试批量 rerank 接口
        test_payload = {
            "model": self.model,
            "query": "test query",
            "documents": ["test document 1", "test document 2"],
            "top_n": 2,
        }
        
        try:
            response = await client.post(url, headers=headers, json=test_payload)
            
            if response.status_code == 200:
                data = response.json()
                # 检查是否有 results 字段（标准 rerank 响应）
                if "results" in data and isinstance(data["results"], list):
                    self._api_style = "batch"
                    self._is_valid = True
                    logger.info(
                        "Reranker 验证成功（批量模式）",
                        endpoint=self.endpoint,
                        model=self.model,
                        url=url
                    )
                    return True
                else:
                    # 检查是否是其他格式的响应（如 SiliconFlow 可能返回不同的格式）
                    logger.warning(
                        "Reranker 响应格式异常",
                        status=response.status_code,
                        response_keys=list(data.keys()) if isinstance(data, dict) else type(data),
                        endpoint=self.endpoint
                    )
            else:
                # 记录详细的错误信息
                error_text = response.text[:500] if response.text else ""
                logger.warning(
                    "Reranker 验证请求失败",
                    status=response.status_code,
                    error=error_text,
                    endpoint=self.endpoint,
                    url=url,
                    has_api_key=bool(self.api_key)
                )
            
        except httpx.HTTPStatusError as e:
            error_text = e.response.text[:500] if e.response.text else ""
            logger.error(
                "Reranker 验证 HTTP 错误",
                status=e.response.status_code,
                error=error_text,
                endpoint=self.endpoint,
                url=url,
                has_api_key=bool(self.api_key)
            )
        except Exception as e:
            logger.error(
                "Reranker 验证请求异常",
                error=str(e),
                error_type=type(e).__name__,
                endpoint=self.endpoint,
                url=url,
                has_api_key=bool(self.api_key)
            )
        
        # 标记为无效
        self._is_valid = False
        logger.error(
            "Reranker 服务不可用或接口不兼容",
            endpoint=self.endpoint,
            model=self.model,
            url=url if 'url' in locals() else "unknown",
            has_api_key=bool(self.api_key),
            suggestion="请检查：1) RERANKER_ENDPOINT 和 RERANKER_MODEL 配置是否正确；2) RERANKER_API_KEY 是否已设置（如需要）；3) 服务是否可访问"
        )
        return False
    
    def _normalize_score(self, score: float) -> float:
        """归一化分数到 [0, 1]"""
        if not self.normalize_output:
            return score
        return _normalize_reranker_score(score, use_sigmoid=True)
    
    async def rerank(
        self,
        query: str,
        documents: List[str],
        top_n: Optional[int] = None,
    ) -> List[float]:
        """
        对文档进行重排序
        
        Args:
            query: 查询文本
            documents: 候选文档列表
            top_n: 返回前 N 个结果（None 表示返回全部）
            
        Returns:
            分数列表（与输入文档顺序一致）
        """
        if not documents:
            return []
        
        # 首次调用时验证
        if not self._validated:
            await self._validate_and_detect_api()
        
        # 如果服务无效，返回空分数
        if not self._is_valid:
            return [0.0] * len(documents)
        
        client = await self._get_client()
        url = self._get_rerank_url()
        headers = self._build_headers()
        
        payload = {
            "model": self.model,
            "query": query,
            "documents": documents,
        }
        if top_n is not None:
            payload["top_n"] = top_n
        
        try:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            
            # 解析响应
            results = data.get("results", [])
            
            # 构建 index -> score 映射
            score_map: Dict[int, float] = {}
            for item in results:
                idx = item.get("index", -1)
                score = item.get("relevance_score") or item.get("score", 0.0)
                if idx >= 0:
                    score_map[idx] = self._normalize_score(score)
            
            # 按原始顺序返回分数
            scores = []
            for i in range(len(documents)):
                scores.append(score_map.get(i, 0.0))
            
            return scores
            
        except httpx.HTTPStatusError as e:
            error_text = e.response.text[:500] if e.response.text else ""
            logger.error(
                "Reranker 请求 HTTP 错误",
                status=e.response.status_code,
                error=error_text,
                url=url,
                model=self.model,
                has_api_key=bool(self.api_key)
            )
            return [0.0] * len(documents)
        except Exception as e:
            logger.error(
                "Reranker 请求失败",
                error=str(e),
                error_type=type(e).__name__,
                url=url,
                model=self.model,
                has_api_key=bool(self.api_key)
            )
            return [0.0] * len(documents)
    
    async def rerank_with_indices(
        self,
        query: str,
        documents: List[str],
        top_n: Optional[int] = None,
    ) -> List[Tuple[int, float]]:
        """
        对文档重排序并返回 (索引, 分数) 列表，按分数降序排列
        
        Args:
            query: 查询文本
            documents: 候选文档列表
            top_n: 返回前 N 个结果
            
        Returns:
            [(index, score), ...] 列表，按分数降序
        """
        scores = await self.rerank(query, documents, top_n)
        
        # 创建 (index, score) 列表并排序
        indexed_scores = list(enumerate(scores))
        indexed_scores.sort(key=lambda x: x[1], reverse=True)
        
        if top_n is not None:
            indexed_scores = indexed_scores[:top_n]
        
        return indexed_scores
    
    async def rerank_batch(
        self,
        queries: List[str],
        documents_list: List[List[str]],
        top_n: Optional[int] = None,
    ) -> List[List[float]]:
        """
        批量重排序多个查询
        
        Args:
            queries: 查询列表
            documents_list: 每个查询对应的文档列表
            top_n: 每个查询返回前 N 个
            
        Returns:
            每个查询的分数列表
        """
        if len(queries) != len(documents_list):
            raise ValueError("queries 和 documents_list 长度必须相同")
        
        async def rerank_single(query: str, docs: List[str]) -> List[float]:
            async with self._semaphore:
                return await self.rerank(query, docs, top_n)
        
        tasks = [
            rerank_single(q, docs)
            for q, docs in zip(queries, documents_list)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.warning("批量 rerank 失败", index=i, error=str(result))
                final_results.append([0.0] * len(documents_list[i]))
            else:
                final_results.append(result)
        
        return final_results
    
    async def aclose(self):
        """关闭客户端"""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None


# ============================================================================
# 客户端池管理
# ============================================================================

class ModelClientPool:
    """
    模型客户端池（单例模式）
    
    统一管理 Embedding 和 Reranker 客户端
    """
    
    _instance: Optional['ModelClientPool'] = None
    _lock = asyncio.Lock()
    
    def __init__(self):
        self._embedding_client: Optional[EmbeddingClient] = None
        self._reranker_client: Optional[RerankerClient] = None
    
    @classmethod
    async def get_instance(cls) -> 'ModelClientPool':
        """获取单例实例"""
        if cls._instance is None:
            async with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    async def get_embedding_client(self) -> EmbeddingClient:
        """获取 Embedding 客户端"""
        if self._embedding_client is None:
            self._embedding_client = EmbeddingClient()
            logger.info(
                "Embedding 客户端初始化",
                base_url=self._embedding_client.base_url,
                model=self._embedding_client.model,
                is_ollama=self._embedding_client.is_ollama
            )
        return self._embedding_client
    
    async def get_reranker_client(self) -> RerankerClient:
        """获取 Reranker 客户端"""
        if self._reranker_client is None:
            self._reranker_client = RerankerClient()
            if self._reranker_client.is_enabled():
                logger.info(
                    "Reranker 客户端初始化",
                    endpoint=self._reranker_client.endpoint,
                    model=self._reranker_client.model
                )
            else:
                logger.info("Reranker 未配置，跳过初始化")
        return self._reranker_client
    
    async def close_all(self):
        """关闭所有客户端"""
        tasks = []
        if self._embedding_client:
            tasks.append(self._embedding_client.aclose())
        if self._reranker_client:
            tasks.append(self._reranker_client.aclose())
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self._embedding_client = None
        self._reranker_client = None


# ============================================================================
# 便捷函数
# ============================================================================

async def get_embedding_client() -> EmbeddingClient:
    """获取 Embedding 客户端（推荐使用）"""
    pool = await ModelClientPool.get_instance()
    return await pool.get_embedding_client()


async def get_reranker_client() -> RerankerClient:
    """获取 Reranker 客户端（推荐使用）"""
    pool = await ModelClientPool.get_instance()
    return await pool.get_reranker_client()


async def close_model_clients():
    """关闭所有模型客户端（应用关闭时调用）"""
    if ModelClientPool._instance:
        await ModelClientPool._instance.close_all()
