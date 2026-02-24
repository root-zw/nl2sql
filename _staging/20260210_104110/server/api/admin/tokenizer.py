"""
分词器管理API

提供：
- 分词器状态查询
- 缓存管理
- 词典热更新
- 分词测试
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

router = APIRouter(prefix="/tokenizer", tags=["分词器管理"])


class TokenizeRequest(BaseModel):
    """分词请求"""
    text: str = Field(..., description="待分词文本")
    mode: str = Field("default", description="分词模式: default/dense/sparse/search")
    with_pos: bool = Field(False, description="是否包含词性标注")


class TokenizeResponse(BaseModel):
    """分词响应"""
    tokens: List[str]
    token_count: int
    pos_tags: Optional[List[Dict[str, str]]] = None
    elapsed_ms: float


class TokenizerStatsResponse(BaseModel):
    """分词器统计响应"""
    total_calls: int
    total_time_ms: str
    avg_time_ms: str
    cache: Optional[Dict[str, Any]] = None


class AddWordRequest(BaseModel):
    """添加词请求"""
    word: str = Field(..., description="要添加的词")
    freq: Optional[int] = Field(None, description="词频")
    tag: Optional[str] = Field(None, description="词性标签")


@router.get("/stats", response_model=TokenizerStatsResponse, summary="获取分词器统计")
async def get_tokenizer_stats():
    """获取分词器统计信息，包括调用次数、耗时、缓存命中率等"""
    try:
        from server.nl2ir.tokenizer import get_tokenizer
        tokenizer = get_tokenizer()
        return tokenizer.get_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取统计失败: {str(e)}")


@router.post("/tokenize", response_model=TokenizeResponse, summary="测试分词")
async def tokenize_text(request: TokenizeRequest):
    """测试分词效果"""
    import time
    
    try:
        from server.nl2ir.tokenizer import get_tokenizer
        tokenizer = get_tokenizer()
        
        start = time.time()
        
        if request.mode == "dense":
            tokens = tokenizer.tokenize_for_dense(request.text)
        elif request.mode == "sparse":
            tokens = tokenizer.tokenize_for_sparse(request.text)
        elif request.mode == "search":
            tokens = tokenizer.cut_for_search(request.text)
        else:
            tokens = tokenizer.cut(request.text)
        
        pos_tags = None
        if request.with_pos:
            pos_result = tokenizer.cut_with_pos(request.text)
            pos_tags = [{"word": w, "pos": p} for w, p in pos_result]
        
        elapsed_ms = (time.time() - start) * 1000
        
        return TokenizeResponse(
            tokens=tokens,
            token_count=len(tokens),
            pos_tags=pos_tags,
            elapsed_ms=round(elapsed_ms, 3),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分词失败: {str(e)}")


@router.post("/cache/clear", summary="清除分词缓存")
async def clear_tokenizer_cache():
    """清除分词结果缓存"""
    try:
        from server.nl2ir.tokenizer import get_tokenizer
        tokenizer = get_tokenizer()
        tokenizer.clear_cache()
        return {"success": True, "message": "缓存已清除"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清除缓存失败: {str(e)}")


@router.post("/dictionary/reload", summary="重新加载词典")
async def reload_dictionary():
    """重新加载领域词典（热更新）"""
    try:
        from server.nl2ir.tokenizer import get_tokenizer
        tokenizer = get_tokenizer()
        tokenizer.reload_dictionaries()
        return {"success": True, "message": "词典已重新加载"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"重载词典失败: {str(e)}")


@router.post("/dictionary/add-word", summary="添加词到词典")
async def add_word_to_dictionary(request: AddWordRequest):
    """动态添加词到词典"""
    try:
        from server.nl2ir.tokenizer import get_tokenizer
        tokenizer = get_tokenizer()
        tokenizer.add_word(request.word, freq=request.freq, tag=request.tag)
        return {
            "success": True,
            "message": f"词 '{request.word}' 已添加",
            "word": request.word,
            "freq": request.freq,
            "tag": request.tag,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"添加词失败: {str(e)}")


@router.delete("/dictionary/del-word/{word}", summary="从词典删除词")
async def del_word_from_dictionary(word: str):
    """从词典删除词"""
    try:
        from server.nl2ir.tokenizer import get_tokenizer
        tokenizer = get_tokenizer()
        tokenizer.del_word(word)
        return {"success": True, "message": f"词 '{word}' 已删除"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除词失败: {str(e)}")
