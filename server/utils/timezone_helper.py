"""
时区辅助工具
统一系统时区为 Asia/Shanghai

使用方法：
    from server.utils.timezone_helper import now_with_tz, SYSTEM_TIMEZONE
    
    # 获取当前时间（带时区）
    current_time = now_with_tz()
    
    # 或者直接导入SYSTEM_TIMEZONE
    dt = datetime.now(SYSTEM_TIMEZONE)
"""

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

from server.config import settings


def _load_system_timezone() -> ZoneInfo:
    """根据配置加载系统时区，失败时回退到UTC。"""
    try:
        return ZoneInfo(settings.timezone)
    except Exception:
        return ZoneInfo("UTC")


# 系统标准时区
SYSTEM_TIMEZONE = _load_system_timezone()

# UTC时区
UTC_TIMEZONE = timezone.utc


def now_with_tz() -> datetime:
    """
    获取当前时间（带时区）
    
    Returns:
        datetime: 按配置时区的当前时间
    """
    return datetime.now(SYSTEM_TIMEZONE)


def utc_to_local(dt: datetime) -> datetime:
    """
    将UTC时间转换为本地时间（Asia/Shanghai）
    
    Args:
        dt: UTC时间（可以是带时区或不带时区）
    
    Returns:
        datetime: 配置时区的时间
    """
    if dt.tzinfo is None:
        # 假设输入是UTC时间
        dt = dt.replace(tzinfo=timezone.utc)
    
    return dt.astimezone(SYSTEM_TIMEZONE)


def local_to_utc(dt: datetime) -> datetime:
    """
    将本地时间（Asia/Shanghai）转换为UTC时间
    
    Args:
        dt: 本地时间（可以是带时区或不带时区）
    
    Returns:
        datetime: UTC时区的时间
    """
    if dt.tzinfo is None:
        # 假设输入是本地时间
        dt = dt.replace(tzinfo=SYSTEM_TIMEZONE)
    
    return dt.astimezone(timezone.utc)


def ensure_timezone(dt: datetime, assume_utc: bool = False) -> datetime:
    """
    确保datetime对象带有时区信息
    
    Args:
        dt: datetime对象
        assume_utc: 如果为True，无时区时假设为UTC；否则假设为本地时区
    
    Returns:
        datetime: 带时区的datetime对象
    """
    if dt.tzinfo is not None:
        return dt
    
    if assume_utc:
        return dt.replace(tzinfo=timezone.utc)
    else:
        return dt.replace(tzinfo=SYSTEM_TIMEZONE)


def format_datetime(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    格式化datetime为字符串（自动转换为本地时区）
    
    Args:
        dt: datetime对象
        fmt: 格式化字符串
    
    Returns:
        str: 格式化后的字符串
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SYSTEM_TIMEZONE)
    else:
        dt = dt.astimezone(SYSTEM_TIMEZONE)
    
    return dt.strftime(fmt)


def now_utc() -> datetime:
    """
    获取当前UTC时间（带时区）
    
    Returns:
        datetime: 带有UTC时区的当前时间
    """
    return datetime.now(UTC_TIMEZONE)


def utcnow_with_tz() -> datetime:
    """
    获取当前UTC时间（带时区）- 别名函数
    
    兼容旧代码中使用datetime.utcnow()的地方
    
    Returns:
        datetime: 带有UTC时区的当前时间
    """
    return datetime.now(UTC_TIMEZONE)


def get_datetime_with_delta(
    base_time: Optional[datetime] = None,
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0
) -> datetime:
    """
    获取基于某个时间的偏移时间
    
    Args:
        base_time: 基准时间（默认为当前时间）
        days: 天数偏移
        hours: 小时偏移
        minutes: 分钟偏移
        seconds: 秒数偏移
    
    Returns:
        datetime: 偏移后的时间（带时区）
    """
    if base_time is None:
        base_time = now_with_tz()
    elif base_time.tzinfo is None:
        base_time = base_time.replace(tzinfo=SYSTEM_TIMEZONE)
    
    delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    return base_time + delta


def to_isoformat(dt: datetime, with_tz: bool = True) -> str:
    """
    将datetime转换为ISO格式字符串
    
    Args:
        dt: datetime对象
        with_tz: 是否包含时区信息
    
    Returns:
        str: ISO格式的时间字符串
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=SYSTEM_TIMEZONE)
    
    if with_tz:
        return dt.isoformat()
    else:
        return dt.replace(tzinfo=None).isoformat()


def parse_datetime(dt_str: str, assume_local: bool = True) -> datetime:
    """
    解析时间字符串为datetime对象
    
    Args:
        dt_str: 时间字符串（ISO格式）
        assume_local: 如果没有时区信息，是否假设为本地时区
    
    Returns:
        datetime: 解析后的datetime对象（带时区）
    """
    dt = datetime.fromisoformat(dt_str)
    
    if dt.tzinfo is None:
        if assume_local:
            dt = dt.replace(tzinfo=SYSTEM_TIMEZONE)
        else:
            dt = dt.replace(tzinfo=UTC_TIMEZONE)
    
    return dt

