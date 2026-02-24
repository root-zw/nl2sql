"""多连接查询监控指标"""

from prometheus_client import Counter, Histogram, Gauge

# 连接解析次数
connection_resolution_total = Counter(
    'nl2sql_connection_resolution_total',
    'Total connection resolution attempts',
    ['status', 'method']
)

# 连接解析耗时
connection_resolution_duration = Histogram(
    'nl2sql_connection_resolution_duration_seconds',
    'Time spent resolving connection',
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0]
)

# 跨连接冲突次数
cross_connection_conflicts = Counter(
    'nl2sql_cross_connection_conflicts_total',
    'Number of cross-connection conflicts detected'
)

# 用户指定冲突次数
user_specified_conflicts = Counter(
    'nl2sql_user_specified_conflicts_total',
    'Number of conflicts between user-specified and inferred connections',
    ['resolution']
)

# 当前活跃连接数
active_connections_gauge = Gauge(
    'nl2sql_active_connections',
    'Number of active database connections'
)

# 连接解析成功率
connection_resolution_success_rate = Counter(
    'nl2sql_connection_resolution_success_total',
    'Successful connection resolutions',
    ['connection_id']
)

# 表未找到错误次数
table_not_found_errors = Counter(
    'nl2sql_table_not_found_errors_total',
    'Number of table not found errors'
)

