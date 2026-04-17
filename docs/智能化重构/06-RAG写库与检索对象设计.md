# RAG写库与检索对象设计

## 1. 文档信息

- 状态：现状已核对
- 类型：实现说明文档
- 最后更新时间：2026-04-16

## 2. 文档目标

本文档只回答当前实现相关的问题：

- 当前 RAG 原始事实源在哪里
- PostgreSQL 原表有哪些
- Milvus 真实集合有哪些
- 在线写回、全量同步、增量同步分别由谁负责
- 在线检索按什么过滤条件运行

本文档不是目标态大蓝图，而是当前实现真相文档。

## 3. 当前真实分层

当前 RAG 在代码里实际是三层：

1. PostgreSQL 原始事实层
2. Milvus 检索服务层
3. 在线检索消费层

必须明确：

- PostgreSQL 是事实源
- Milvus 是可重建的检索服务层
- 在线链路不应直接写 Milvus

## 4. 当前原始事实源

### 4.1 主要正式表

当前与 RAG 直接相关的主要原表是：

- `business_domains`
- `db_tables`
- `fields`
- `field_enum_values`
- `qa_few_shot_samples`

### 4.2 主要同步表

当前同步层相关对象主要是：

- `milvus_pending_changes`
- `milvus_sync_history`

### 4.3 few-shot 反馈表

当前 few-shot 还存在单独反馈表：

- `qa_few_shot_feedback`

## 5. 当前写库链路

### 5.1 正式元数据链路

当前正式元数据的真实链路是：

1. 管理端或导入逻辑先写 PostgreSQL 正式表
2. 触发器把变更写入 `milvus_pending_changes`
3. 同步服务读取待同步记录
4. `build_*_entities` 构造 Milvus 实体
5. `sync_milvus.py` / `unified_sync_service.py` 写入 Milvus

### 5.2 few-shot 在线写回链路

当前 few-shot 在线写回链路是：

1. 查询成功
2. `FewShotWriter.record_successful_query(...)`
3. `FewShotDatasetService.upsert_samples(...)`
4. 写入 `qa_few_shot_samples`
5. 触发器写入 `milvus_pending_changes`
6. 同步服务把样本转成 Milvus 实体

## 6. 当前 `qa_few_shot_samples` 表

### 6.1 关键字段

当前关键字段包括：

- `sample_id`
- `connection_id`
- `question`
- `sql_text`
- `ir_json`
- `tables`
- `tables_json`
- `domain_id`
- `quality_score`
- `source_tag`
- `sample_type`
- `sql_context`
- `error_msg`
- `metadata`
- `last_verified_at`
- `is_verified`
- `is_active`

### 6.2 当前真实约束

- 唯一键：`(connection_id, question, sql_text)`
- 索引：`connection_id`、`domain_id`、`quality_score`
- 质量分约束：`0 <= quality_score <= 1`

### 6.3 当前需要注意的兼容事实

虽然表上已经有以下专列：

- `sample_type`
- `sql_context`
- `error_msg`

当前同步查询仍保留从 `metadata` 兼容读取的逻辑：

- `sample_type`
  - `COALESCE(metadata->>'sample_type', 'standard')`
- `sql_context`
  - `COALESCE(metadata->>'sql_context', qs.sql_text)`
- `error_msg`
  - `metadata->>'error_msg'`

因此当前字段来源仍是“专列 + metadata 兼容来源”的状态。

## 7. 当前 Milvus 集合

### 7.1 集合清单

当前真实集合如下：

- `semantic_metadata`
  - 配置项：`MILVUS_COLLECTION`
- `enum_values_dual`
  - 配置项：`MILVUS_ENUM_COLLECTION`
- `qa_few_shot_samples`
  - 配置项：`MILVUS_FEW_SHOT_COLLECTION`

### 7.2 `semantic_metadata`

当前集合承载：

- 业务域
- 表
- 字段

当前关键字段包括：

- `item_id`
- `connection_id`
- `domain_id`
- `table_id`
- `entity_type`
- `semantic_type`
- `schema_name`
- `table_name`
- `column_name`
- `display_name`
- `description`
- `graph_text`
- `field_id`
- `dense_vector`
- `sparse_vector`
- `bm25_text`
- `json_meta`
- `is_active`

当前实现使用：

- `entity_type` 作为分区键

### 7.3 `enum_values_dual`

当前集合承载：

- 枚举值检索对象

当前关键字段包括：

- `value_id`
- `field_id`
- `table_id`
- `domain_id`
- `field_name`
- `table_name`
- `connection_id`
- `value`
- `display_name`
- `synonyms`
- `value_index_text`
- `context_index_text`
- `bm25_text`
- `json_meta`
- `frequency`
- `value_vector`
- `context_vector`
- `sparse_vector`
- `is_active`

当前实现使用：

- `connection_id` 作为分区键

### 7.4 `qa_few_shot_samples` 集合

当前集合承载：

- few-shot 样本检索对象

当前关键字段包括：

- `sample_id`
- `connection_id`
- `domain_id`
- `sample_type`
- `question`
- `ir_json`
- `sql_context`
- `error_msg`
- `quality_score`
- `bm25_text`
- `json_meta`
- `dense_vector`
- `sparse_vector`
- `is_active`

当前实现没有把 `connection_id` 配成分区键，它只是普通字段。

## 8. 当前实体构建函数

当前实体构建主要由以下函数负责：

- `build_domain_entities`
- `build_table_entities`
- `build_field_entities`
- `build_enum_entities`
- `build_few_shot_entities`

其中 `build_few_shot_entities(...)` 当前还会派生：

- `related_fields`
- `query_type`

这些是 Milvus 检索辅助字段，不是 PostgreSQL 原表顶层事实字段。

## 9. 当前删除与回滚策略

### 9.1 全量同步

当前全量同步的总体策略是：

- 先按连接或对象类型删除旧实体
- 再整体重建

### 9.2 增量同步

当前增量同步总体是：

- 先删除旧实体
- 再插入新实体

当前删除主键的现实如下：

- 语义元数据
  - 主要依赖 `connection_id + item_id`
- 枚举值
  - 主要依赖 `value_id`
- few-shot
  - 主要依赖 `sample_id`

这里需要明确：

- 当前能工作，主要是因为 `UUID` 全局唯一
- 但这不是长期最稳的治理约束

## 10. 当前在线检索过滤规则

### 10.1 业务域检索

当前 `DomainDetector` 过滤核心是：

- `entity_type == "domain"`
- `is_active == true`
- `connection_id == 当前连接`

### 10.2 表检索

当前 `TableRetriever` 过滤核心是：

- `entity_type == "table"`
- `is_active == true`
- `connection_id == 当前连接`

并且当前实现已经不再把 `domain_id` 当作前置硬过滤。

### 10.3 度量字段检索

当前度量字段检索核心是：

- `entity_type == "field"`
- `semantic_type == "measure"`
- `is_active == true`
- `connection_id == 当前连接`

### 10.4 枚举值检索

当前枚举值检索核心是：

- 先收缩到候选字段
- 再按 `field_id` 集合过滤
- 再按 `connection_id` 和 `is_active` 过滤

### 10.5 few-shot 检索

当前 few-shot 检索核心是：

- `connection_id == 当前连接`
- `is_active == true`
- `quality_score >= 阈值`

当前没有把 `domain_id` 作为硬过滤条件。

## 11. 当前必须保留的硬规则

基于当前实现，以下规则应继续保留：

1. PostgreSQL 是事实源，Milvus 不是正式真相
2. 在线流程不直接写 Milvus
3. Milvus 实体必须可以从 PostgreSQL 重建
4. 所有检索对象都应显式带 `connection_id`
5. `sparse_vector` 由 BM25 Function 从 `bm25_text` 自动生成
6. few-shot 样本至少受 `quality_score` 和 `is_active` 双重控制

## 12. 当前风险点

当前最值得记录的风险点包括：

- few-shot 字段来源仍有 `metadata` 兼容逻辑
- 枚举与 few-shot 的增量删除条件仍偏弱
- few-shot 集合没有以 `connection_id` 作为分区键

## 13. 文档边界

本文档当前只负责说明实现真相，不负责以下内容：

- Query Draft 设计
- 运行时记忆设计
- 治理候选与审核设计

这些内容应由 `01-05` 继续承担，但 `01-05` 不能反向覆盖本文档中的实现事实。
