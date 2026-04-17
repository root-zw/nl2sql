# 学习事件与Runtime Memory设计

## 1. 文档信息

- 状态：草案
- 对应总文档章节：`00-智能化重构总方案.md`
- 最后更新时间：2026-04-17

## 2. 设计目标

本模块希望解决的问题是：

- 当前系统到底已经有哪些“学习相关对象”
- 为什么这些对象还不等于统一学习系统
- 如果继续建设，建议先补什么

## 3. 当前现实

### 3.1 当前已存在的学习相关对象

当前仓库已经存在以下对象或能力：

- `query_history`
- `conversation_messages`
- `qa_few_shot_samples`
- `qa_few_shot_feedback`
- `parse_with_feedback`

### 3.2 它们当前各自的职责

- `query_history`
  - 查询结果与执行信息归档
- `conversation_messages`
  - 对话消息存档
- `qa_few_shot_samples`
  - few-shot 检索样本
- `qa_few_shot_feedback`
  - few-shot 样本反馈
- `parse_with_feedback`
  - 局部解析纠偏能力

### 3.3 当前仍不存在的对象

以下对象当前代码和 SQL 中都还没有：

- `learning_events`
- `runtime_memories`

因此它们在本文中都只是目标态建议。

## 4. 当前问题

当前学习相关能力的主要问题是：

- 事实记录分散
- 在线动作和结果反馈没有统一事件层
- few-shot 样本不能替代完整学习事实
- 没有统一“下一次问答可直接使用”的记忆层
- 没有正式定义“同一会话里围绕上一份结果继续分析”的短期结果上下文

## 5. 目标设计建议

### 5.1 事件优先原则

更稳妥的路线不是先做复杂归纳，而是：

1. 先记事件
2. 再做记忆提炼
3. 再做治理候选

### 5.2 建议新增的事件层

建议未来引入：

- `learning_events`

建议第一版至少记录：

- `query_submitted`
- `draft_generated`
- `action_applied`
- `draft_approved`
- `execution_approved`
- `execution_completed`
- `result_feedback_recorded`
- `memory_written`
- `governance_candidate_observed`

### 5.3 建议新增的运行时记忆层

建议未来引入：

- `runtime_memories`

建议它只承担“能直接改善下一次问答”的对象，例如：

- 用户显式偏好
- 同一用户的稳定歧义解法
- 组织级临时运行时提示

### 5.4 会话内结果上下文不应并入 `runtime_memories`

还需要单独区分一层短期能力：

- `analysis_context`

它负责解决的是：

- 用户查出一份结果后，继续追问这份结果
- 用户再查另一部分数据，并与上一份结果做对比

这层能力的本质是：

- 同一会话内的短期分析上下文
- 同步主链路上的产品状态
- 对当前或下一条查询立即生效

因此不应把它直接并入 `runtime_memories`，原因包括：

- 它依赖的是刚刚产生的具体结果引用，而不是稳定偏好
- 它通常只在当前会话或当前主题下有效
- 它不应经过异步提炼后才生效
- 它失效得更快，也更像在线状态而不是长期记忆

## 6. 记忆层边界建议

必须明确：

- `runtime_memories` 不是正式语义层
- `runtime_memories` 也不是会话内结果级追问上下文
- 组织级稳定知识最终仍应进入正式治理层
- 已发布的正式对象应继续回写现有表，如 `fields`、`field_enum_values`、`global_rules`

建议同时固定三层边界：

- `query_sessions.state_json`
  - 承担当前查询状态与短期 `analysis_context`
- `runtime_memories`
  - 承担跨查询、跨会话仍可复用的稳定增强
- 正式治理资产
  - 承担已审核、已发布的长期标准知识

## 7. 建议的数据结构

### 7.1 `learning_events`

建议最小字段：

- `event_id`
- `event_key`
- `query_id`
- `conversation_id`
- `user_id`
- `event_type`
- `event_version`
- `payload_json`
- `source_component`
- `created_at`

### 7.2 `runtime_memories`

建议最小字段：

- `memory_id`
- `scope_type`
- `scope_id`
- `memory_type`
- `memory_key`
- `memory_value_json`
- `confidence_score`
- `support_count`
- `source`
- `last_verified_at`
- `expires_at`
- `is_active`

## 8. 同步与异步边界建议

### 8.1 同步主链路建议只做

- 写最小事件
- 维护当前查询和短期 `analysis_context`
- 返回当前查询结果

### 8.2 异步链路建议承担

- 偏好提炼
- 稳定模式识别
- 记忆写入
- 治理候选生成

不建议把以下内容放进异步链路后才生效：

- 结果完成后的继续追问上下文
- 上一份结果作为当前对比基准的引用

### 8.3 事件投递与顺序语义建议

如果后续真的把学习链补起来，建议明确以下处理语义：

- `query_sessions` / `draft_actions` 提交成功后，才能派生 `learning_events`
- 同一 `query_id` 下的事件处理顺序应以持久化顺序为准，而不是以内存回调顺序为准
- `runtime_memories` 只能基于已提交事件提炼，不能直接基于内存中的 trace 或临时对象提炼
- `governance_candidates` 只能来自已落地事件或已验证记忆，不能直接从未提交动作生成
- 异步任务至少要支持按 `event_key` 去重，避免重试时重复写记忆或重复生成候选

### 8.4 第一阶段建议固定的异步链

建议第一阶段先固定最小链路：

1. 同步写 `learning_events`
2. 先由事件窗口异步观察 `governance_candidates`
3. 如运行时增强价值已经明确，再异步提炼最小 `runtime_memories`

不建议一开始把“动作 -> 候选 -> 发布建议”压成一个同步大事务。

## 9. 与当前对象的衔接建议

建议第一阶段保持共存，而不是替换：

- `query_history`
  - 继续承担查询归档
- `conversation_messages`
  - 继续承担消息存档
- `query_sessions.state_json.analysis_context`
  - 继续承担会话内短期结果上下文
- `qa_few_shot_samples`
  - 继续承担样本沉淀
- `learning_events`
  - 承担统一学习事实层
- `runtime_memories`
  - 承担下一次问答增强

## 10. 第一阶段建议

### 第一阶段建议实现

- 最小 `learning_events`
- 显式结果反馈入口
- 显式“记住这次偏好”入口
- `runtime_memories` 提炼契约预留
- 如收益明确，再补最小 `runtime_memories`

### 第一阶段不建议实现

- 大规模隐式反馈建模
- 复杂自动聚类
- 复杂组织级自动归纳发布

## 11. 当前不应写成事实的项

以下内容当前都还不是事实：

- 系统已经有统一事件层
- 当前已经存在运行时记忆层
- few-shot 样本已经覆盖学习闭环
