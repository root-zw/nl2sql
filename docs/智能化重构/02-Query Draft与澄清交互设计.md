# Query Draft与澄清交互设计

> 历史说明：本文早期关于“没有统一动作表、没有草稿版本号、没有统一确认主链路”的判断已经过期。当前实现请以 `server/api/query/routes.py`、`server/services/query_session_service.py`、`server/services/draft_action_service.py` 和 `17-Prompt编排、Skill分层与统一确认最终方案.md` / `18-统一确认现状与目标态落地路线图.md` 为准。

## 1. 文档信息

- 状态：历史草案（已过期，非当前实现真相源）
- 对应总文档章节：`00-智能化重构总方案.md`
- 最后更新时间：2026-04-17

## 2. 设计目标

本模块希望把“系统当前理解”从黑盒过程改造成一个可见、可版本化、可修改的对象。

## 3. 当前现实

### 3.1 当前最接近的承载点

当前最接近本模块的线上能力是：

- 表选择确认卡
- 高成本执行确认卡
- 前端确认卡容器
- `QueryRequest`
- `QueryResponse`

### 3.2 当前必须明确的事实

当前仓库里：

- 没有持久化的 `Query Draft` 表
- 没有草稿版本号
- 没有统一动作表
- 没有语义级澄清的后端主链路

### 3.3 当前一个容易误判的点

前端 `Chat.vue` 里仍然保留了“请确认 AI 的理解”的确认卡容器，但后端当前真正返回该卡的主要场景是：

- 成本超限执行确认

因此不能把该 UI 容器误判为已经有完整的语义级 `Query Draft` 实现。

## 4. 当前已实现的兼容协议

当前与“用户确认后继续执行”最相关的输入字段是：

- `selected_table_id`
- `selected_table_ids`
- `multi_table_mode`
- `original_query_id`
- `force_execute`

它们分别服务于：

- 表选择确认后的续接
- 多表查询续接
- 高成本执行确认后的继续执行

## 5. 目标设计建议

### 5.1 建议新增的 Query Draft 对象

建议未来引入：

- `query_drafts`

建议最小字段如下：

- `draft_id`
- `query_id`
- `version`
- `status`
- `user_goal_summary`
- `draft_json`
- `uncertainty_json`
- `confirmed_points_json`
- `previous_version_id`
- `created_at`
- `updated_at`

### 5.2 建议最小草稿内容

建议 `draft_json` 第一版至少覆盖：

- 指标
- 维度
- 时间范围
- 过滤条件
- 候选表
- 已选表
- 排序
- 结果形态

### 5.3 建议最小不确定点结构

建议 `uncertainty_json` 的单项至少包含：

- `point_id`
- `ambiguity_type`
- `field_path`
- `question_text`
- `options`
- `must_resolve_before_execute`
- `recommended_option`
- `reason`

## 6. 建议的状态与版本规则

### 6.1 建议状态

建议第一版使用：

- `drafting`
- `awaiting_user_action`
- `approved`
- `executing`
- `completed`
- `cancelled`

### 6.2 版本规则建议

- 每次用户动作最多生成一个新版本
- 前端提交动作时必须带 `draft_version`
- 版本不匹配时后端返回冲突错误和最新摘要

## 7. 统一动作协议建议

### 7.1 当前现实

当前用户动作是分散表达的：

- 表选择靠 `selected_table_id(s)`
- 成本确认靠 `force_execute`
- 语义修订基本没有统一结构

### 7.2 目标态建议

建议未来引入：

- `draft_actions`

建议第一版动作类型包括：

- `choose_table`
- `change_table`
- `manual_select_table`
- `choose_option`
- `confirm_draft`
- `revise_field`
- `add_context`
- `approve_execution`
- `reject_execution`
- `abandon_and_new_query`
- `give_feedback`
- `remember_preference`
- `cancel_query`

### 7.3 动作对象建议

建议最小字段如下：

- `action_id`
- `query_id`
- `draft_id`
- `draft_version`
- `action_type`
- `actor_type`
- `actor_id`
- `payload_json`
- `idempotency_key`
- `created_at`

### 7.4 动作应用语义建议

仅有 `draft_actions` 对象定义还不够，后续必须把动作处理语义写死：

1. 先校验 `query_id`、`draft_id`、`draft_version`
2. 再校验 `action_id` 或 `idempotency_key` 是否重复
3. 在同一事务内写入 `draft_actions`
4. 在同一事务内推进 `query_drafts` 与 `query_sessions`
5. 提交后再异步发出学习事件、记忆提炼或治理候选任务

建议明确：

- 同一个动作重复提交时，应返回已有结果，不应重复推进状态
- 版本不匹配时，不应“尽量帮用户继续执行”，而应返回冲突与最新摘要
- 一个动作最多推进一次主状态，不允许一个请求里隐式生成多个产品动作

### 7.4.1 待确认阶段自然语言回复的解析规则

目标态中，用户在确认阶段继续输入自然语言，不应直接绕过动作协议。

建议固定为：

- 当当前 `query_id` 仍有 `pending_actions` 时，新的自由文本先进入 `pending_reply_resolution`
- 该步骤只负责判断：
  - 这是在回应当前 pending
  - 这是在开启一个新问题
  - 还是还需要一句极短澄清

如果判断为回应当前 pending：

- 再把它落成正式 `draft_action`
- 例如 `choose_table`、`confirm_draft`、`revise_field`、`approve_execution`

如果判断为新问题：

- 输出 `abandon_and_new_query`
- 结束当前等待态
- 在同一会话中创建新的 `query_id`

如果判断不稳定：

- 不直接硬猜
- 只追加一轮极短澄清

### 7.5 动作与事件边界建议

建议后续固定以下分层：

- `draft_actions`
  - 记录“用户或系统做了什么产品动作”
- `learning_events`
  - 记录“这些动作和结果沉淀成了什么学习事实”

一个动作可以派生多个事件，但事件不应反向替代动作对象本身。

## 8. 第一阶段建议覆盖的澄清类型

第一阶段不建议铺满所有语义澄清，建议只做高价值通用类型：

- `table_ambiguity`
- `metric_ambiguity`
- `time_range_ambiguity`
- `grain_ambiguity`

## 9. 前后端接口建议

### 9.1 当前兼容策略

第一阶段建议继续兼容：

- 初始查询仍走 `POST /api/query`
- 表选择确认仍复用 `selected_table_id` / `selected_table_ids`
- 高成本确认仍兼容 `force_execute`

### 9.2 未来建议新增

建议未来新增：

- `POST /api/query-drafts/{draft_id}/actions`

建议返回：

- 最新 `Query Draft`
- 当前状态
- 下一步可执行动作
- 如果已自动推进，则返回新的执行状态

### 9.3 流式续跑与兼容规则建议

为了兼容当前 `WS /api/query/stream` 主链路，建议明确：

- 初始查询继续走 `POST /api/query` 或 `WS /api/query/stream`
- 动作提交可以走 `POST /api/query-drafts/{draft_id}/actions`
- 但动作推进后的产品级查询仍应复用原 `query_id`
- 如果动作导致系统继续执行，前端可以重建流式订阅，但不应新建业务级查询 ID
- 过渡期 `selected_table_id(s)`、`force_execute`、`original_query_id` 继续保留，仅作为兼容桥接，不再代表最终协议

## 10. 分阶段建议

### 第一阶段

- 不重做前端整页
- 先复用现有确认卡和表选择卡承载能力
- 后端先新增最小动作对象
- 如 `query_sessions.state_json` 无法承载前端读取，再补最小草稿对象

### 第二阶段

- 再补“为什么这样理解”
- 再补更细粒度字段修订

## 11. 当前不应写成事实的项

以下内容当前还不是事实，只能作为建议：

- 系统已经有 `Query Draft`
- 表选择和执行审批已经统一进同一动作协议
- 前端已经有完整语义级澄清交互
