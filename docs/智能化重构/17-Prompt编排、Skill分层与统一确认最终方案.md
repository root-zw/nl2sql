# Prompt编排、Skill分层与统一确认最终方案

## 1. 文档信息

- 状态：收敛稿
- 类型：开发前置设计文档
- 最后更新时间：2026-04-17

## 2. 文档目标

本文档用于把以下几个此前分散讨论的问题收敛成一套可开发的最终形态方案：

- 表选择确认、意图确认、执行确认最终应如何统一
- Prompt 在最终系统里应如何分层、组装和治理
- `Skill` 在最终系统里是否需要存在，以及它与 Prompt 的关系
- 当前代码、现有 RFC 和目标态之间有哪些断层
- 后续开发应优先改哪些层，而不是继续堆单点提示词

本文档不代表这些能力当前已经落地；它是基于当前代码现状、已有 RFC 和讨论结论形成的开发前置方案。

## 2.1 2026-04-17 实施校正

本轮改造后，以下内容已经不再只是设计稿：

- `query_sessions` 已落地，并提供 `GET /api/query-sessions/{query_id}`
- `draft_actions` 已落地，并提供 `POST /api/query-sessions/{query_id}/actions`
- `Chat.vue` 已接入统一确认容器
  - 确认阶段按钮动作走 `draft_actions`
  - 确认阶段输入框自由回复也走 `draft_actions`
  - `table_resolution` / `draft_confirmation` / `execution_guard` 三段按钮都已补齐
  - 结果态已提供“不是这张表，重新选表”按钮
- `draft_confirmation` 已接通自动重写闭环
  - 用户 `revise` 后会在同一 `query_id` 下自动重算草稿
  - 系统会重新停在 `draft_confirmation` 等待用户再次确认
- 高置信度自动选表成功后，后端会保留 `candidate_snapshot`
  - 用户可在后续把当前表推翻并回退到 `table_resolution`
- `CONFIRMATION_MODE` 已落地
  - 当前仓库默认值为 `always_confirm`

但以下内容仍然只是目标态，不应误读为已完成：

- 显式 `query_drafts`
- `learning_events` / `runtime_memories` / `governance_candidates`
- LangGraph / 独立 Skills Runtime

## 3. 当前代码事实确认

### 3.1 当前稳定在线的三类确认

当前在线链路中真正稳定工作的确认已有三类：

- 表选择确认
- 草稿确认
- 高成本执行确认

其中 `draft_confirmation` 已能承接“用户修改意见 -> 自动重算草稿 -> 再确认”的完整闭环。

### 3.2 当前前后端确认协议已进入“后端统一、前端兼容过渡”阶段

当前响应状态仍以以下枚举为主：

- `success`
- `confirm_needed`
- `table_selection_needed`
- `error`

当前前端已经新增基于 `query_sessions` 的统一确认容器，但仍保留：

- `pendingTableSelection`
- `pendingConfirm`

作为兼容回退。

因此截至 2026-04-17，更准确的判断是：

- 后端状态与动作协议已经统一
- 前端主路径已经统一到 `query_sessions`
- 旧的两套确认状态仍保留为 fallback，而不是主协议

### 3.3 当前 LLM 选表路径是可走通的

基于当前代码，`LLM表选择 -> 用户确认 -> 继续 NL2IR` 这条路径是闭环可走的：

- 后端在未指定表时先走 `llm_select_table`
- 若需确认则返回 `table_selection_needed`
- 前端确认后带 `selected_table_ids`、`multi_table_mode`、`original_query_id` 重新发起请求
- 后端第二次请求会直接使用已选表继续进入 `parser.parse()`

因此“LLM选表路径是否走得通”这一点，当前代码答案是肯定的。

### 3.4 当前默认链路还不能一次给出“待选表 + 最终 IR 确认”

虽然当前仓库里已经存在“一次调用同时选表并生成 IR”的潜在能力，但默认主链路并没有把它暴露成统一确认协议。

当前默认 LLM 选表路径的关键限制是：

- 在未选表时，如表选择需要确认，会直接提前返回
- 这一轮不会继续进入 `parser.parse()`
- 因此不会得到真正基于已确认表生成的最终 IR 确认内容

这说明当前系统还不具备真正意义上的“一次交互中同时完成表选择和最终意图确认”的稳定协议。

### 3.5 当前 Prompt 组装现实

当前 Prompt 组装现实有三个重要特点：

- `table_selector` 已经具备 `system + user_template + function_schema`
- `nl2ir` 当前只有 `system + function_schema`
- `nl2ir` 的 user prompt 主要由代码动态拼接、再由枚举策略二次增强

这意味着：

- Prompt 资产形态在不同场景之间并不统一
- `NL2IR` 缺少独立可治理的 `user_template`
- Prompt 组装逻辑散落在多个模块中，而不是被统一抽象

### 3.6 当前治理资产和 Prompt 之间的关系已部分存在，但还未成体系

当前仓库已经有正式治理资产：

- `fields`
- `field_enum_values`
- `table_relationships`
- `global_rules`
- `prompt_templates`

同时，在线链路已经会消费其中一部分资产：

- `derived_metric`
- `custom_instruction`
- `default_filter`

但当前仍未形成一套完整的“Prompt 编排协议”来明确：

- 哪些治理对象进入 Prompt
- 哪些治理对象只作用于编译或执行
- 哪些内容只是运行时证据，不属于正式治理

### 3.7 当前 Skill 仍不是已实现事实

当前仓库和现有依赖中都没有真正运行中的：

- 统一 `Skill Protocol`
- 独立 `Skills Runtime`
- 基于 Skill 的主查询编排运行时

因此：

- 当前可以谈 `Skill` 作为目标态能力抽象
- 但不能把它写成已经落地的系统事实
- 也不应把“先引入 Skills Runtime”设为第一波开发目标

## 4. 最终形态的核心判断

### 4.1 最终目标不是“更长的 Prompt”

最终目标不是继续把系统提示词越写越长，而是把当前大 Prompt 拆成：

- 核心契约
- 技能块
- 治理策略块
- 运行时证据块
- 状态上下文块
- 输出结构约束

也就是说，后续的重点不是“再补规则”，而是“把规则归位”。

### 4.2 最终产品不是“双卡片确认”，而是“统一确认读模型”

最终交互不应长期维持：

- 一张表选择卡
- 一张理解确认卡

最终应统一成一个用户可见的确认读模型，在一个对话式确认容器里按层展示：

- `safe_summary`
- `table_resolution`
- `provisional_draft`
- `confirmed_draft`
- `execution_guard`
- `pending_actions`

### 4.3 没有确认表时，不能把第二阶段内容当成最终确认

如果用户尚未确认表，系统不能把第二阶段内容标记成“最终理解已确认”。

此时只能展示以下两类内容：

- 表无关、治理安全的理解摘要
- 基于推荐表生成的暂定草稿

因此最终协议里必须显式区分：

- `safe_summary`
- `provisional_draft`
- `confirmed_draft`

### 4.4 LangGraph 和独立 Skills Runtime 都不是第一波前提

基于当前仓库现实，后续可以评估：

- `LangGraph`
- `Skill Protocol`
- 独立 `Skills Runtime`

但它们都不应成为第一波开发前提。

第一波真正的前提应是：

- 先固定状态对象
- 先固定动作协议
- 先固定 Prompt 组装协议
- 先固定统一确认读模型

## 5. 最终交互方案

### 5.1 最终交互原则

最终交互应坚持：

- chat-first
- 统一确认容器
- 明确暂定与已确认边界
- 所有继续执行动作复用同一个 `query_id`
- 用户动作结构化写入 `draft_actions`

### 5.2 统一确认读模型建议

建议最终形成统一确认读模型，至少包含以下部分：

- `query_id`
- `session.status`
- `context.safe_summary`
- `table_resolution`
- `draft`
- `execution_guard`
- `pending_actions`
- `dependency_meta`

建议最小结构如下：

```json
{
  "query_id": "uuid",
  "session": {
    "status": "awaiting_user_action",
    "current_node": "table_resolution"
  },
  "context": {
    "safe_summary": {
      "user_goal_summary": "用户想查询近三年城镇住宅用地面积变化",
      "domain_hint": "自然资源",
      "known_constraints": ["时间范围为近三年"],
      "open_points": ["需确认应使用哪张业务表"]
    }
  },
  "table_resolution": {
    "status": "awaiting_confirmation",
    "candidates": [],
    "recommended_table_ids": [],
    "reason_summary": "存在多个业务阶段相近的候选表"
  },
  "draft": {
    "status": "provisional",
    "table_dependent": true,
    "invalidate_on_table_change": true,
    "natural_language": "如果按推荐表A理解，本次查询将按年份统计住宅用地面积并比较变化",
    "draft_json": {}
  },
  "execution_guard": null,
  "pending_actions": [
    "choose_table",
    "cancel_query"
  ]
}
```

### 5.3 显示层级建议

统一确认容器中的内容应按以下优先级展示：

1. 用户问题
2. 当前理解摘要
3. 候选表与推荐理由
4. 暂定草稿或已确认草稿
5. 风险提示与执行确认
6. 当前待确认动作

### 5.4 表未确认时允许展示的内容

表未确认时允许展示：

- 业务域
- 候选表
- 表描述
- 标签
- 年份信息
- 关键维度与关键指标预览
- 暂定理解摘要
- 基于推荐表的暂定草稿

表未确认时不应展示成“最终理解”的内容包括：

- 最终指标 UUID 选择结论
- 最终过滤条件结论
- 最终 IR 确认结论
- 最终字段级枚举绑定结论

### 5.5 表已确认后必须重算的内容

一旦用户确认表，以下内容必须基于已确认表重新生成：

- 候选字段范围
- 枚举匹配
- 派生指标可用范围
- 自定义指令生效范围
- 最终 Draft / IR
- 澄清点

因此最终协议里需要有显式依赖关系：

- `invalidate_on_table_change: true`

### 5.5.1 高置信度误选表必须作为正常场景处理

最终必须明确：

- 表高置信度只意味着可以自动推进
- 不意味着表选择一定正确
- 用户必须始终可以推翻当前自动选表

因此即使在 `adaptive` 模式下跳过了 `table_resolution`：

- 结果区仍应显示“当前使用数据表”
- 仍应给出 `change_table`
- 仍应给出 `manual_select_table`

这里的 `change_table` 不应理解为“原样重试同一份提示词”，而应理解为：

- 用户否定当前自动选表
- 系统带着纠偏状态重新规划表选择

### 5.5.2 高置信度误选表的纠错优先级

建议最终固定以下优先级：

1. 优先复用上一轮候选表快照，让用户直接改选
2. 如需模型重规划，必须带上“上一轮推荐表已被否定”的状态
3. 如剩余候选仍不稳定，进入定向澄清
4. 最后进入 `manual_select_table`，由用户直接指定表

也就是说，最终兜底不是“再问一次模型”，而是“用户自己选表”。

### 5.5.3 高置信度误选表后的失效规则

只要当前使用表被推翻，以下内容都必须失效并重算：

- `provisional_draft`
- `confirmed_draft`
- `IR`
- `SQL`
- `result`

不建议对旧 IR 做局部修补，因为当前链路本质上是“先定表，再在该表结构范围内生成 IR”。

### 5.5.4 高置信度误选表的状态机建议

建议在统一确认与执行状态机中显式补上以下分支：

```text
if user_rejects_current_table:
    invalidate draft/ir/sql/result
    if candidate_snapshot_has_alternatives:
        goto table_resolution
    elif constrained_replan_available:
        goto table_resolution
    elif clarification_needed:
        goto table_clarification
    else:
        goto manual_select_table
```

### 5.5.5 高置信度误选表如何进入学习机制

这部分最终应明确接入学习闭环，但要严格区分“本轮状态”和“跨轮学习”。

建议流转顺序固定为：

1. 用户执行 `change_table` / `choose_table` / `manual_select_table`
2. 当前查询先更新 `query_sessions.state_json`
3. 同时记录 `draft_actions`
4. 再派生对应 `learning_events`
5. 只有重复出现且证据稳定时，才提升为 `runtime_memories`
6. 再由聚合后的收益问题进入 `governance_candidates`

建议至少记录以下学习事件：

- `auto_table_rejected`
- `table_reselected`
- `manual_table_selected`
- `table_clarification_requested`
- `table_selection_confirmed`

建议至少记录以下事件载荷：

- `original_recommended_table_ids`
- `final_selected_table_ids`
- `rejected_table_ids`
- `selection_mode`
- `question_snapshot`
- `retry_reason`

需要明确边界：

- 单次用户否定自动选表，不应直接改写正式标签或元数据
- 单次 `change_table` 也不应直接固化为长期记忆
- 只有跨查询重复出现的稳定模式，才应进入长期学习或治理候选

因此最终学习闭环不是：

- 一次用户改表 -> 永久规则

而应是：

- 一次用户改表 -> 记录交互事实
- 多次重复模式 -> 形成运行时记忆
- 稳定高收益问题 -> 进入治理发布链路

### 5.6 确认策略建议

最终不建议只做一个布尔意义上的“是否确认”开关，更合理的是引入确认策略：

- `adaptive`
  - 默认模式
  - 按置信度、歧义度和风险等级决定是否需要用户确认
- `always_confirm`
  - 每次提问都先进入统一确认容器
  - 即使表选择和草稿都高置信度，也先展示确认结果再继续

如后续确有需要，可再扩展：

- `risk_only`
  - 仅在高风险执行或强制人工决策点才确认

第一阶段如需简化实现，可以先只做：

- `adaptive`
- `always_confirm`

### 5.7 确认策略的硬边界

无论采用哪种确认策略，都必须明确：

- 高成本执行确认不能被关闭
- 安全风险确认不能被关闭
- 强制人工决策点不能被关闭

也就是说：

- `confirmation_mode` 只控制语义确认 / 草稿确认
- 不控制执行保护层的强制确认

### 5.8 各策略下的行为建议

#### 5.8.1 `adaptive`

建议行为：

- 表高置信度且无明显竞争候选
  - 可跳过表确认
- 表不稳定或多表模式不稳定
  - 进入 `choose_table`
- 表已确定但 Draft 存在关键不确定点
  - 进入 `choose_option` 或 `confirm_draft`
- 成本或安全超限
  - 进入 `approve_execution`

也就是说，`adaptive` 不是“只确认一次”，而是“按需要确认”。

#### 5.8.2 `always_confirm`

建议行为：

- 每次用户提问后都先进入统一确认容器
- 如表尚未明确：
  - 展示 `safe_summary`
  - 展示候选表
  - 可展示基于推荐表的 `provisional_draft`
- 用户完成 `choose_table`
  - 系统重算 Draft
- 再展示 `confirmed_draft` 或待确认 Draft
- 如后续存在高风险执行：
  - 仍继续进入 `approve_execution`

因此在 `always_confirm` 下，推荐实现为：

- 至少一次语义确认
- 必要时叠加执行确认

### 5.9 两阶段都需要确认时的统一交互

当同一条查询同时命中：

- 表选择确认
- 草稿确认

不建议做成两套完全独立的卡片协议。

最终建议是：

- 复用同一个统一确认组件
- 按不同 stage 复用不同内容
- 在同一 `query_id` 下连续推进

建议 stage 至少包括：

- `table_resolution`
- `draft_confirmation`
- `execution_guard`

推荐交互方式：

1. 系统先输出 `table_resolution` 阶段确认
2. 用户执行 `choose_table`
3. 系统重算 Draft
4. 系统输出 `draft_confirmation` 阶段确认
5. 用户执行 `confirm_draft` 或 `choose_option`
6. 如需执行批准，再进入 `execution_guard`

### 5.10 合并确认的可选优化

在以下条件同时满足时，可考虑提供“接受推荐方案”的合并确认动作：

- 推荐表稳定
- `provisional_draft` 稳定
- 不存在关键字段级歧义

建议动作名：

- `accept_recommended_plan`

其语义等于：

- 接受推荐表
- 接受基于该推荐表的暂定草稿

但必须明确标记：

- 草稿基于推荐表生成
- 若用户改表，Draft 必须失效并重算

#### 5.10.1 待确认阶段的自然语言回复规则

最终不应把统一确认容器理解成“用户只能点按钮”的交互。

当同一 `query_id` 仍存在 `pending_actions` 时，用户如果继续在输入框输入自然语言，系统不应默认把这句话直接当成“新问题”执行，而应先进入一个内部解析步骤：

- `pending_reply_resolution`

这个步骤的目标不是生成 IR 或直接执行查询，而是把用户这句话解析成结构化动作。

建议解析结果只允许落到以下三类之一：

- `reply_to_pending`
  - 这句话是在回应当前待确认事项
- `abandon_and_new_query`
  - 这句话的语义是放弃当前 pending，转而开始一个新问题
- `need_short_clarification`
  - 当前语义不够稳定，不能安全判断到底是在继续当前问题，还是在开启新问题

如果结果是 `reply_to_pending`，则应继续映射为统一动作协议中的正式动作，例如：

- `choose_table`
- `change_table`
- `manual_select_table`
- `choose_option`
- `confirm_draft`
- `revise_field`
- `add_context`
- `approve_execution`
- `reject_execution`
- `cancel_query`

按钮点击和自然语言回复的最终落点必须一致：

- 按钮是快捷触发
- 自然语言回复先经过 `pending_reply_resolution`
- 之后统一落成 `draft_actions`

#### 5.10.2 新问题识别与兜底

最终必须承认一件事：

- 用户在待确认阶段继续输入，不一定是在回复当前 pending
- 也可能是在同一会话里直接开始一个新问题

因此系统不能把“模型总能自动分清”作为前提，而应固定以下兜底规则：

1. 如果模型高置信度判断是当前 pending 的回复：
   - 继续作用于当前 `query_id`
   - 将其解析结果落成正式 `draft_action`
2. 如果模型高置信度判断这是一个新问题：
   - 生成 `abandon_and_new_query`
   - 当前查询结束等待态
   - 在同一会话中创建新的 `query_id`
   - 新问题重新进入 `table_resolution -> draft_confirmation -> execution_guard`
3. 如果模型无法稳定判断：
   - 不直接硬猜
   - 只追加一轮极短澄清

建议澄清原则固定为：

- 优先问“你是想继续刚才这条查询，还是开始一个新问题？”
- 不应在低置信度下直接推进状态或直接执行

这意味着最终系统的真实目标态不是“按钮交互”和“自由聊天”二选一，而是：

- 外层保持 chat-first
- 内层使用统一动作协议
- 自然语言回复通过 `pending_reply_resolution` 收敛到同一套状态机

#### 5.10.3 结果完成后的追问与对比

最终目标态里，用户在结果出来后继续输入，也应保持 chat-first，而不是跳出到另一套“分析向导”。

需要明确：

- 结果后的继续分析仍在同一聊天输入框里完成
- 基于上一份结果再查另一部分并做对比，也仍在同一聊天线程里完成
- 这类输入不应只靠聊天原文硬拼，而应先识别是否需要继承结果上下文

建议新增一个内部阶段：

- `followup_context_resolution`

该阶段只负责判断：

- 这是继续分析上一份结果
- 这是基于上一份结果发起对比
- 这是一个独立新问题
- 还是还需要一句极短澄清

如果识别为前两类，则新查询在进入 `table_resolution` 之前，应先带上最小 `analysis_context`。

建议固定边界：

- `followup_context_resolution` 只做结果上下文路由
- 不直接生成最终 IR
- 不把上一份整张结果表原样塞进 Prompt
- 如果结果引用不足，只允许补一句极短澄清

### 5.11 前后端字段建议

后续建议引入统一字段：

- `confirmation_mode`
- `candidate_snapshot`
- `recommended_table_ids`
- `rejected_table_ids`
- `manual_table_override`
- `invalidated_artifacts`
- `analysis_context`

建议取值：

- `adaptive`
- `always_confirm`

建议支持两层来源：

- 用户默认偏好
- 会话级临时覆盖

建议前端使用位置：

- Chat 页统一确认区的偏好开关
- 会话级设置

建议后端使用位置：

- `query_sessions.state_json`
- 动作推进时的策略判断
- Prompt Assembly 的 `state_context_block`
- Prompt Assembly 的 `result_context_block`

### 5.12 状态推进建议

建议把确认策略也作为状态机判断输入的一部分。

可参考的推进逻辑：

```text
if execution_guard_requires_confirmation:
    goto approve_execution
elif user_rejects_current_table:
    invalidate draft/ir/sql/result
    if candidate_snapshot_has_alternatives:
        goto choose_table
    elif constrained_replan_available:
        goto choose_table
    elif clarification_needed:
        goto choose_option
    else:
        goto manual_select_table
elif confirmation_mode == "always_confirm":
    if table_not_confirmed:
        goto choose_table
    elif draft_not_confirmed:
        goto confirm_draft
    else:
        goto auto_advance
elif confirmation_mode == "adaptive":
    if table_low_confidence_or_required:
        goto choose_table
    elif draft_low_confidence_or_has_key_uncertainty:
        goto confirm_draft
    else:
        goto auto_advance
```

其中：

- `approve_execution` 优先级高于其他语义确认
- `always_confirm` 也不应绕过执行保护
- `manual_select_table` 是最终人工兜底，不应再让选表模型覆盖
- 当 `pending_actions` 非空且用户提交自由文本时，应先进入 `pending_reply_resolution`，而不是直接执行查询或直接判定为新问题

## 6. 最终 Prompt 架构

### 6.1 总体原则

最终 Prompt 体系应改造成“分层组装”，而不是长期依赖单个超长系统提示词。

建议统一采用以下结构：

- `core_contract`
- `stage_contract`
- `selected_skill_blocks`
- `published_policy_blocks`
- `runtime_evidence_blocks`
- `state_context_block`
- `result_context_block`
- `function_schema`

### 6.2 各层职责

#### 6.2.1 `core_contract`

负责承载跨场景稳定不变的内容：

- 角色职责
- 输出纪律
- 禁止行为
- UUID / schema 使用约束
- 不确定性表达原则

这部分应属于 Prompt 模板核心层，不应长期混入行业知识或租户知识。

#### 6.2.2 `stage_contract`

负责承载当前调用阶段特有的硬边界。

建议至少区分：

- `table_resolution`
- `pending_reply_resolution`
- `followup_context_resolution`
- `draft_generation`
- `execution_guard`
- `explanation`

不同阶段应各自有独立 contract，而不是依赖一个大而全的 system prompt。

其中：

- `pending_reply_resolution`
  - 只负责判断“这句话是在回复当前 pending、在开启新问题，还是需要再澄清一句”
  - 不负责直接生成最终 IR
  - 输出应优先受结构化动作约束，而不是自由文本表达
- `followup_context_resolution`
  - 只负责判断“这句话是否依赖上一份结果继续分析”
  - 只负责产出是否需要继承 `analysis_context`
  - 不负责直接生成最终 IR

#### 6.2.3 `selected_skill_blocks`

负责承载可复用的解析能力块，例如：

- 查询类型判断
- 时间解析
- 占比解析
- 同比环比
- TopN / window 细分
- 多表关联识别
- 跨年对比识别

这些内容不应永久全部驻留在 system prompt 主体中，而应按当前问题激活。

#### 6.2.4 `published_policy_blocks`

负责承载正式治理发布后的策略和事实，例如：

- 已发布的派生指标
- 已发布的自定义指令
- 已发布的领域业务阶段规则
- 已发布的 Prompt 场景模板

这里强调：

- 稳定业务语义应优先进入正式治理资产
- Prompt 不应再长期承载隐式业务补丁

#### 6.2.5 `runtime_evidence_blocks`

负责承载本轮调用的临时证据：

- 候选表
- 候选字段
- 枚举命中
- few-shot 命中
- 检索得分摘要

这部分不是正式治理，不应被误当作长期稳定规则。

#### 6.2.6 `state_context_block`

负责承载当前查询状态：

- 已确认点
- 未确认点
- 上一步动作结果
- 当前草稿版本
- 当前阶段
- 当前使用数据表
- 已否定表
- 是否进入人工选表真相

注意：

- `state_context_block` 只承载本轮查询状态
- 跨轮学习结果如需参与下一次选表，应来自 `runtime_memories` 或已发布治理资产
- 不应把一次性的用户纠错事实直接混入长期 Prompt 规则
- 基于上一份结果继续分析所需的上下文，不应混在这里，而应进入 `result_context_block`

这部分最终应来自：

- `query_sessions.state_json`
- `draft_actions`

#### 6.2.7 `result_context_block`

负责承载同一会话里“上一份结果如何参与本次新查询”的短期上下文，例如：

- 当前继承了哪一份结果
- 该结果来自哪条查询
- 当前是继续分析还是结果对比
- 当前沿用了哪些表、过滤条件、指标、维度范围
- 当前是否存在对比基准结果

注意：

- `result_context_block` 只服务同一会话内的短期追问
- 它不等于长期 `runtime_memories`
- 它也不应把完整结果大表直接塞进 Prompt
- 更适合只带 `result_ref`、范围摘要和必要的结构化结果元信息

这部分最终应来自：

- `query_sessions.state_json.analysis_context`
- 可追溯的结果引用对象，如 `conversation_messages` / `query_history` 中的结果快照摘要

### 6.3 最终 Prompt 组装公式

建议最终统一成：

`final_prompt = core_contract + stage_contract + selected_skill_blocks + published_policy_blocks + runtime_evidence_blocks + state_context_block + result_context_block + function_schema`

### 6.4 为什么当前大 Prompt 需要拆

当前大 Prompt 中大量内容其实混合了承担以下职责：

- 场景通用约束
- 查询类型知识
- 行业知识
- 误判修正
- 示例库
- 规则库

这会导致：

- 难治理
- 难缓存
- 难追溯
- 难测试
- 难灰度
- 难解释这次到底用了什么内容

因此最终必须拆。

## 7. Skill 的最终定位

### 7.1 Skill 需要存在，但不等于插件运行时

最终系统里应存在 `Skill` 概念，但这里的 `Skill` 应理解为：

- 仓库内部能力块
- 可组合
- 可测试
- 可观测
- 有明确输入输出边界

而不是：

- 插件市场
- 多 Agent 插件生态
- 第一波就要上线的独立运行时

### 7.2 Skill 与 Prompt 的关系

最终形态下：

- Prompt 是组装容器
- Skill 是可选能力块
- Governance 是正式发布后的策略源
- Runtime evidence 是本轮临时证据

因此 Skill 不替代 Prompt，Skill 是 Prompt 编排输入的一部分。

### 7.3 哪些内容适合做 Skill

适合做 Skill 的内容包括：

- 查询类型识别
- 时间解析
- 占比解析
- 同比环比解析
- 分类与过滤区分
- 记录数与度量聚合区分
- 多表关联识别
- 暂定草稿生成
- 澄清问题生成

### 7.4 哪些内容不适合做 Skill

以下内容更适合进入正式治理资产或运行时证据，而不是做 Skill：

- 某个租户的字段枚举值
- 某个领域的具体表别名列表
- 某个场景下的默认过滤值
- 某个字段的显示名和同义词
- 某个发布后的关系确认结果

### 7.5 Skill 第一阶段建议形态

第一阶段不建议实现独立 `Skills Runtime`，更合理的形态是：

- 每个 Skill 先有独立名称
- 有触发条件
- 有 Prompt block
- 有对应 few-shot 或评测样本
- 有基本的可观测标签

也就是说，先把隐式 Skill 显式化，再决定要不要 runtime。

## 8. 治理资产、Prompt 和运行时证据的边界

### 8.1 应进入 `prompt_templates` 的内容

应进入 `prompt_templates` 的内容包括：

- 场景职责
- 输出结构纪律
- 通用场景边界
- 通用表达约束

### 8.2 应进入正式治理资产的内容

应优先进入正式治理资产的内容包括：

- 表显示名、描述、标签
- 字段显示名、同义词、字段类型
- 枚举标准化
- 表关系
- 派生指标
- 默认过滤
- 自定义业务指令

### 8.3 应作为运行时证据的内容

仅应作为运行时证据存在的内容包括：

- 本轮候选表
- 本轮候选字段
- 本轮枚举命中
- 本轮检索排序
- 本轮 few-shot 命中

### 8.4 应进入编译 / 执行层而不是 Prompt 的内容

以下内容不应主要靠 Prompt 长期承载：

- 默认过滤注入
- SQL 守卫
- 成本保护
- 权限过滤
- 行级过滤

这些内容应由编译、规则或执行保护层承担。

## 9. Prompt 资产治理建议

### 9.1 当前问题

当前 Prompt 资产治理最大的问题不是“没有后台入口”，而是：

- 没有统一 Prompt 组装协议
- 模板、规则、运行时证据边界不清
- 不同场景的 Prompt 资产形态不一致
- Prompt 版本与在线效果之间难以建立稳定关联

### 9.2 最终建议新增 Prompt Assembly 层

建议未来新增统一 Prompt 组装服务，至少负责：

- 识别当前 stage
- 激活 Skill blocks
- 读取治理资产
- 读取运行时证据
- 组装最终 Prompt
- 生成 Prompt snapshot

### 9.3 Prompt Snapshot 建议

建议每次模型调用都生成可追溯的 Prompt 快照，至少记录：

- `query_id`
- `stage`
- `template_version`
- `active_skills`
- `policy_sources`
- `evidence_sources`
- `final_prompt_hash`
- `token_estimate`
- `truncation_info`

第一阶段如暂不落独立表，可先记录在：

- `query_sessions.state_json`
- trace metadata

## 10. 最终代码改造方向

### 10.1 Prompt 层

建议逐步推进以下改造：

- 为 `NL2IR` 补独立 `user_template`
- 统一 `table_selector` / `nl2ir` / `vector_table_selector` 的 Prompt 资产形态
- 统一改为通过 `prompt_loader` 读取正式配置
- 抽出统一 `Prompt Assembly Service`

### 10.2 状态与动作层

建议逐步推进：

- `query_sessions`
- `draft_actions`
- 统一确认读模型
- 暂定草稿与正式草稿的依赖关系

### 10.3 前端交互层

建议逐步推进：

- Chat 页统一确认容器
- 表选择与草稿确认合并为同一对话块
- 暂定草稿明确标记
- 确认后保持原 `query_id`
- 结果区显式展示“当前使用数据表”
- 为高置信度自动推进场景补 `change_table` / `manual_select_table`

### 10.4 治理与发布层

建议逐步推进：

- 将稳定领域知识从大 Prompt 拆入正式治理资产
- 将 Prompt 中的大量行业性“误判修正规则”逐步治理化
- 用 `governance_candidates` 承接在线收益

## 11. 文档改造建议

本方案与以下文档强相关，后续需要同步：

- `02-Query Draft与澄清交互设计.md`
  - 补 `safe_summary`、`provisional_draft`、统一确认读模型
- `05-共享契约与关键对象定义.md`
  - 补统一确认读模型与 Prompt snapshot 相关契约
- `08-治理域与发布链路设计.md`
  - 补治理资产与 Prompt / 编译 / 执行边界
- `11-用户产品交互与后台工作台设计.md`
  - 改成 chat-first 的统一确认交互
- `12-平台支撑层与外部集成设计.md`
  - 增加 Prompt Assembly 作为平台级支撑面
- `15-实施矩阵与阶段蓝图.md`
  - 在第一波开发前明确 Prompt 编排协议是前置设计约束
- `16-施工附录与代码落点清单.md`
  - 增加 Prompt Assembly、NL2IR user template、统一确认读模型的代码落点

## 12. 建议的实施顺序

建议按以下顺序推进：

1. 先冻结统一确认读模型和 Prompt 编排协议
2. 再落 `query_sessions`
3. 再落 `draft_actions`
4. 再让前端切到统一确认容器
5. 再抽 Prompt Assembly
6. 再把大 Prompt 中可治理的领域知识逐步迁出
7. 最后再评估是否需要独立 `Skill Protocol` 执行层或 `LangGraph`

## 13. 当前不应写成事实的结论

以下内容当前都还不是实现事实，只能作为目标态建议：

- 系统已经具备统一 Query Draft 确认
- 系统已经具备统一 Prompt 编排服务
- 系统已经具备独立 Skills Runtime
- 系统已经完成“表选择 + 草稿确认 + 执行确认”的统一协议
- `LangGraph` 已经是主编排骨架

## 14. 一句话收敛结论

最终形态不是“两个更长的 Prompt + 更多确认卡”，而是：

- 用 `query_sessions` 和 `draft_actions` 统一状态与动作
- 用统一确认读模型承载表选择、暂定草稿和执行确认
- 用 Prompt Assembly 分层组装 `core contract + stage contract + skills + policies + evidence + state`
- 用治理发布逐步把稳定知识从超长系统提示词迁回正式资产层
