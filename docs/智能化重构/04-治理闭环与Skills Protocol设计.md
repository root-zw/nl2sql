# 治理闭环与Skills Protocol设计

## 1. 文档信息

- 状态：草案
- 对应总文档章节：`00-智能化重构总方案.md`
- 最后更新时间：2026-04-17

## 2. 设计目标

本模块希望明确两件事：

- 学习结果如何进入正式治理，而不是长期停留在运行时补丁
- 当前系统里哪些能力适合抽象成统一协议的能力单元

## 3. 当前现实

### 3.1 当前已存在的治理基础

当前系统已经有比较清晰的正式治理基础：

- `fields`
- `field_enum_values`
- `table_relationships`
- `global_rules`
- `metadata_change_log`
- Prompt 模板管理
- 模型供应商管理
- 元数据导入导出
- Milvus 同步

### 3.2 当前仍不存在的对象

当前仓库还没有：

- `governance_candidates`
- 候选审核流
- 统一的 `Skill Protocol`
- 独立的 `Skills Runtime`

### 3.3 当前最接近 Skill 的能力单元

当前最接近未来 `Skill` 的是这些已有能力：

- 表选择
- 动态 Prompt 构建
- NL2IR 解析
- 编译与执行守卫
- 结果解释
- 字段分析
- 关系检测
- 同步发布

### 3.4 当前更合理的近期收敛

从当前代码现状出发，这篇文档不应被理解成“治理闭环”和“Skills Protocol”要同优先级同时启动。

更合理的近期收敛是：

- 先把运行收益收敛到 `governance_candidates`
- 先把审核后的变更通过发布适配器写回现有正式层
- 如确实出现多处重复能力边界，再为少量高价值能力补最小协议描述

`Skill Protocol` 更适合作为第二阶段增强，`Skills Runtime` 更适合作为北极星能力，而不是当前第一波硬目标。

## 4. 治理层边界建议

### 4.1 当前应坚持的方向

如果后续建设治理闭环，建议坚持：

- 运行时记忆不直接等于正式标准
- 稳定模式先形成待审候选
- 发布仍回写当前正式层
- 正式层变更后继续走现有同步链路进入 Milvus

### 4.2 不建议的方向

不建议：

- 再造一套平行正式语义系统
- 让在线能力直接越权改写正式表
- 让运行时记忆长期挂靠正式治理职责

## 5. 候选层设计建议

### 5.1 建议新增对象

建议未来引入：

- `governance_candidates`

### 5.2 第一版建议候选类型

- `field_alias`
- `field_display_update`
- `enum_normalization`
- `default_filter_rule`
- `derived_metric_rule`
- `join_candidate`

### 5.3 第一版建议状态

- `observed`
- `candidate`
- `approved`
- `published`
- `rejected`
- `superseded`

## 6. 发布适配器建议

如果后续做治理发布，建议不要新建平行知识库，而是做“发布适配器”：

- 候选审核通过后
- 适配器把变更翻译成当前正式层写入动作
- 再触发已有同步能力

建议第一版优先映射到：

- `fields`
- `field_enum_values`
- `global_rules`
- `table_relationships`

## 7. Skill Protocol建议

### 7.1 当前定位

这里的 `Skill` 不是插件市场，不是外部 Agent 插件，而是仓库内部能力单元的统一描述协议。

### 7.2 协议目标

建议协议满足：

- 轻量
- 可测试
- 可观测
- 框架无关
- 能声明副作用边界

### 7.3 建议字段

- `name`
- `purpose`
- `input_schema`
- `output_schema`
- `requires_confirmation`
- `can_write_memory`
- `can_write_governance`
- `side_effect_level`
- `fallback_policy`
- `observability_tags`

### 7.4 副作用等级建议

- `none`
- `runtime_memory`
- `governance_candidate`
- `published_semantic_write`

第一阶段不建议允许普通在线能力直接进入 `published_semantic_write`。

## 8. 与框架的关系

### 8.1 当前可以说的结论

当前可以保留的结论是：

- `Skill Protocol` 应先于具体运行时确定
- 协议不应绑死在某个框架函数签名上

### 8.2 当前不应说成定论的结论

当前不应写成：

- `LangGraph` 已经是本仓库主运行时
- 未来一定要有独立 `skills/` 目录

这些都还没有被当前仓库实现验证。

## 9. 第一阶段建议

### 第一阶段建议实现

- 最小 `governance_candidates`
- 最小候选状态流
- 一个面向现有正式层的发布适配器
- 如确有重复边界，再补一到两个高价值能力单元的最小协议描述

### 第一阶段不建议实现

- 复杂插件市场
- 大规模动态技能编排
- 全自动治理发布
- 独立 `Skills Runtime`

## 10. 当前不应写成事实的项

以下内容当前都还不是事实：

- 系统已经有治理候选池
- 当前已经有统一 Skill Runtime
- 当前已完成协议化技能包装
