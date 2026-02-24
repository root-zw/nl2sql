<template>
  <div class="custom-instruction-form">
    <el-form-item label="规则内容" prop="rule_definition.instruction">
      <el-input
        v-model="localValue.instruction"
        type="textarea"
        :rows="8"
        placeholder="请用自然语言描述业务规则，系统会将此规则传递给大模型..."
        :disabled="disabled"
      />
      <el-text type="info" size="small">
        用自然语言描述业务规则，如："当用户询问周末数据时，请注意周末包括周六和周日..."
      </el-text>
    </el-form-item>
    
    <el-form-item label="触发关键词">
      <el-select
        v-model="localValue.trigger_keywords"
        multiple
        filterable
        allow-create
        default-first-option
        :reserve-keyword="false"
        placeholder="输入关键词后按回车添加（可选）"
        style="width: 100%;"
        :disabled="disabled"
      >
      </el-select>
      <el-text type="info" size="small">
        配置后，只有问题中包含关键词时才显示此规则；不配置则总是显示
      </el-text>
    </el-form-item>
    
    <el-divider content-position="left">规则示例</el-divider>
    
    <el-collapse>
      <el-collapse-item title="示例1：时间范围规则" name="1">
        <div class="example-content">
          <p><strong>规则内容：</strong></p>
          <p>当用户询问周末相关数据时，请注意：周末的订单包括周六和周日两天的数据。如果用户只说「周末」而没有指定具体日期，请查询最近一个完整周末（周六+周日）的数据。</p>
          <p><strong>触发关键词：</strong>周末, weekend, 周六周日</p>
        </div>
      </el-collapse-item>
      
      <el-collapse-item title="示例2：数据范围规则" name="2">
        <div class="example-content">
          <p><strong>规则内容：</strong></p>
          <p>当统计订单数量时，请注意区分"订单数"和"商品数"：订单数是指订单表中的记录数，一个订单可能包含多个商品；商品数是指订单明细表中的记录数。如果用户问"有多少订单"，应统计订单数而不是商品数。</p>
          <p><strong>触发关键词：</strong>订单数, 订单数量, 多少订单</p>
        </div>
      </el-collapse-item>
      
      <el-collapse-item title="示例3：业务逻辑规则" name="3">
        <div class="example-content">
          <p><strong>规则内容：</strong></p>
          <p>系统中存在"有效订单"和"无效订单"的概念：有效订单是指已支付且未退款的订单；无效订单包括未支付、已取消、已退款的订单。当用户询问订单相关统计时，默认应该只统计有效订单，除非用户明确说要包含无效订单。</p>
          <p><strong>触发关键词：</strong>订单, 销售, 营收</p>
        </div>
      </el-collapse-item>
      
      <el-collapse-item title="示例4：字段关系说明" name="4">
        <div class="example-content">
          <p><strong>规则内容：</strong></p>
          <p>当用户问到"客户等级"相关问题时，请注意：客户等级分为普通会员、银牌会员、金牌会员、钻石会员四个级别。等级越高，享受的折扣越多。如果用户问"高等级客户"，应该包括金牌会员和钻石会员。</p>
          <p><strong>触发关键词：</strong>客户等级, 会员等级, VIP, 高级客户</p>
        </div>
      </el-collapse-item>
    </el-collapse>
    
    <el-alert
      type="success"
      :closable="false"
      show-icon
      style="margin-top: 16px;"
    >
      <template #title>
        <strong>自定义规则的优势</strong>
      </template>
      <ul style="margin: 8px 0; padding-left: 20px;">
        <li>零代码配置：无需修改系统代码</li>
        <li>自然语言：以人类可读的方式描述规则</li>
        <li>灵活通用：适用于各种复杂业务场景</li>
        <li>智能触发：根据关键词自动判断是否应用</li>
      </ul>
    </el-alert>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'

const props = defineProps({
  modelValue: {
    type: Object,
    default: () => ({})
  },
  disabled: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['update:modelValue'])

const localValue = ref({
  instruction: '',
  trigger_keywords: [],
  ...props.modelValue
})

// 防止递归更新
let isUpdating = false

watch(localValue, (newVal) => {
  if (!isUpdating) {
    isUpdating = true
    emit('update:modelValue', { ...newVal })
    setTimeout(() => { isUpdating = false }, 0)
  }
}, { deep: true })

watch(() => props.modelValue, (newVal) => {
  if (!isUpdating && JSON.stringify(newVal) !== JSON.stringify(localValue.value)) {
    isUpdating = true
    localValue.value = {
      instruction: '',
      trigger_keywords: [],
      ...newVal
    }
    setTimeout(() => { isUpdating = false }, 0)
  }
}, { deep: true, immediate: true })
</script>

<style scoped>
.custom-instruction-form {
  padding: 0;
}

.example-content {
  padding: 12px;
  background: #f5f7fa;
  border-radius: 4px;
}

.example-content p {
  margin: 8px 0;
}

.example-content strong {
  color: #409eff;
}

/* 响应式设计 */
@media screen and (max-width: 768px) {
  :deep(.el-form-item__label) {
    font-size: 13px;
  }
  
  :deep(.el-input__inner),
  :deep(.el-textarea__inner) {
    font-size: 16px;
  }
  
  .example-content {
    font-size: 12px;
  }
}
</style>

