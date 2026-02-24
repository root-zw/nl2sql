<template>
  <div class="model-config-manage">
    <el-tabs v-model="activeTab" type="border-card">
      <el-tab-pane label="提示词管理" name="prompts">
        <PromptManage />
      </el-tab-pane>
      <el-tab-pane label="模型供应商" name="providers">
        <ModelProviderManage />
      </el-tab-pane>
      <el-tab-pane label="环境变量配置" name="env">
        <el-card>
          <template #header>
            <div class="card-header">
              <span>⚙️ 环境变量模型配置</span>
              <el-tag type="info" size="small">只读</el-tag>
            </div>
          </template>
          <ModelConfigPanel />
        </el-card>
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import ModelConfigPanel from './ModelConfigPanel.vue'
import ModelProviderManage from './ModelProviderManage.vue'
import PromptManage from './PromptManage.vue'

const activeTab = ref('prompts')
</script>

<style scoped>
.model-config-manage {
  padding: 0;
  width: 100%;
}

.model-config-manage :deep(.el-tabs__content) {
  padding: 16px;
}

.model-config-manage .el-card {
  width: 100%;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 16px;
  font-weight: 600;
}

@media screen and (max-width: 768px) {
  .card-header {
    font-size: 15px;
  }
}
</style>


