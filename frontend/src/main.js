import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import 'element-plus/theme-chalk/dark/css-vars.css'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import zhCn from 'element-plus/es/locale/lang/zh-cn'

import App from './App.vue'
import router from './router'
import request from './utils/request'

// 全局样式
import './styles/global.css'
import './styles/admin.css'

// 全局挂载 axios
import axios from 'axios'

const app = createApp(App)
const pinia = createPinia()

// 注册Element Plus图标
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(key, component)
}

// 全局属性
app.config.globalProperties.$axios = axios
app.config.globalProperties.$request = request

app.use(pinia)
app.use(router)
app.use(ElementPlus, { locale: zhCn })

app.mount('#app')

