import { defineStore } from 'pinia'
import { login as loginApi, logout as logoutApi } from '@/api/auth'

export const useUserStore = defineStore('user', {
  state: () => ({
    token: localStorage.getItem('token') || '',
    user: JSON.parse(localStorage.getItem('user') || '{}')
  }),
  
  getters: {
    isLoggedIn: (state) => !!state.token,
    isSuperAdmin: (state) => state.user.role === 'super_admin',
    isTenantAdmin: (state) => state.user.role === 'tenant_admin',
    username: (state) => state.user.username || '',
    role: (state) => state.user.role || ''
  },
  
  actions: {
    async login(username, password) {
      const data = await loginApi(username, password)
      this.token = data.token.access_token
      this.user = data.user
      localStorage.setItem('token', this.token)
      localStorage.setItem('user', JSON.stringify(this.user))
    },
    
    async logout() {
      try {
        await logoutApi()
      } catch (e) {
        console.error('Logout error:', e)
      }
      this.token = ''
      this.user = {}
      localStorage.removeItem('token')
      localStorage.removeItem('user')
    }
  }
})

