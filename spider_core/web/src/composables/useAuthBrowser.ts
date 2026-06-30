import { ref } from 'vue'
import { apiFetchJson, apiPost } from '@/api'
import type { ApiResponse } from '@/types'

export interface AuthLoginSession {
  session_id: string
  status: string
  login_url?: string
  needs_captcha?: boolean
  captcha_image?: string
  has_cookie?: boolean
  user_agent?: string
}

export function useAuthBrowser() {
  const sessionId = ref('')
  const status = ref('未启动')
  const health = ref('未检测')
  const loading = ref(false)
  const captchaImage = ref('')
  const needsCaptcha = ref(false)

  async function start() {
    loading.value = true
    try {
      const res = await apiPost<ApiResponse<AuthLoginSession>>('/api/auth/browser/start')
      const data = res.data
      if (!data) throw new Error(res.msg || '启动失败')
      sessionId.value = data.session_id
      status.value = data.status || 'waiting_login'
      captchaImage.value = data.captcha_image || ''
      needsCaptcha.value = !!data.needs_captcha
      return data
    } finally {
      loading.value = false
    }
  }

  async function refreshCaptcha() {
    if (!sessionId.value) return
    const res = await apiPost<ApiResponse<{ captcha_image: string }>>('/api/auth/browser/captcha', {
      session_id: sessionId.value,
    })
    captchaImage.value = res.data?.captcha_image || ''
  }

  async function checkHealth() {
    loading.value = true
    try {
      const res = await apiFetchJson<ApiResponse<{ status?: string }>>('/api/auth/browser/health')
      health.value = res.code === 200 ? '可用' : (res.msg || '不可用')
      return res
    } finally {
      loading.value = false
    }
  }

  async function login(email: string, password: string, captcha: string, rememberCookie = true) {
    if (!sessionId.value) throw new Error('请先开始登录')
    loading.value = true
    try {
      const res = await apiPost<ApiResponse<AuthLoginSession>>('/api/auth/browser/login', {
        session_id: sessionId.value,
        email,
        password,
        captcha,
        remember_cookie: rememberCookie,
      })
      status.value = res.data?.status || 'captured'
      // 登录成功后会话已在后端销毁，清理本地状态。
      sessionId.value = ''
      captchaImage.value = ''
      return res
    } finally {
      loading.value = false
    }
  }

  async function close() {
    if (!sessionId.value) return
    const current = sessionId.value
    sessionId.value = ''
    captchaImage.value = ''
    await apiPost('/api/auth/browser/close', { session_id: current })
    status.value = 'closed'
  }

  return { sessionId, status, health, loading, captchaImage, needsCaptcha, start, refreshCaptcha, checkHealth, login, close }
}
