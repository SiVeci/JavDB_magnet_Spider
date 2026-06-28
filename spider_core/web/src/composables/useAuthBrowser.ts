import { ref } from 'vue'
import { apiFetch, apiPost } from '@/api'

interface ApiResponse<T> {
  code: number
  msg?: string
  data: T
}

export interface AuthBrowserSession {
  session_id: string
  status: string
  login_url?: string
  viewer_url?: string
  has_cookie?: boolean
  user_agent?: string
}

export function useAuthBrowser() {
  const sessionId = ref('')
  const status = ref('未启动')
  const health = ref('未检测')
  const loading = ref(false)

  async function start() {
    loading.value = true
    try {
      const res = await apiPost<ApiResponse<AuthBrowserSession>>('/api/auth/browser/start')
      const data = res.data
      sessionId.value = data.session_id
      status.value = data.status || 'waiting_login'
      if (data.viewer_url) window.open(data.viewer_url, '_blank', 'noopener,noreferrer')
      return data
    } finally {
      loading.value = false
    }
  }

  async function checkHealth() {
    loading.value = true
    try {
      const res = await apiFetch('/api/auth/browser/health').then((r: Response) => r.json()) as ApiResponse<{ status?: string }>
      health.value = res.code === 200 ? '可连接' : (res.msg || '不可用')
      return res
    } finally {
      loading.value = false
    }
  }

  async function refreshStatus() {
    if (!sessionId.value) return null
    const query = encodeURIComponent(sessionId.value)
    const res = await apiFetch(`/api/auth/browser/status?session_id=${query}`).then((r: Response) => r.json()) as ApiResponse<AuthBrowserSession>
    if (res.code === 200 && res.data?.status) status.value = res.data.status
    return res.data
  }

  async function capture(rememberCookie = true) {
    if (!sessionId.value) throw new Error('请先打开登录页')
    loading.value = true
    try {
      const res = await apiPost<ApiResponse<AuthBrowserSession>>('/api/auth/browser/capture', {
        session_id: sessionId.value,
        remember_cookie: rememberCookie,
      })
      status.value = res.data?.status || 'captured'
      return res
    } finally {
      loading.value = false
    }
  }

  async function close() {
    if (!sessionId.value) return
    const current = sessionId.value
    sessionId.value = ''
    await apiPost('/api/auth/browser/close', { session_id: current })
    status.value = 'closed'
  }

  return { sessionId, status, health, loading, start, checkHealth, refreshStatus, capture, close }
}
