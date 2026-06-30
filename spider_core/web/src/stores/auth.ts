import { defineStore } from 'pinia'
import { ref } from 'vue'
import { verifyToken } from '@/api'
import { toErrMsg } from '@/utils/error'
import { STORAGE_KEYS } from '@/constants/storageKeys'

export const useAuthStore = defineStore('auth', () => {
  const token = ref<string>(sessionStorage.getItem(STORAGE_KEYS.authToken) || '')
  const authRequired = ref(false)
  const locked = ref(false)
  const lockMessage = ref('')

  function setToken(t: string) {
    token.value = t
    sessionStorage.setItem(STORAGE_KEYS.authToken, t)
  }

  function clearToken() {
    token.value = ''
    sessionStorage.removeItem(STORAGE_KEYS.authToken)
  }

  function lockForAuth(message = '') {
    clearToken()
    locked.value = true
    lockMessage.value = message
  }

  function unlock() {
    locked.value = false
    lockMessage.value = ''
  }

  async function login(inputToken: string): Promise<{ ok: boolean; error?: string }> {
    if (!inputToken.trim()) return { ok: false, error: '请输入访问令牌' }
    try {
      const ok = await verifyToken(inputToken.trim())
      if (!ok) return { ok: false, error: '访问令牌缺失或无效' }
      setToken(inputToken.trim())
      unlock()
      return { ok: true }
    } catch (err: unknown) {
      const msg = toErrMsg(err, '验证失败，请稍后重试')
      return { ok: false, error: msg }
    }
  }

  async function checkInitialAuth(): Promise<{ ok: boolean; version?: string }> {
    let version = 'v?.?.?'
    try {
      const res = await fetch('/api/version').then(r => r.json())
      const payload = res.data || res
      version = payload.version || 'v?.?.?'
      authRequired.value = !!payload.auth_required
    } catch {
      lockForAuth('无法读取鉴权状态，请刷新重试')
      return { ok: false, version }
    }

    if (!authRequired.value) return { ok: true, version }

    if (!token.value) {
      lockForAuth()
      return { ok: false, version }
    }

    try {
      if (await verifyToken(token.value)) return { ok: true, version }
      lockForAuth('访问令牌缺失或无效')
      return { ok: false, version }
    } catch (err: unknown) {
      const msg = toErrMsg(err, '验证失败，请稍后重试')
      lockForAuth(msg)
      return { ok: false, version }
    }
  }

  return { token, authRequired, locked, lockMessage, setToken, clearToken, lockForAuth, unlock, login, checkInitialAuth }
})
