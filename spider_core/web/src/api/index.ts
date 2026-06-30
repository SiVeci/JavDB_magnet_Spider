import { useAuthStore } from '@/stores/auth'
import { STORAGE_KEYS } from '@/constants/storageKeys'

const AUTH_HEADER = 'X-JavDB-Token'

function getToken(): string {
  try {
    return sessionStorage.getItem(STORAGE_KEYS.authToken) || ''
  } catch {
    return ''
  }
}

function buildHeaders(extra: Record<string, string> = {}): Record<string, string> {
  const token = getToken()
  const headers: Record<string, string> = { ...extra }
  if (token) headers[AUTH_HEADER] = token
  return headers
}

async function handleUnauthorized() {
  try {
    const auth = useAuthStore()
    auth.lockForAuth()
  } catch {
    // store not ready (e.g. during init)
  }
}

export async function apiFetch(url: string, options: RequestInit = {}): Promise<Response> {
  const headers = buildHeaders((options.headers as Record<string, string>) || {})
  const response = await fetch(url, { ...options, headers })
  if (response.status === 401) {
    await handleUnauthorized()
    throw new Error('访问令牌缺失或无效')
  }
  return response
}

export async function apiFetchJson<T = unknown>(url: string, options: RequestInit = {}): Promise<T> {
  const response = await apiFetch(url, options)
  if (!response.ok) {
    let body: { msg?: string; message?: string } = {}
    try { body = await response.json() } catch { /* ignore */ }
    const msg = body.msg || body.message || `请求失败 (${response.status})`
    throw Object.assign(new Error(msg), { status: response.status, body })
  }
  return response.json() as Promise<T>
}

export async function apiPost<T = unknown>(url: string, body: unknown = null): Promise<T> {
  const options: RequestInit = { method: 'POST' }
  if (body !== null && body !== undefined) {
    options.headers = { 'Content-Type': 'application/json' }
    options.body = JSON.stringify(body)
  }
  return apiFetchJson<T>(url, options)
}

export async function apiDownloadBlob(url: string, filename: string): Promise<void> {
  const response = await apiFetch(url)
  if (!response.ok) throw new Error(`下载失败 (${response.status})`)
  const blob = await response.blob()
  const a = document.createElement('a')
  a.href = URL.createObjectURL(blob)
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(a.href), 0)
}

export async function verifyToken(token: string): Promise<boolean> {
  const response = await fetch('/api/status', { headers: { [AUTH_HEADER]: token } })
  if (response.status === 401) return false
  if (!response.ok) throw new Error('验证失败，请稍后重试')
  return true
}
