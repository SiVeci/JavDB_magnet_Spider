import { defineStore } from 'pinia'
import { ref } from 'vue'
import { apiFetch } from '@/api'
import type { RuntimeConfig } from '@/types'
import { STORAGE_KEYS } from '@/constants/storageKeys'

const DEFAULT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'

export const useSettingsStore = defineStore('settings', () => {
  const config = ref<RuntimeConfig>({
    cookie: '',
    remember_cookie: false,
    user_agent: DEFAULT_UA,
    proxies: '',
    trackers: [],
    has_cookie: false,
    cookie_source: 'unknown',
    cookie_captured_at: 0,
    cookie_validated_at: 0,
    cookie_status: 'missing',
    cookie_last_error: '',
  })

  function saveCookieCache() {
    localStorage.setItem(STORAGE_KEYS.rememberCookie, config.value.remember_cookie ? '1' : '0')
    localStorage.removeItem(STORAGE_KEYS.cookie)
    sessionStorage.removeItem(STORAGE_KEYS.cookie)
  }
  async function load() {
    const remember = localStorage.getItem(STORAGE_KEYS.rememberCookie) === '1'
    config.value.remember_cookie = remember
    config.value.cookie = ''

    const res = await apiFetch('/api/runtime_config').then((r: Response) => r.json())
    if (res.code !== 200) return
    const data = res.data
    config.value.remember_cookie = !!data.remember_cookie
    config.value.cookie = ''
    config.value.user_agent = data.user_agent || localStorage.getItem(STORAGE_KEYS.ua) || DEFAULT_UA
    config.value.proxies = data.proxies || localStorage.getItem(STORAGE_KEYS.proxy) || ''
    config.value.trackers = data.trackers || []
    config.value.has_cookie = !!data.has_cookie
    config.value.cookie_source = data.cookie_source || 'unknown'
    config.value.cookie_captured_at = data.cookie_captured_at || 0
    config.value.cookie_validated_at = data.cookie_validated_at || 0
    config.value.cookie_status = data.cookie_status || (data.has_cookie ? 'unverified' : 'missing')
    config.value.cookie_last_error = data.cookie_last_error || ''
  }

  async function save(showMsg = false): Promise<string> {
    saveCookieCache()
    localStorage.setItem(STORAGE_KEYS.ua, config.value.user_agent)
    localStorage.setItem(STORAGE_KEYS.proxy, config.value.proxies)
    const res = await apiFetch('/api/runtime_config', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        cookie: config.value.cookie,
        remember_cookie: config.value.remember_cookie,
        user_agent: config.value.user_agent,
        proxies: config.value.proxies,
        trackers: config.value.trackers,
      }),
    }).then((r: Response) => r.json())
    config.value.cookie = ''
    return showMsg ? (res.msg || 'Saved') : ''
  }

  async function checkCookie(): Promise<string> {
    const res = await apiFetch('/api/auth/check_cookie', { method: 'POST' }).then((r: Response) => r.json())
    await load()
    return res.msg || res.data?.message || 'Cookie check finished'
  }

  function parseProxy(): { host: string; port: string } {
    const val = config.value.proxies || ''
    const clean = val.replace(/^https?:\/\//, '')
    const idx = clean.lastIndexOf(':')
    return {
      host: idx > 0 ? clean.slice(0, idx) : clean,
      port: idx > 0 ? clean.slice(idx + 1) : '',
    }
  }

  function setProxy(host: string, port: string) {
    if (!host && !port) { config.value.proxies = ''; return }
    config.value.proxies = `http://${host.replace(/^https?:\/\//, '')}:${port}`
  }

  return { config, load, save, checkCookie, saveCookieCache, parseProxy, setProxy, DEFAULT_UA }
})
