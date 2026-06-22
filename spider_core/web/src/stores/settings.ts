import { defineStore } from 'pinia'
import { ref } from 'vue'
import { apiFetch } from '@/api'
import type { RuntimeConfig } from '@/types'

const DEFAULT_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'

export const useSettingsStore = defineStore('settings', () => {
  const config = ref<RuntimeConfig>({
    cookie: '',
    remember_cookie: false,
    user_agent: DEFAULT_UA,
    proxies: '',
    trackers: [],
  })

  function saveCookieCache() {
    const cookie = config.value.cookie
    if (config.value.remember_cookie) {
      localStorage.setItem('javdb_remember_cookie', '1')
      localStorage.setItem('javdb_cookie', cookie)
      sessionStorage.removeItem('javdb_cookie')
    } else {
      localStorage.setItem('javdb_remember_cookie', '0')
      localStorage.removeItem('javdb_cookie')
      sessionStorage.setItem('javdb_cookie', cookie)
    }
  }

  async function load() {
    const remember = localStorage.getItem('javdb_remember_cookie') === '1'
    config.value.remember_cookie = remember
    config.value.cookie = remember
      ? (localStorage.getItem('javdb_cookie') || '')
      : (sessionStorage.getItem('javdb_cookie') || '')

    const res = await apiFetch('/api/runtime_config').then((r: Response) => r.json())
    if (res.code !== 200) return
    const data = res.data
    config.value.remember_cookie = !!data.remember_cookie
    if (data.remember_cookie && data.cookie) config.value.cookie = data.cookie
    config.value.user_agent = data.user_agent || localStorage.getItem('javdb_ua') || DEFAULT_UA
    config.value.proxies = data.proxies || localStorage.getItem('javdb_proxy') || ''
    config.value.trackers = data.trackers || []
  }

  async function save(showMsg = false): Promise<string> {
    saveCookieCache()
    localStorage.setItem('javdb_ua', config.value.user_agent)
    localStorage.setItem('javdb_proxy', config.value.proxies)
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
    return showMsg ? (res.msg || '已保存') : ''
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

  return { config, load, save, saveCookieCache, parseProxy, setProxy, DEFAULT_UA }
})
