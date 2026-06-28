<script setup lang="ts">
import { ref, computed } from 'vue'
import { useSettingsStore } from '@/stores/settings'
import { useToast } from '@/composables/useToast'
import { useAuthBrowser } from '@/composables/useAuthBrowser'

const settings = useSettingsStore()
const { showToast } = useToast()
const authBrowser = useAuthBrowser()

const proxyParsed = computed(() => settings.parseProxy())
const proxyHost = ref(proxyParsed.value.host)
const proxyPort = ref(proxyParsed.value.port)
const trackerText = computed({
  get: () => settings.config.trackers.join('\n'),
  set: (v) => { settings.config.trackers = v.split(/\r?\n/).map(s => s.trim()).filter(Boolean) },
})
const cookieSourceLabel = computed(() => ({
  manual: '手动粘贴',
  android_webview: 'Android WebView',
  auth_browser: 'Auth Browser Service',
  unknown: '未知',
}[settings.config.cookie_source || 'unknown'] || '未知')
)
const cookieStatusLabel = computed(() => ({
  missing: '未配置',
  unverified: '未验证',
  valid: '有效',
  invalid: '无效',
  expired: '疑似过期',
  network_error: '网络错误',
  blocked: '被拦截',
}[settings.config.cookie_status || 'missing'] || '未验证')
)
function formatTime(value?: number): string {
  return value ? new Date(value * 1000).toLocaleString() : '-'
}
const isAndroidClient = computed(() => /Android/i.test(navigator.userAgent || ''))
const showAuthBrowserDetails = computed(() =>
  !isAndroidClient.value && settings.config.cookie_source !== 'android_webview'
)
const hostError = ref(false)
const portError = ref(false)

function validateProxy(): boolean {
  const h = proxyHost.value.trim()
  const p = proxyPort.value.trim()
  hostError.value = false
  portError.value = false
  if (!h && !p) { settings.setProxy('', ''); return true }
  if (!h) { hostError.value = true }
  if (!p) { portError.value = true }
  if (hostError.value || portError.value) { showToast('代理地址和端口必须同时填写'); return false }
  settings.setProxy(h, p)
  return true
}

async function handleSave() {
  if (!validateProxy()) return
  try {
    const msg = await settings.save(true)
    if (msg) showToast(msg)
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '保存失败')
  }
}

async function openAuthBrowser() {
  try {
    const data = await authBrowser.start()
    showToast(data.viewer_url ? '远程登录入口已打开，请登录后再点击获取 Cookie' : '授权浏览器已启动，请在弹出的 Auth Browser 窗口登录后再点击获取 Cookie')
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '无法打开登录页')
  }
}

async function testAuthBrowserConnection() {
  try {
    const res = await authBrowser.checkHealth()
    if (res.code !== 200) { showToast(res.msg || 'Auth Browser Service 不可用'); return }
    showToast('Auth Browser Service 可连接')
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : 'Auth Browser Service 不可用')
  }
}

async function captureAuthCookie() {
  try {
    const res = await authBrowser.capture(settings.config.remember_cookie)
    await settings.load()
    showToast(res.msg || 'Cookie 已捕获并保存')
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : 'Cookie 捕获失败')
  }
}

async function checkCookie() {
  try {
    const msg = await settings.checkCookie()
    showToast(msg)
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : 'Cookie 检测失败')
  }
}
</script>

<template>
  <section class="max-w-3xl mx-auto">
    <section class="card overflow-hidden">
      <div class="card-head">
        <h2 class="card-title">全局运行配置</h2>
        <button
          type="button"
          @click="handleSave"
          class="btn btn-icon-md btn-neutral"
          title="保存全局配置"
          aria-label="保存全局配置"
        >
          <svg
            viewBox="0 0 24 24"
            aria-hidden="true"
            class="h-4 w-4"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
            stroke-linecap="round"
            stroke-linejoin="round"
          >
            <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2Z" />
            <path d="M17 21v-8H7v8" />
            <path d="M7 3v5h8" />
          </svg>
        </button>
      </div>
      <div class="p-5 space-y-4 text-sm">
        <div>
          <div class="flex justify-between items-center mb-1">
            <label class="text-[color:var(--c-text-muted)]">Cookie</label>
            <a href="https://javdb.com" target="_blank" class="text-xs text-info-text underline">打开 JavDB</a>
          </div>
          <textarea
            v-model="settings.config.cookie"
            rows="1"
            class="input input-mono h-10 min-h-10 resize-y"
          ></textarea>
          <label class="mt-2 flex items-center gap-2 text-xs text-[color:var(--c-text-muted)]">
            <input
              v-model="settings.config.remember_cookie"
              type="checkbox"
              class="w-4 h-4 accent-[color:var(--c-primary)]"
            />
            <span>记住 Cookie（写入后端数据库）</span>
          </label>
          <div
            v-if="showAuthBrowserDetails"
            class="mt-3 grid gap-2 rounded-lg border border-[color:var(--c-border-soft)] bg-surface-sunken p-3 text-xs"
          >
            <div class="grid grid-cols-2 gap-2 sm:grid-cols-3">
              <div><span class="text-[color:var(--c-text-muted)]">Cookie：</span>{{ settings.config.has_cookie ? '已配置' : '未配置' }}</div>
              <div><span class="text-[color:var(--c-text-muted)]">来源：</span>{{ cookieSourceLabel }}</div>
              <div><span class="text-[color:var(--c-text-muted)]">状态：</span>{{ cookieStatusLabel }}</div>
              <div><span class="text-[color:var(--c-text-muted)]">最近获取：</span>{{ formatTime(settings.config.cookie_captured_at) }}</div>
              <div><span class="text-[color:var(--c-text-muted)]">最近验证：</span>{{ formatTime(settings.config.cookie_validated_at) }}</div>
              <div class="min-w-0 truncate" :title="settings.config.cookie_last_error || ''">
                <span class="text-[color:var(--c-text-muted)]">最近错误：</span>{{ settings.config.cookie_last_error || '-' }}
              </div>
            </div>
            <div class="flex flex-wrap gap-2">
              <button type="button" @click="checkCookie" class="btn btn-sm btn-info">检测 Cookie</button>
              <button type="button" @click="openAuthBrowser" class="btn btn-sm btn-soft">重新获取 Cookie</button>
            </div>
          </div>
          <div
            v-if="showAuthBrowserDetails"
            class="mt-3 rounded-lg border border-[color:var(--c-border-soft)] bg-surface-sunken p-3"
          >
            <div class="mb-2 flex flex-wrap items-center justify-between gap-2">
              <span class="text-xs font-semibold text-[color:var(--c-text-muted)]">Auth Browser: {{ authBrowser.status.value }} / {{ authBrowser.health.value }}</span>
              <button
                v-if="authBrowser.sessionId.value"
                type="button"
                @click="authBrowser.refreshStatus"
                class="btn btn-sm btn-soft"
              >刷新状态</button>
            </div>
            <div class="flex flex-wrap gap-2">
              <button
                type="button"
                @click="testAuthBrowserConnection"
                :disabled="authBrowser.loading.value"
                class="btn btn-sm btn-soft"
              >测试连接</button>
              <button
                type="button"
                @click="openAuthBrowser"
                :disabled="authBrowser.loading.value"
                class="btn btn-sm btn-info"
              >打开登录页获取 Cookie</button>
              <button
                type="button"
                @click="captureAuthCookie"
                :disabled="authBrowser.loading.value || !authBrowser.sessionId.value"
                class="btn btn-sm btn-warning"
              >我已登录，获取 Cookie</button>
            </div>
          </div>
        </div>
        <div>
          <label class="field-label">User-Agent</label>
          <input v-model="settings.config.user_agent" type="text" class="input input-mono" />
        </div>
        <div>
          <label class="field-label">HTTP 代理</label>
          <div class="flex gap-2">
            <input
              v-model="proxyHost"
              type="text"
              :class="['input input-mono w-2/3', hostError ? 'is-invalid' : '']"
              placeholder="127.0.0.1"
            />
            <input
              v-model="proxyPort"
              type="text"
              :class="['input input-mono w-1/3', portError ? 'is-invalid' : '']"
              placeholder="7890"
            />
          </div>
        </div>
        <div>
          <label class="field-label">Tracker 列表</label>
          <textarea
            v-model="trackerText"
            rows="1"
            class="input input-mono h-10 min-h-10 resize-y"
            placeholder="一行一个 tracker URL"
          ></textarea>
        </div>
      </div>
    </section>
  </section>
</template>
