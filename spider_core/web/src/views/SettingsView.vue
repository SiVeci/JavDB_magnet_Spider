<script setup lang="ts">
import { ref, computed } from 'vue'
import { DEFAULT_SCORE_CONDITIONS, useSettingsStore } from '@/stores/settings'
import { useToast } from '@/composables/useToast'
import AuthLoginModal from '@/components/AuthLoginModal.vue'
import { toErrMsg } from '@/utils/error'

const settings = useSettingsStore()
const { showToast } = useToast()

const scoreConditionOptions = [
  { value: 'uncensored', label: '无码资源' },
  { value: 'hd', label: '高清资源' },
  { value: 'subtitle', label: '字幕资源' },
  { value: 'largest_size', label: '文件体积最大' },
] as const
type ScoreCondition = typeof scoreConditionOptions[number]['value']
const scoreLevels = [
  { key: 'magnet_score_100_condition', label: '一级优先级', score: 100 },
  { key: 'magnet_score_10_condition', label: '二级优先级', score: 10 },
  { key: 'magnet_score_1_condition', label: '三级优先级', score: 1 },
] as const
type ScoreConditionKey = typeof scoreLevels[number]['key']

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
  auth_browser: '账号登录',
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

// 账号登录弹窗开关
const loginModalOpen = ref(false)

function getScoreCondition(key: ScoreConditionKey): string {
  return settings.config[key] || ''
}

function conditionLabel(value: string): string {
  return scoreConditionOptions.find(option => option.value === value)?.label || '未设置'
}

const selectedScoreConditions = computed(() =>
  new Set(scoreLevels.map(level => getScoreCondition(level.key)))
)
const unusedScoreConditionLabel = computed(() => {
  const unused = scoreConditionOptions.find(option => !selectedScoreConditions.value.has(option.value))
  return unused?.label || '无'
})
const scoreFormula = computed(() =>
  scoreLevels.map(level => `${conditionLabel(getScoreCondition(level.key))} × ${level.score}`).join(' + ')
)
const hasInvalidScoreConditions = computed(() => {
  const values = scoreLevels.map(level => getScoreCondition(level.key))
  const validValues = values.every(value => scoreConditionOptions.some(option => option.value === value))
  return !validValues || new Set(values).size !== scoreLevels.length
})

function isConditionDisabled(levelKey: ScoreConditionKey, condition: ScoreCondition): boolean {
  if (condition === getScoreCondition(levelKey)) return false
  return scoreLevels.some(level => level.key !== levelKey && getScoreCondition(level.key) === condition)
}

function restoreDefaultScoreConditions() {
  settings.config.magnet_score_100_condition = DEFAULT_SCORE_CONDITIONS.magnet_score_100_condition
  settings.config.magnet_score_10_condition = DEFAULT_SCORE_CONDITIONS.magnet_score_10_condition
  settings.config.magnet_score_1_condition = DEFAULT_SCORE_CONDITIONS.magnet_score_1_condition
}

function validateScoreConditions(): boolean {
  if (hasInvalidScoreConditions.value) {
    showToast('磁力评分条件必须从四个支持项中选择三个且不能重复')
    return false
  }
  return true
}

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
  if (!validateScoreConditions()) return
  if (!validateProxy()) return
  try {
    const msg = await settings.save(true)
    if (msg) showToast(msg)
  } catch (err: unknown) {
    showToast(toErrMsg(err, '保存失败'))
  }
}

function openAuthBrowser() {
  loginModalOpen.value = true
}

async function onLoginSuccess(msg: string) {
  await settings.load()
  showToast(msg)
}

async function checkCookie() {
  try {
    const msg = await settings.checkCookie()
    showToast(msg)
  } catch (err: unknown) {
    showToast(toErrMsg(err, 'Cookie 检测失败'))
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
            <label class="text-muted">Cookie</label>
            <a href="https://javdb.com" target="_blank" class="text-xs text-info-text underline">打开 JavDB</a>
          </div>
          <textarea
            v-model="settings.config.cookie"
            rows="1"
            class="input input-mono h-10 min-h-10 resize-y"
          ></textarea>
          <label class="mt-2 flex items-center gap-2 text-xs text-muted">
            <input
              v-model="settings.config.remember_cookie"
              type="checkbox"
              class="w-4 h-4 accent-[color:var(--c-primary)]"
            />
            <span>记住 Cookie（写入后端数据库）</span>
          </label>
          <div
            v-if="showAuthBrowserDetails"
            class="mt-3 grid gap-2 rounded-lg border border-soft bg-surface-sunken p-3 text-xs"
          >
            <div class="grid grid-cols-2 gap-2 sm:grid-cols-3">
              <div><span class="text-muted">Cookie：</span>{{ settings.config.has_cookie ? '已配置' : '未配置' }}</div>
              <div><span class="text-muted">来源：</span>{{ cookieSourceLabel }}</div>
              <div><span class="text-muted">状态：</span>{{ cookieStatusLabel }}</div>
              <div><span class="text-muted">最近获取：</span>{{ formatTime(settings.config.cookie_captured_at) }}</div>
              <div><span class="text-muted">最近验证：</span>{{ formatTime(settings.config.cookie_validated_at) }}</div>
              <div class="min-w-0 truncate" :title="settings.config.cookie_last_error || ''">
                <span class="text-muted">最近错误：</span>{{ settings.config.cookie_last_error || '-' }}
              </div>
            </div>
            <div class="flex flex-wrap gap-2">
              <button type="button" @click="checkCookie" class="btn btn-sm btn-info">检测 Cookie</button>
              <button
                v-if="showAuthBrowserDetails"
                type="button"
                @click="openAuthBrowser"
                class="btn btn-sm btn-soft"
              >账号登录获取 Cookie</button>
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
        <div class="border-t border-soft pt-5">
          <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <h3 class="font-bold">磁力评分优先级</h3>
              <p class="mt-1 text-xs text-muted">固定分值为 100 / 10 / 1，选择每一级对应的条件。</p>
            </div>
            <button type="button" class="btn btn-sm btn-soft shrink-0" @click="restoreDefaultScoreConditions">恢复默认</button>
          </div>
          <div class="mt-4 space-y-3">
            <div
              v-for="level in scoreLevels"
              :key="level.key"
              class="flex flex-col gap-2 md:flex-row md:items-center"
            >
              <div class="flex shrink-0 items-center justify-between gap-3 md:w-40">
                <span class="font-semibold">{{ level.label }}</span>
                <span class="text-muted">{{ level.score }} 分</span>
              </div>
              <select
                v-model="settings.config[level.key]"
                :aria-label="level.label"
                class="input min-w-0 flex-1"
              >
                <option
                  v-for="option in scoreConditionOptions"
                  :key="option.value"
                  :value="option.value"
                  :disabled="isConditionDisabled(level.key, option.value)"
                >
                  {{ option.label }}
                </option>
              </select>
            </div>
          </div>
          <div class="mt-4 space-y-1 rounded-lg border border-soft bg-surface-sunken p-3 text-xs text-muted">
            <div>未参与评分：{{ unusedScoreConditionLabel }}</div>
            <div>当前规则：{{ scoreFormula }}</div>
            <div>文件体积最大：仅比较同一影片中有效磁力且大小大于 0 的候选；并列最大同时命中，大小未知或为 0 时不命中。</div>
            <div>保存设置不会自动修改历史分数；如需更新历史候选，请前往数据库集合页点击“自动选择”。</div>
            <div v-if="hasInvalidScoreConditions" class="text-danger-text">当前条件存在重复或无效值，请修正后再保存。</div>
          </div>
        </div>
      </div>
    </section>

    <!-- 账号登录弹窗：curl_cffi 直登，填账号/密码/验证码即可获取 Cookie -->
    <AuthLoginModal
      v-model:open="loginModalOpen"
      :remember-cookie="settings.config.remember_cookie"
      @success="onLoginSuccess"
    />
  </section>
</template>
