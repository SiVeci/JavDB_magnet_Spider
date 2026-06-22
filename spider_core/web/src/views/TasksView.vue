<script setup lang="ts">
import { ref, computed, nextTick, onMounted, onUnmounted, watch } from 'vue'
import { useTasksStore, isTerminal, isPausable, isResumable, isCancelable } from '@/stores/tasks'
import { useSettingsStore } from '@/stores/settings'
import { useToast } from '@/composables/useToast'
import { useClipboard } from '@/composables/useClipboard'
import { apiFetch } from '@/api'
import type { Task } from '@/types'

const tasks = useTasksStore()
const settings = useSettingsStore()
const { showToast } = useToast()
const { copyText } = useClipboard()

// Task config state
const startUrl = ref(localStorage.getItem('javdb_url') || '')
const filename = ref(localStorage.getItem('javdb_filename') || '')
const availableTags = ref<{ name: string; value: string }[]>([])
const selectedTags = ref<Set<string>>(new Set())
const actorBaseUrl = ref('')
const actorBaseParams = ref(new URLSearchParams())
const tagsCollapsed = ref(false)
const showTagsPanel = ref(false)
const logCollapsed = ref(true)
const logBodyEl = ref<HTMLElement | null>(null)
const logBodyHeight = ref('90px')
const LOG_MIN_HEIGHT = 90
const LOG_LAYOUT_GAP = 4
let layoutFrame: number | null = null
let layoutObserver: ResizeObserver | null = null

function buildActorUrl(tagValues: string | null = null): string {
  const params = new URLSearchParams(actorBaseParams.value)
  params.set('locale', 'zh')
  if (tagValues) params.set('t', tagValues)
  else params.delete('t')
  return `${actorBaseUrl.value}?${params.toString()}`
}

async function fetchTags() {
  if (!startUrl.value.trim()) { showToast('请先输入起始页面 URL'); return }
  await settings.save(false)
  try {
    const parsed = new URL(startUrl.value.trim())
    actorBaseUrl.value = `${parsed.origin}${parsed.pathname}`
    const p = new URLSearchParams(parsed.search)
    p.set('locale', 'zh')
    if (!p.has('sort_type')) p.set('sort_type', '0')
    p.delete('page'); p.delete('t')
    actorBaseParams.value = p
    const normalized = buildActorUrl()
    startUrl.value = normalized
    selectedTags.value.clear()
    const tags = await tasks.fetchTags(normalized)
    availableTags.value = tags
    tagsCollapsed.value = false
    showTagsPanel.value = tags.length > 0
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '获取标签失败')
  }
}

function toggleTag(value: string) {
  const set = new Set(selectedTags.value)
  if (set.has(value)) set.delete(value)
  else set.add(value)
  selectedTags.value = set
  startUrl.value = set.size ? buildActorUrl(Array.from(set).join(',')) : buildActorUrl()
}

async function addTask(crawlMode = '') {
  if (!startUrl.value.trim()) { showToast('URL 不能为空'); return }
  await settings.save(false)
  let proxy = ''
  try {
    const { host, port } = settings.parseProxy()
    if (host || port) {
      if (!host || !port) { showToast('代理地址和端口必须同时填写'); return }
      proxy = `http://${host.replace(/^https?:\/\//, '')}:${port}`
    }
  } catch { /* ignore */ }
  const payload = {
    start_url: startUrl.value.trim(),
    filename: filename.value.trim(),
    crawl_mode: crawlMode,
    cookie: settings.config.cookie,
    remember_cookie: settings.config.remember_cookie,
    user_agent: settings.config.user_agent,
    proxies: proxy,
  }
  const res = await tasks.addTask(payload)
  if (res.code === 409 && res.needs_mode) {
    const useIncremental = confirm(`检测到已有数据库集合：${displayName(res.filename || '')}\n点击"确定"使用增量，点击"取消"使用覆盖。`)
    return addTask(useIncremental ? 'incremental' : 'overwrite')
  }
  if (res.code !== 200) { showToast(res.msg || '添加任务失败'); return }
  filename.value = ''
}

async function startQueue() {
  try {
    await tasks.startQueue()
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '无法启动队列')
  }
}

async function cleanupFinished() {
  const count = tasks.queueStatus.finished_count || 0
  if (count <= 0) return
  if (!confirm(`确定清理 ${count} 个已结束任务吗？\n不会删除数据库集合或已爬取数据。`)) return
  try {
    const msg = await tasks.cleanupFinished()
    showToast(msg)
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '清理失败')
  }
}

async function doTaskAction(taskId: string, action: string, opts: { method?: string; body?: unknown; path?: string } = {}) {
  try {
    const res = await tasks.taskAction(taskId, action, opts)
    if ((res as { code?: number }).code !== 200) showToast((res as { msg?: string }).msg || '操作失败')
    else if ((res as { msg?: string }).msg) showToast((res as { msg?: string }).msg!)
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '操作失败')
  }
}

async function pauseTask(taskId: string) { await doTaskAction(taskId, 'pause') }
async function resumeTask(taskId: string) {
  await settings.save(false)
  await doTaskAction(taskId, 'resume')
}
async function cancelTask(taskId: string) {
  if (!confirm('确定取消这个任务吗？')) return
  await doTaskAction(taskId, 'cancel')
}
async function deleteTask(taskId: string) {
  if (!confirm('删除这个任务记录吗？不会删除数据库集合或已爬取数据。')) return
  await doTaskAction(taskId, '', { method: 'DELETE', path: `/api/tasks/${encodeURIComponent(taskId)}` })
}
async function refreshCookie(taskId: string) {
  await doTaskAction(taskId, 'refresh_cookie')
}
async function submitManualCookie(taskId: string) {
  const cookie = prompt('请粘贴 JavDB Cookie：')
  if (!cookie?.trim()) return
  try {
    const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}/cookie`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cookie: cookie.trim() }),
    }).then((r: Response) => r.json())
    if (res.code !== 200) { showToast(res.msg || '设置 Cookie 失败'); return }
    showToast(res.msg || 'Cookie 已更新')
    await tasks.refresh()
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '操作失败')
  }
}
async function clearLogs() {
  try {
    const res = await apiFetch('/api/clear_logs', { method: 'POST' }).then((r: Response) => r.json())
    showToast(res.msg || '已清除')
    await tasks.refresh()
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '清除失败')
  }
}
async function setTaskMode(taskId: string, mode: string) {
  await doTaskAction(taskId, 'mode', { body: { mode } })
}

async function copyIncrementalMagnets(taskId: string) {
  try {
    const links = await tasks.getIncrementalMagnets(taskId)
    if (!links.length) { showToast('暂无新增影片磁力可复制'); return }
    const copied = await copyText(links.join('\n'))
    showToast(copied ? `已复制 ${links.length} 条新增影片磁力` : '自动复制失败，请在弹窗中手动复制新增影片磁力')
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '复制失败')
  }
}

function displayName(val: string) { return String(val || '').replace(/\.csv$/i, '') }

function progressPercent(progress: string): number {
  const parts = String(progress || '0/0').split('/')
  return Number(parts[1]) ? Math.min(100, Math.round(Number(parts[0]) / Number(parts[1]) * 100)) : 0
}

const STATE_META: Record<string, { label: string; badge: string; bar: string }> = {
  pending:          { label: '排队中',      badge: 'badge-neutral', bar: 'bg-info' },
  running:          { label: '运行中',      badge: 'badge-info',    bar: 'bg-info' },
  pause_requested:  { label: '暂停中',      badge: 'badge-warning', bar: 'bg-warning' },
  paused:           { label: '已暂停',      badge: 'badge-warning', bar: 'bg-warning' },
  waiting_cookie:   { label: '等待 Cookie', badge: 'badge-warning', bar: 'bg-info' },
  waiting_choice:   { label: '等待模式',    badge: 'badge-info',    bar: 'bg-info' },
  cancel_requested: { label: '取消中',      badge: 'badge-danger',  bar: 'bg-danger' },
  canceled:         { label: '已取消',      badge: 'badge-danger',  bar: 'bg-danger' },
  finished:         { label: '已完成',      badge: 'badge-success', bar: 'bg-success' },
  failed:           { label: '失败',        badge: 'badge-danger',  bar: 'bg-danger' },
}
function stateMeta(s: string) { return STATE_META[s] || { label: s || '-', badge: 'badge-neutral', bar: 'bg-info' } }

const currentTask = computed(() => tasks.currentTask)

function scheduleFitLogLayout() {
  if (layoutFrame !== null) return
  layoutFrame = requestAnimationFrame(() => {
    layoutFrame = null
    fitLogLayout()
  })
}

function fitLogLayout() {
  if (logCollapsed.value || !logBodyEl.value) return
  const rect = logBodyEl.value.getBoundingClientRect()
  const monitorCard = logBodyEl.value.closest('.card')
  const monitorRect = monitorCard?.getBoundingClientRect()
  const outerTailHeight = monitorRect ? Math.max(0, monitorRect.bottom - rect.bottom) : 0
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0
  const documentTop = rect.top + (window.scrollY || 0)
  const main = logBodyEl.value.closest('main')
  const mainBottomPadding = main ? parseFloat(window.getComputedStyle(main).paddingBottom) || 0 : 16
  const availableHeight = Math.floor(viewportHeight - documentTop - mainBottomPadding - outerTailHeight - LOG_LAYOUT_GAP)
  logBodyHeight.value = `${Math.max(LOG_MIN_HEIGHT, availableHeight)}px`
}

function toggleLogCollapsed() {
  logCollapsed.value = !logCollapsed.value
  scheduleFitLogLayout()
}

const resizeHandler = () => scheduleFitLogLayout()
watch([logCollapsed, currentTask, showTagsPanel, tagsCollapsed], async () => {
  await nextTick()
  scheduleFitLogLayout()
})

onMounted(() => {
  window.addEventListener('resize', resizeHandler)
  if (typeof ResizeObserver !== 'undefined') {
    layoutObserver = new ResizeObserver(() => scheduleFitLogLayout())
    const view = document.getElementById('view-tasks')
    if (view) layoutObserver.observe(view)
  }
  scheduleFitLogLayout()
})

onUnmounted(() => {
  window.removeEventListener('resize', resizeHandler)
  if (layoutFrame !== null) cancelAnimationFrame(layoutFrame)
  layoutObserver?.disconnect()
})
</script>

<template>
  <section
    id="view-tasks"
    class="grid min-h-0 grid-cols-1 content-start items-start gap-4 xl:grid-cols-[430px_1fr] xl:gap-6"
  >
    <!-- 左栏：任务配置 -->
    <div class="shrink-0 space-y-2 overflow-visible">
      <section class="card overflow-hidden">
        <div class="card-head">
          <h2 class="card-title">任务配置</h2>
        </div>
        <div class="px-5 py-2 space-y-2 text-sm">
          <div>
            <label class="field-label">起始页面 URL</label>
            <div class="flex gap-2">
              <input
                v-model="startUrl"
                type="text"
                class="input flex-1"
                placeholder="https://javdb.com/actors/..."
              />
              <button type="button" @click="fetchTags" class="btn btn-md btn-info shrink-0">标签</button>
            </div>
            <div v-if="showTagsPanel" class="hidden mt-3 border border-[color:var(--c-primary-soft)] rounded-lg overflow-hidden" :class="{ '!block': showTagsPanel }">
              <button
                type="button"
                @click="tagsCollapsed = !tagsCollapsed"
                class="w-full bg-[color:var(--c-primary-soft)] px-3 py-2 text-xs font-bold text-[color:var(--c-primary-text)] flex items-center justify-between"
              >
                <span>标签过滤</span>
                <span>{{ tagsCollapsed ? '▼' : '▲' }}</span>
              </button>
              <div v-if="!tagsCollapsed" class="max-h-[130px] overflow-y-auto overscroll-contain p-3 flex flex-wrap gap-2">
                <button
                  v-for="tag in availableTags"
                  :key="tag.value"
                  type="button"
                  @click="toggleTag(tag.value)"
                  :class="[
                    'px-3 py-1.5 rounded text-xs border transition-colors',
                    selectedTags.has(tag.value)
                      ? 'bg-primary text-white border-primary'
                      : 'bg-surface text-[color:var(--c-text-muted)] border-[color:var(--c-border)] hover:bg-surface-sunken'
                  ]"
                >{{ tag.name }}</button>
              </div>
            </div>
          </div>
          <div>
            <label class="field-label">自定义任务/文件名</label>
            <div class="flex gap-2">
              <input
                v-model="filename"
                type="text"
                class="input min-w-0 flex-1"
                placeholder="留空则入队时自动命名"
              />
              <button
                type="button"
                @click="addTask()"
                title="添加到任务列表"
                aria-label="添加到任务列表"
                class="btn btn-icon-lg btn-primary"
              >
                <svg aria-hidden="true" viewBox="0 0 24 24" class="h-5 w-5" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round">
                  <path d="M12 5v14"></path><path d="M5 12h14"></path>
                </svg>
              </button>
            </div>
          </div>
        </div>
      </section>
    </div>

    <!-- 右栏：任务监控 -->
    <section class="card flex min-h-0 flex-col overflow-hidden">
      <div class="card-head">
        <h2 class="card-title">任务监控</h2>
        <button @click="tasks.refresh()" class="btn btn-sm btn-soft">刷新</button>
      </div>
      <div class="grid min-h-0 flex-1 content-start grid-cols-1 items-start overflow-hidden lg:grid-cols-[280px_1fr]">
        <!-- 队列面板 -->
        <aside class="min-h-0 self-start overflow-hidden border-r border-[color:var(--c-border-soft)] bg-surface-sunken">
          <div class="toolbar px-4 py-1.5">
            <div class="flex items-center gap-2">
              <button
                @click="startQueue"
                :disabled="!tasks.queueStatus.can_start"
                title="开始任务队列"
                aria-label="开始任务队列"
                class="btn btn-icon-sm bg-emerald-600 text-white hover:bg-emerald-700"
              >
                <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="currentColor">
                  <path d="M8 5.5v13l10-6.5-10-6.5z"></path>
                </svg>
              </button>

              <!-- 当前任务控制按钮 -->
              <template v-if="currentTask">
                <button
                  v-if="isPausable(currentTask.state)"
                  @click="pauseTask(currentTask.task_id)"
                  title="暂停当前任务" aria-label="暂停当前任务"
                  class="btn btn-icon-sm btn-warning"
                >
                  <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="currentColor">
                    <path d="M7 5h4v14H7z"></path><path d="M13 5h4v14h-4z"></path>
                  </svg>
                </button>
                <button
                  v-if="isResumable(currentTask.state)"
                  @click="resumeTask(currentTask.task_id)"
                  title="恢复当前任务" aria-label="恢复当前任务"
                  class="btn btn-icon-sm btn-info"
                >
                  <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M4 7v6h6"></path><path d="M20 17a8 8 0 0 0-13.66-5.66L4 13"></path>
                  </svg>
                </button>
                <button
                  v-if="isCancelable(currentTask.state)"
                  @click="cancelTask(currentTask.task_id)"
                  title="取消当前任务" aria-label="取消当前任务"
                  class="btn btn-icon-sm btn-danger"
                >
                  <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="9"></circle>
                    <path d="M9 9l6 6"></path><path d="M15 9l-6 6"></path>
                  </svg>
                </button>
              </template>

              <button
                @click="tasks.toggleShowFinished()"
                :title="tasks.showFinished ? '隐藏已结束' : '显示已结束'"
                :aria-label="tasks.showFinished ? '隐藏已结束' : '显示已结束'"
                class="btn btn-icon-sm btn-neutral"
              >
                <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M3 6h18"></path><path d="M3 12h18"></path><path d="M3 18h18"></path>
                </svg>
              </button>
              <button
                @click="cleanupFinished"
                :disabled="(tasks.queueStatus.finished_count || 0) <= 0"
                title="清理已结束" aria-label="清理已结束"
                class="btn btn-icon-sm btn-danger"
              >
                <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M3 6h18"></path><path d="M8 6V4h8v2"></path>
                  <path d="M6 6l1 15h10l1-15"></path><path d="M10 11v6"></path><path d="M14 11v6"></path>
                </svg>
              </button>
              <div class="flex min-w-0 items-center gap-3 text-xs text-[color:var(--c-text-muted)]">
                <span>待处理 {{ tasks.queueStatus.active_count || 0 }} 个</span>
                <span>已结束 {{ tasks.queueStatus.finished_count || 0 }} 个</span>
              </div>
            </div>
          </div>

          <!-- 任务列表 -->
          <div class="divide-y divide-[color:var(--c-border)] overflow-y-auto log-box">
            <div v-if="!tasks.visibleTasks.length" class="empty-state p-4">暂无任务</div>
            <template v-else>
              <div
                v-for="task in tasks.visibleTasks"
                :key="task.task_id"
                :class="[
                  'relative grid grid-cols-[minmax(0,1fr)_42px_72px_52px] items-center gap-1 overflow-hidden px-3 py-1 text-xs',
                  task.task_id === tasks.queueStatus.current_task_id ? 'bg-info-soft' : 'bg-surface'
                ]"
              >
                <div class="min-w-0">
                  <div class="truncate font-bold text-xs leading-tight" :title="displayName(task.final_filename || task.filename || '自动命名')">
                    {{ displayName(task.final_filename || task.filename || '自动命名') }}
                  </div>
                  <div class="truncate font-mono text-[10px] leading-tight text-[color:var(--c-text-subtle)]">
                    {{ (task.task_id || '').slice(0, 8) }}
                  </div>
                </div>
                <div class="font-mono text-[color:var(--c-text-muted)] text-right">{{ task.progress || '0/0' }}</div>
                <div class="text-right">
                  <span :class="['badge w-[72px]', stateMeta(task.state).badge]">{{ stateMeta(task.state).label }}</span>
                </div>
                <div class="flex justify-end gap-1">
                  <button
                    v-if="task.can_copy_incremental_magnets"
                    @click="copyIncrementalMagnets(task.task_id)"
                    title="复制新增影片磁力" aria-label="复制新增影片磁力"
                    class="btn btn-info h-6 w-6 p-0 text-sm"
                  >⧉</button>
                  <button
                    @click="deleteTask(task.task_id)"
                    title="删除任务" aria-label="删除任务"
                    class="btn btn-danger h-6 w-6 p-0 text-sm"
                  >×</button>
                </div>
                <div class="absolute inset-x-0 bottom-0 h-[2px] bg-[color:var(--c-bg)]">
                  <div :class="['h-full transition-all duration-300', stateMeta(task.state).bar]" :style="{ width: progressPercent(task.progress) + '%' }"></div>
                </div>
              </div>
            </template>
          </div>
        </aside>

        <!-- 日志面板 -->
        <section class="flex min-h-0 self-start flex-col gap-3 overflow-hidden p-5">
          <!-- 当前任务操作按钮 -->
          <div v-if="currentTask" class="flex flex-wrap gap-2">
            <button v-if="currentTask.state === 'waiting_cookie'" @click="refreshCookie(currentTask.task_id)" class="btn btn-sm btn-warning">读安卓 Cookie</button>
            <button v-if="currentTask.state === 'waiting_cookie'" @click="submitManualCookie(currentTask.task_id)" class="btn btn-sm btn-info">粘贴 Cookie</button>
            <button v-if="currentTask.state === 'waiting_choice'" @click="setTaskMode(currentTask.task_id, 'incremental')" class="btn btn-sm btn-info">增量</button>
            <button v-if="currentTask.state === 'waiting_choice'" @click="setTaskMode(currentTask.task_id, 'overwrite')" class="btn btn-sm btn-danger">覆盖</button>
          </div>

          <div class="min-h-0 overflow-hidden rounded-lg border border-[color:var(--c-border)]">
            <button
              type="button"
              @click="toggleLogCollapsed"
              class="w-full px-3 py-2 bg-surface-sunken border-b border-[color:var(--c-border)] text-sm font-semibold flex items-center justify-between text-left"
            >
              <span>运行日志</span>
              <div class="flex items-center gap-2">
                <button type="button" @click.stop="clearLogs" class="text-xs text-[color:var(--c-text-muted)] hover:text-danger px-1">清除</button>
                <span class="text-xs text-[color:var(--c-text-muted)]">{{ logCollapsed ? '▼' : '▲' }}</span>
              </div>
            </button>
            <div
              ref="logBodyEl"
              v-show="!logCollapsed"
              class="overflow-y-auto log-box bg-surface p-3 font-mono text-xs text-[color:var(--c-text-muted)]"
              :style="{ height: logBodyHeight, minHeight: `${LOG_MIN_HEIGHT}px` }"
            >
              <div v-if="!tasks.logs.length">等待任务启动...</div>
              <div v-for="(log, i) in tasks.logs" :key="i" class="mb-1 border-b border-[color:var(--c-border-soft)] pb-1">{{ log }}</div>
            </div>
          </div>
        </section>
      </div>
    </section>
  </section>
</template>
