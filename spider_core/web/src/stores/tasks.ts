import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { apiFetch, apiFetchJson, apiPost } from '@/api'
import { useEventStream } from '@/composables/useEventStream'
import type { Task, QueueStatus, ApiResponse } from '@/types'

const TERMINAL_STATES = ['finished', 'canceled', 'failed']
const PAUSABLE_STATES = ['running', 'pending', 'pause_requested']
const RESUMABLE_STATES = ['paused', 'waiting_cookie', 'waiting_choice']
const CANCELABLE_STATES = [...PAUSABLE_STATES, ...RESUMABLE_STATES]

export const isTerminal = (s: string) => TERMINAL_STATES.includes(s)
export const isPausable = (s: string) => PAUSABLE_STATES.includes(s)
export const isResumable = (s: string) => RESUMABLE_STATES.includes(s)
export const isCancelable = (s: string) => CANCELABLE_STATES.includes(s)

export const useTasksStore = defineStore('tasks', () => {
  const tasks = ref<Task[]>([])
  const queueStatus = ref<QueueStatus>({
    queue_state: 'idle',
    can_start: false,
    active_count: 0,
    finished_count: 0,
  })
  const logs = ref<string[]>([])
  const showFinished = ref(false)
  const sseConnected = ref(false)
  const collectionsChanged = ref(false)

  let pollHandle: ReturnType<typeof setTimeout> | null = null
  let sseInstance: ReturnType<typeof useEventStream> | null = null

  const visibleTasks = computed(() =>
    showFinished.value ? tasks.value : tasks.value.filter((t: Task) => !isTerminal(t.state))
  )

  const currentTask = computed(() =>
    tasks.value.find((t: Task) => t.task_id === queueStatus.value.current_task_id) ||
    tasks.value.find((t: Task) => ['running', 'pause_requested', 'cancel_requested', 'waiting_cookie', 'waiting_choice', 'paused'].includes(t.state)) ||
    null
  )

  function applySSEEvent(event: { tasks: Task[]; queue: QueueStatus; logs: string[]; collectionsChanged: boolean }) {
    tasks.value = event.tasks || []
    if (event.queue) queueStatus.value = event.queue as QueueStatus
    logs.value = event.logs || []
    if (event.collectionsChanged) collectionsChanged.value = true
  }

  async function refresh() {
    const [tasksRes, queueRes, statusRes] = await Promise.all([
      apiFetch('/api/tasks').then((r: Response) => r.json()) as Promise<ApiResponse<Task[]>>,
      apiFetch('/api/tasks/queue_status').then((r: Response) => r.json()) as Promise<ApiResponse<QueueStatus>>,
      apiFetch('/api/status').then((r: Response) => r.json()) as Promise<{ logs?: string[] }>,
    ])
    tasks.value = (tasksRes as ApiResponse<Task[]>).data || []
    queueStatus.value = (queueRes as ApiResponse<QueueStatus>).data || queueStatus.value
    logs.value = (statusRes as { logs?: string[] }).logs || []
  }

  function startPolling(intervalMs = 2500) {
    if (pollHandle) return
    const tick = async () => {
      try { await refresh() } catch (err) { console.error('轮询出错:', err) }
      finally { if (pollHandle) pollHandle = setTimeout(tick, intervalMs) }
    }
    pollHandle = setTimeout(tick, intervalMs)
  }

  function stopPolling() {
    if (pollHandle) { clearTimeout(pollHandle); pollHandle = null }
  }

  function startSSE(getToken?: () => string) {
    if (sseInstance) return
    sseInstance = useEventStream(
      (event) => {
        applySSEEvent(event as { tasks: Task[]; queue: QueueStatus; logs: string[]; collectionsChanged: boolean })
        if (!sseConnected.value) {
          sseConnected.value = true
          stopPolling()
        }
      },
      getToken,
    )
    sseInstance.status.value  // access to init watcher
    // 监听连接状态：断开时回退轮询
    const unwatchStatus = (sseInstance as { status: { value: string } }).status
    const checkDisconnect = () => {
      if (['disconnected', 'error'].includes(String(unwatchStatus.value)) && sseConnected.value) {
        sseConnected.value = false
        if (!pollHandle) startPolling()
      }
    }
    // 每 5s 检查 SSE 状态，确保断线时能回退
    const monitor = setInterval(checkDisconnect, 5000)
    sseInstance.connect()
    // 清理函数挂到 store（在 app unmount 时调用）
    ;(sseInstance as unknown as { _monitor: ReturnType<typeof setInterval> })._monitor = monitor
  }

  function stopSSE() {
    if (sseInstance) {
      const m = (sseInstance as unknown as { _monitor?: ReturnType<typeof setInterval> })._monitor
      if (m) clearInterval(m)
      sseInstance.disconnect()
      sseInstance = null
      sseConnected.value = false
    }
  }

  function toggleShowFinished() { showFinished.value = !showFinished.value }

  function clearCollectionsChanged() { collectionsChanged.value = false }

  async function startQueue() {
    const res = await apiFetch('/api/tasks/start_queue', { method: 'POST' }).then((r: Response) => r.json())
    if (res.code !== 200) throw new Error(res.msg || '无法启动队列')
    if (!sseConnected.value) startPolling()
    await refresh()
  }

  async function taskAction(taskId: string, action: string, options: { method?: string; body?: unknown; path?: string } = {}) {
    const method = options.method || 'POST'
    const path = options.path || `/api/tasks/${encodeURIComponent(taskId)}/${action}`
    const reqOptions: RequestInit = { method }
    if (options.body !== undefined) {
      reqOptions.headers = { 'Content-Type': 'application/json' }
      reqOptions.body = JSON.stringify(options.body)
    }
    const res = await apiFetchJson<ApiResponse>(path, reqOptions)
    await refresh()
    return res
  }

  async function addTask(payload: Record<string, unknown>): Promise<{ code: number; msg?: string; needs_mode?: boolean; filename?: string }> {
    const response = await apiFetch('/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    })
    const res = await response.json()
    if (res.code === 200) await refresh()
    return res
  }

  async function cleanupFinished() {
    const res = await apiFetch('/api/tasks/cleanup', { method: 'POST' }).then((r: Response) => r.json())
    if (res.code !== 200) throw new Error(res.msg || '清理失败')
    await refresh()
    return res.msg || '已清理已结束任务'
  }

  async function fetchTags(url: string): Promise<{ name: string; value: string }[]> {
    const res = await apiPost<ApiResponse<{ name: string; value: string }[]>>('/api/get_tags', { url })
    if (res.code !== 200) throw new Error(res.msg || '获取标签失败')
    return res.data || []
  }

  async function getIncrementalMagnets(taskId: string): Promise<string[]> {
    const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}/incremental_magnets`).then((r: Response) => r.json())
    if (res.code !== 200) throw new Error(res.msg || '读取失败')
    return res.data || []
  }

  return {
    tasks, queueStatus, logs, showFinished, sseConnected, collectionsChanged,
    visibleTasks, currentTask,
    refresh, startPolling, stopPolling, startSSE, stopSSE, toggleShowFinished, clearCollectionsChanged,
    startQueue, taskAction, addTask, cleanupFinished, fetchTags, getIncrementalMagnets,
  }
})
