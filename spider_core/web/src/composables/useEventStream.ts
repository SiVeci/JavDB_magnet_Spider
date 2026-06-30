import { ref } from 'vue'

export type SSEStatus = 'connecting' | 'connected' | 'disconnected' | 'error'

export interface SSEEvent {
  type: 'snapshot' | 'update'
  tasks: unknown[]
  queue: unknown
  logs: string[]
  collectionsChanged: boolean
}

export function useEventStream(
  onEvent: (event: SSEEvent) => void,
  token?: () => string,
) {
  const status = ref<SSEStatus>('disconnected')
  let es: EventSource | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null
  let reconnectDelay = 2000
  let stopped = false

  function buildUrl(): string {
    const t = token?.()
    return t ? `/api/events?token=${encodeURIComponent(t)}` : '/api/events'
  }

  function connect() {
    if (stopped) return
    status.value = 'connecting'
    es = new EventSource(buildUrl())

    es.onopen = () => {
      status.value = 'connected'
      reconnectDelay = 2000
    }

    es.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data) as SSEEvent
        if (data.type === 'snapshot' || data.type === 'update') {
          onEvent(data)
        }
      } catch {
        // ignore malformed events
      }
    }

    es.onerror = () => {
      status.value = 'error'
      es?.close()
      es = null
      if (!stopped) {
        reconnectTimer = setTimeout(() => {
          reconnectDelay = Math.min(reconnectDelay * 1.5, 30000)
          connect()
        }, reconnectDelay)
      }
    }
  }

  function disconnect() {
    stopped = true
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null }
    es?.close()
    es = null
    status.value = 'disconnected'
  }

  return { status, connect, disconnect }
}
