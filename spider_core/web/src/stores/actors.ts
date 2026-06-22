import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { apiFetch } from '@/api'
import type { Actor, ActorCategory, ActorsData, Tag } from '@/types'

const DEFAULT_CATEGORIES: ActorCategory[] = [
  { key: 'all', label: '全部' },
  { key: 'g0t0', label: '有码女优' },
  { key: 'g1t0', label: '有码男优' },
  { key: 'g0t1', label: '无码演员' },
  { key: 'g0t2', label: '欧美女优' },
  { key: 'g1t2', label: '欧美男优' },
]

interface TagState {
  loading?: boolean
  tags?: Tag[]
  selected?: Set<string>
  error?: string
}

export const useActorsStore = defineStore('actors', () => {
  const data = ref<ActorsData>({ categories: DEFAULT_CATEGORIES, actors: [] })
  const loaded = ref(false)
  const category = ref('all')
  const expandedActorId = ref<string | null>(null)
  const tagStates = ref<Record<string, TagState>>({})

  const filtered = computed(() => {
    if (category.value === 'all') return data.value.actors
    return data.value.actors.filter((a: Actor) => a.category === category.value)
  })

  const categories = computed(() =>
    data.value.categories?.length ? data.value.categories : DEFAULT_CATEGORIES
  )

  function categoryLabel(key: string): string {
    return categories.value.find((c: ActorCategory) => c.key === key)?.label ?? key
  }

  function normalize(raw: Partial<ActorsData>): ActorsData {
    return {
      categories: raw.categories?.length ? raw.categories : DEFAULT_CATEGORIES,
      actors: raw.actors || [],
      failed: raw.failed,
    }
  }

  async function load() {
    try {
      const res = await apiFetch('/api/actors').then((r: Response) => r.json())
      if (res.code === 200 && res.data) {
        data.value = normalize(res.data)
        loaded.value = true
      }
    } catch (err) {
      console.error('加载收藏演员失败:', err)
    }
  }

  async function refresh(cat: string): Promise<{ failed?: ActorsData['failed'] }> {
    const resp = await apiFetch('/api/actors/refresh', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ category: cat }),
    })
    const res = await resp.json().catch(() => ({}))
    if (!resp.ok || res.code !== 200) throw new Error(res.msg || `刷新失败 (${resp.status})`)
    data.value = normalize(res.data || {})
    loaded.value = true
    return { failed: res.data?.failed }
  }

  function selectCategory(key: string) {
    category.value = key
    expandedActorId.value = null
  }

  async function toggleActorTags(actorId: string) {
    if (expandedActorId.value === actorId) {
      expandedActorId.value = null
      return
    }
    expandedActorId.value = actorId
    const st = tagStates.value[actorId]
    if (!st || (!st.tags && !st.loading && !st.error)) {
      await loadActorTags(actorId)
    }
  }

  async function loadActorTags(actorId: string) {
    const actor = data.value.actors.find((a: Actor) => a.actor_id === actorId)
    if (!actor) return
    const prevSelected = tagStates.value[actorId]?.selected
    tagStates.value[actorId] = { loading: true, selected: prevSelected || new Set() }
    try {
      const resp = await apiFetch('/api/get_tags', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: actor.actor_url }),
      })
      if (resp.status === 404) {
        tagStates.value[actorId] = { tags: [], selected: new Set() }
      } else {
        const res = await resp.json().catch(() => ({}))
        if (!resp.ok || res.code !== 200) {
          tagStates.value[actorId] = { tags: [], selected: new Set(), error: res.msg || '获取标签失败' }
        } else {
          const lastValues = new Set<string>((actor.last_task_tags || []).map((t: { name: string; value: string }) => t.value))
          tagStates.value[actorId] = { tags: res.data || [], selected: lastValues }
        }
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : '获取标签失败'
      tagStates.value[actorId] = { tags: [], selected: new Set(), error: msg }
    }
  }

  function toggleTag(actorId: string, value: string) {
    const st = tagStates.value[actorId]
    if (!st) return
    if (!st.selected) st.selected = new Set()
    if (st.selected.has(value)) st.selected.delete(value)
    else st.selected.add(value)
  }

  async function addTask(actorId: string, crawlMode = ''): Promise<{ code: number; msg?: string; needs_mode?: boolean; filename?: string }> {
    const actor = data.value.actors.find((a: Actor) => a.actor_id === actorId)
    if (!actor) throw new Error('演员不存在')
    const st = tagStates.value[actorId] || {}
    const selected = st.selected || new Set()
    const tags = Array.isArray(st.tags) && !st.error
      ? st.tags.filter(t => selected.has(t.value))
      : (actor.last_task_tags || [])
    const resp = await apiFetch('/api/actors/add_task', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ actor_id: actorId, tags, crawl_mode: crawlMode }),
    })
    const res = await resp.json().catch(() => ({}))
    if (res.code === 200) {
      actor.last_task_tags = res.data?.tags || tags
    }
    return res
  }

  function getActorById(id: string): Actor | undefined {
    return data.value.actors.find((a: Actor) => a.actor_id === id)
  }

  return {
    data, loaded, category, expandedActorId, tagStates,
    filtered, categories, categoryLabel,
    load, refresh, selectCategory, toggleActorTags, loadActorTags, toggleTag, addTask, getActorById,
  }
})
