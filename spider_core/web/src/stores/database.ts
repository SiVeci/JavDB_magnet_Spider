import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { apiFetch, apiFetchJson, apiPost } from '@/api'
import type { Collection, Movie, Magnet, ApiResponse } from '@/types'

export const RANKING_CATEGORIES = [
  { key: 'censored', label: '有码' },
  { key: 'uncensored', label: '无码' },
  { key: 'western', label: '欧美' },
  { key: 'fc2', label: 'FC2' },
  { key: 'playback', label: '热播' },
  { key: 'top250', label: 'TOP250', dynamicOptions: true, subLabel: '动态分类' },
]

export const RANKING_PERIODS = [
  { key: 'daily', label: '日榜' },
  { key: 'weekly', label: '周榜' },
  { key: 'monthly', label: '月榜' },
]

export const useDatabaseStore = defineStore('database', () => {
  const collections = ref<Collection[]>([])
  const collectionMovies = ref<Record<string, Movie[]>>({})
  const searchQuery = ref('')
  const selectedCollections = ref<Set<string>>(new Set())

  // Magnet check job state
  const activeMagnetCheckJob = ref<{
    running: boolean; cancelled?: boolean; scope?: string; target?: string | number
  } | null>(null)

  // Top250 options
  const top250Options = ref<{ key: string; label: string }[] | null>(null)

  // Tag filters per collection/ranking key
  const tagFilters = ref<Record<string, string[]>>({})
  const excludeFilters = ref<Record<string, string[]>>({})

  const totalMovies = computed(() =>
    collections.value.reduce((s: number, c: Record<string, unknown>) => s + Number((c as { count?: number }).count || 0), 0)
  )

  const filteredCollections = computed(() => {
    const q = searchQuery.value.trim().toLowerCase()
    if (!q) return collections.value
    return collections.value.filter((c: Collection) => c.name.toLowerCase().includes(q) || c.filename.toLowerCase().includes(q))
  })

  async function loadCollections() {
    const res = await apiFetch('/api/history').then((r: Response) => r.json())
    collections.value = res.data || []
  }

  function getCollection(name: string): Collection | undefined {
    return collections.value.find((c: Collection) => c.name === name || c.filename === name)
  }

  async function ensureMovies(collectionName: string): Promise<boolean> {
    if (collectionMovies.value[collectionName]) return true
    try {
      const res = await apiFetchJson<ApiResponse<Movie[]>>(`/api/collections/${encodeURIComponent(collectionName)}/movies`)
      collectionMovies.value[collectionName] = (res.data || []) as Movie[]
      return true
    } catch {
      return false
    }
  }

  async function deleteCollection(name: string): Promise<void> {
    await apiPost('/api/delete', { filenames: [name] })
    await loadCollections()
    delete collectionMovies.value[name]
  }

  async function batchDeleteCollections(names: string[]): Promise<void> {
    await apiPost('/api/delete', { filenames: names })
    for (const name of names) delete collectionMovies.value[name]
    await loadCollections()
    selectedCollections.value.clear()
  }

  function toggleSelectCollection(name: string) {
    if (selectedCollections.value.has(name)) selectedCollections.value.delete(name)
    else selectedCollections.value.add(name)
  }

  function clearSelection() { selectedCollections.value.clear() }

  async function loadRankingMovies(categoryKey: string, periodKey: string): Promise<Movie[]> {
    const tagsQ = buildTagsQuery(tagFilters.value[`ranking:${categoryKey}:${periodKey}`] || [], excludeFilters.value[`ranking:${categoryKey}:${periodKey}`] || [])
    const url = `/api/rankings/${encodeURIComponent(categoryKey)}/${encodeURIComponent(periodKey)}/movies${tagsQ ? `?${tagsQ}` : ''}`
    const res = await apiFetchJson<ApiResponse<Movie[]>>(url)
    return (res.data || []) as Movie[]
  }

  async function getRankingMagnets(categoryKey: string, periodKey: string): Promise<string[]> {
    const res = await apiFetchJson<ApiResponse<string[]>>(
      `/api/rankings/${encodeURIComponent(categoryKey)}/${encodeURIComponent(periodKey)}/magnets`
    )
    return (res.data || []) as string[]
  }

  function getRankingDownloadUrl(categoryKey: string, periodKey: string): string {
    return `/api/rankings/${encodeURIComponent(categoryKey)}/${encodeURIComponent(periodKey)}/download`
  }

  async function createRankingUpdateTask(categoryKey: string, periodKey: string): Promise<{ code: number; msg?: string; needs_mode?: boolean; filename?: string }> {
    const res = await apiFetch(
      `/api/rankings/${encodeURIComponent(categoryKey)}/${encodeURIComponent(periodKey)}/update`,
      { method: 'POST' }
    ).then((r: Response) => r.json())
    return res
  }

  async function loadTop250Options(refresh = false): Promise<{ key: string; label: string }[]> {
    const suffix = refresh ? '?refresh=1' : ''
    const res = await apiFetchJson<ApiResponse>(`/api/rankings/top250/options${suffix}`)
    if ((res as { code: number }).code !== 200) throw new Error((res as { msg?: string }).msg || 'TOP250 分类加载失败')
    const data = (res as { data?: { options?: { key: string; label: string }[] } }).data || {}
    top250Options.value = data.options || []
    return top250Options.value
  }

  async function loadMovieMagnets(collectionName: string, movieId: string | number): Promise<Magnet[]> {
    // collectionName 仅用于排行榜缓存键区分，实际 API 只需 movieId
    const res = await apiFetchJson<ApiResponse<Magnet[]>>(
      `/api/movies/${encodeURIComponent(String(movieId))}/magnets`
    )
    return (res.data || []) as Magnet[]
  }

  async function autoSelectMagnets(collectionName?: string): Promise<string> {
    const filenames = collectionName ? [collectionName] : []
    const res = await apiPost<{ code: number; msg?: string }>('/api/magnets/auto_select', { filenames })
    if (res.code !== 200) throw new Error(res.msg || '自动选择失败')
    return res.msg || '已完成自动选择'
  }

  async function selectMagnet(collectionName: string, movieId: string | number, magnetId: number): Promise<void> {
    await apiPost(`/api/movies/${encodeURIComponent(String(movieId))}/select_magnet`, { magnet_id: magnetId })
  }

  function setTagFilter(key: string, tags: string[]) { tagFilters.value[key] = tags }
  function setExcludeFilter(key: string, tags: string[]) { excludeFilters.value[key] = tags }

  function buildTagsQuery(tags: string[], excludes: string[]): string {
    const parts: string[] = []
    if (tags.length) parts.push(`tags=${encodeURIComponent(tags.join(','))}`)
    if (excludes.length) parts.push(`exclude_tags=${encodeURIComponent(excludes.join(','))}`)
    return parts.join('&')
  }

  async function getCollectionDownloadUrl(name: string): Promise<string> {
    return `/api/download?name=${encodeURIComponent(name)}`
  }

  return {
    collections, collectionMovies, searchQuery, selectedCollections,
    activeMagnetCheckJob, top250Options, tagFilters, excludeFilters,
    totalMovies, filteredCollections,
    loadCollections, getCollection, ensureMovies,
    deleteCollection, batchDeleteCollections,
    toggleSelectCollection, clearSelection,
    loadRankingMovies, getRankingMagnets, getRankingDownloadUrl, createRankingUpdateTask,
    loadTop250Options, loadMovieMagnets,
    autoSelectMagnets, selectMagnet,
    setTagFilter, setExcludeFilter, buildTagsQuery,
    getCollectionDownloadUrl,
  }
})
