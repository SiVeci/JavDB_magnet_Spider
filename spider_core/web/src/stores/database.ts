import { defineStore } from 'pinia'
import { ref } from 'vue'
import { apiFetch, apiFetchJson, apiPost } from '@/api'
import type { Collection, Movie, Magnet, MagnetCheckJob, ApiResponse } from '@/types'
import { displayName } from '@/utils/format'

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

// 集合健康度四宫格（顺序即展示顺序）
export const HEALTH_ITEMS = [
  { key: 'active_count', title: '有效影片', badge: 'badge-success' },
  { key: 'weak_count', title: '弱影片', badge: 'badge-warning' },
  { key: 'dead_count', title: '无效影片', badge: 'badge-danger' },
  { key: 'failed_count', title: '检测失败影片', badge: 'badge-neutral' },
]

export interface CollectionMovieData {
  movies: Movie[]
  available_tags: string[]
  total_count: number
  collection_filename?: string
}

function emptyMovieData(): CollectionMovieData {
  return { movies: [], available_tags: [], total_count: 0 }
}

export const useDatabaseStore = defineStore('database', () => {
  const collections = ref<Collection[]>([])
  // 集合影片缓存（含 available_tags/total_count），key = 集合名
  const collectionMovieCache = ref<Record<string, CollectionMovieData>>({})
  // 排行榜影片缓存，key = `category:period`
  const rankingMovieCache = ref<Record<string, CollectionMovieData>>({})
  // TOP250 动态分类选项
  const top250Options = ref<{ key: string; label: string }[] | null>(null)
  const top250Stale = ref(false)

  // 标签过滤 / 排除（集合 key = 集合名；排行 key = `ranking:cat:period`）
  const tagFilters = ref<Record<string, string[]>>({})
  const excludeFilters = ref<Record<string, string[]>>({})

  // 磁力检测任务（全局至多一个）
  const activeMagnetCheckJob = ref<MagnetCheckJob | null>(null)
  let magnetCheckTimer: ReturnType<typeof setTimeout> | null = null
  // 检测轮询期间触发的刷新回调（由 DatabaseView 注册，重渲染当前页）
  let onCheckTick: (() => void) | null = null
  let onCheckDone: ((job: MagnetCheckJob) => void) | null = null

  /* ===== 集合 ===== */

  async function loadCollections() {
    const res = await apiFetchJson<ApiResponse<Collection[]>>('/api/history')
    collections.value = res.data || []
  }

  function getCollection(name: string): Collection | undefined {
    return collections.value.find((c: Collection) => c.name === name)
  }

  function totalMovies(): number {
    return collections.value.reduce((s: number, c: Collection) => s + Number(c.count || 0), 0)
  }

  async function ensureMovies(name: string, forceReload = false): Promise<boolean> {
    if (collectionMovieCache.value[name] && !forceReload) return true
    try {
      const res = await apiFetchJson<ApiResponse<CollectionMovieData>>(`/api/collections/${encodeURIComponent(name)}/movies`)
      if (res.code !== 200) return false
      collectionMovieCache.value[name] = res.data || emptyMovieData()
      return true
    } catch {
      return false
    }
  }

  function collectionData(name: string): CollectionMovieData {
    return collectionMovieCache.value[name] || emptyMovieData()
  }

  function invalidateMovieCaches() {
    collectionMovieCache.value = {}
    rankingMovieCache.value = {}
  }

  function movieById(name: string, movieId: string | number): Movie | undefined {
    return collectionData(name).movies.find((m: Movie) => String(m.id) === String(movieId))
  }

  async function deleteCollection(name: string): Promise<string> {
    const res = await apiPost<ApiResponse>('/api/delete', { filenames: [name] })
    delete collectionMovieCache.value[name]
    await loadCollections()
    return res.msg || '已删除'
  }

  async function batchDeleteCollections(names: string[]): Promise<string> {
    const res = await apiPost<ApiResponse>('/api/delete', { filenames: names })
    for (const name of names) delete collectionMovieCache.value[name]
    await loadCollections()
    return res.msg || '已批量删除'
  }

  async function autoSelectMagnets(filenames: string[]): Promise<string> {
    const res = await apiPost<ApiResponse>('/api/magnets/auto_select', { filenames })
    if (res.code !== 200) throw new Error(res.msg || '自动选择失败')
    return res.msg || '自动选择完成'
  }

  async function enqueueCollectionIncremental(name: string): Promise<string> {
    const res = await apiFetchJson<ApiResponse>(`/api/collections/${encodeURIComponent(name)}/incremental_task`, { method: 'POST' })
    if (res.code !== 200) throw new Error(res.msg || '添加增量任务失败')
    return res.msg || '任务已加入队列'
  }

  /* ===== 候选磁力 ===== */

  async function loadMovieMagnets(movieId: string | number): Promise<Magnet[]> {
    const res = await apiFetchJson<ApiResponse<Magnet[]>>(`/api/movies/${encodeURIComponent(String(movieId))}/magnets`)
    return (res.data || []) as Magnet[]
  }

  async function selectMagnet(movieId: string | number, magnetId: number): Promise<void> {
    const res = await apiPost<ApiResponse>(`/api/movies/${encodeURIComponent(String(movieId))}/select_magnet`, { magnet_id: magnetId })
    if (res.code !== 200) throw new Error(res.msg || '更新失败')
  }

  // 把选中磁力字段回写到缓存里的影片记录（等价旧版 applySelectedMagnetToMovie）
  function syncSelectedMagnetToCache(movieId: string | number, magnets: Magnet[]) {
    const selected = magnets.find((m: Magnet) => m.is_selected)
    if (!selected) return
    const apply = (data?: CollectionMovieData) => {
      if (!data) return
      const movie = data.movies.find((mv: Movie) => Number(mv.id) === Number(movieId))
      if (!movie) return
      movie.best_magnet_name = selected.name || ''
      movie.best_magnet_link = selected.link || ''
      movie.priority_score = selected.priority_score || 0
      movie.magnet_date = selected.magnet_date || ''
      movie.size_mb = selected.size_mb || 0
    }
    for (const key of Object.keys(collectionMovieCache.value)) apply(collectionMovieCache.value[key])
    for (const key of Object.keys(rankingMovieCache.value)) apply(rankingMovieCache.value[key])
  }

  /* ===== 排行榜 ===== */

  function rankingCacheKey(categoryKey: string, periodKey: string) { return `${categoryKey}:${periodKey}` }
  function rankingFilterKey(categoryKey: string, periodKey: string) { return `ranking:${categoryKey}:${periodKey}` }

  function rankingApiPath(categoryKey: string, periodKey: string, tail = '') {
    return `/api/rankings/${encodeURIComponent(categoryKey)}/${encodeURIComponent(periodKey)}${tail}`
  }

  function rankingData(categoryKey: string, periodKey: string): CollectionMovieData {
    return rankingMovieCache.value[rankingCacheKey(categoryKey, periodKey)] || emptyMovieData()
  }

  async function ensureRankingMovies(categoryKey: string, periodKey: string, forceReload = false): Promise<boolean> {
    const key = rankingCacheKey(categoryKey, periodKey)
    if (rankingMovieCache.value[key] && !forceReload) return true
    const res = await apiFetchJson<ApiResponse<CollectionMovieData>>(rankingApiPath(categoryKey, periodKey, '/movies'))
    if (res.code !== 200) return false
    rankingMovieCache.value[key] = res.data || emptyMovieData()
    return true
  }

  function rankingMovieById(categoryKey: string, periodKey: string, movieId: string | number): Movie | undefined {
    return rankingData(categoryKey, periodKey).movies.find((m: Movie) => String(m.id) === String(movieId))
  }

  async function getRankingMagnetLinks(categoryKey: string, periodKey: string): Promise<string[]> {
    const res = await apiFetchJson<ApiResponse<string[]>>(rankingApiPath(categoryKey, periodKey, `/magnets${tagsQuerySuffix(rankingFilterKey(categoryKey, periodKey), '?')}`))
    return (res.data || []) as string[]
  }

  function getRankingDownloadUrl(categoryKey: string, periodKey: string): string {
    return rankingApiPath(categoryKey, periodKey, `/download${tagsQuerySuffix(rankingFilterKey(categoryKey, periodKey), '?')}`)
  }

  async function createRankingUpdateTask(categoryKey: string, periodKey: string): Promise<{ code: number; msg?: string; needs_mode?: boolean; filename?: string }> {
    return apiFetch(rankingApiPath(categoryKey, periodKey, '/update'), { method: 'POST' }).then((r: Response) => r.json())
  }

  async function clearRankingList(categoryKey: string, periodKey: string): Promise<string> {
    const res = await apiFetchJson<ApiResponse>(rankingApiPath(categoryKey, periodKey, '/clear'), { method: 'POST' })
    if (res.code !== 200) throw new Error(res.msg || '清空失败')
    delete rankingMovieCache.value[rankingCacheKey(categoryKey, periodKey)]
    return res.msg || '清空成功'
  }

  async function loadTop250Options(refresh = false): Promise<{ key: string; label: string }[]> {
    const suffix = refresh ? '?refresh=1' : ''
    const res = await apiFetchJson<ApiResponse<{ options: { key: string; label: string }[]; stale: boolean }> & { error_type?: string }>(`/api/rankings/top250/options${suffix}`)
    if (res.code !== 200) {
      throw new Error(top250OptionErrorMessage(res.error_type || '', res.msg || 'TOP250 分类加载失败'))
    }
    top250Options.value = res.data?.options || []
    top250Stale.value = !!res.data?.stale
    return top250Options.value
  }

  function top250OptionErrorMessage(errorType: string, fallback = ''): string {
    if (errorType === 'auth') return '刷新失败：Cookie 可能失效，请更新 Cookie 或重新登录 JavDB'
    if (errorType === 'network') return '刷新失败：网络或代理异常，请检查网络和代理设置'
    if (errorType === 'parse') return '刷新失败：TOP250 页面结构可能已变化，未找到分类选项'
    return fallback || '刷新分类失败'
  }

  /* ===== 标签过滤 ===== */

  function getTagFilter(key: string): string[] { return tagFilters.value[key] || [] }
  function getExcludeFilter(key: string): string[] { return excludeFilters.value[key] || [] }

  function toggleTag(key: string, value: string) {
    if (value === 'all') { tagFilters.value[key] = []; return }
    const cur = new Set(getTagFilter(key))
    if (cur.has(value)) cur.delete(value); else cur.add(value)
    tagFilters.value[key] = Array.from(cur)
  }

  function toggleExclude(key: string, value: string) {
    const cur = new Set(getExcludeFilter(key))
    if (cur.has(value)) cur.delete(value); else cur.add(value)
    excludeFilters.value[key] = Array.from(cur)
  }

  function clearExclude(key: string) { excludeFilters.value[key] = [] }

  function tagsQuerySuffix(key: string, prefix = '&'): string {
    const tags = getTagFilter(key)
    const excludes = getExcludeFilter(key)
    const parts: string[] = []
    if (tags.length) parts.push(`tags=${encodeURIComponent(tags.join(','))}`)
    if (excludes.length) parts.push(`exclude_tags=${encodeURIComponent(excludes.join(','))}`)
    return parts.length ? `${prefix}${parts.join('&')}` : ''
  }

  function getCollectionDownloadUrl(name: string): string {
    return `/api/download?name=${encodeURIComponent(name)}${tagsQuerySuffix(name, '&')}`
  }

  async function getCollectionMagnetLinks(name: string): Promise<string[]> {
    const res = await apiFetchJson<ApiResponse<string[]>>(`/api/magnets?name=${encodeURIComponent(name)}${tagsQuerySuffix(name, '&')}`)
    return (res.data || []) as string[]
  }

  /* ===== 磁力检测任务 ===== */

  function registerCheckCallbacks(tick: (() => void) | null, done: ((job: MagnetCheckJob) => void) | null) {
    onCheckTick = tick
    onCheckDone = done
  }

  function stopMagnetCheckPolling() {
    if (magnetCheckTimer) { clearTimeout(magnetCheckTimer); magnetCheckTimer = null }
  }

  function startMagnetCheckPolling() {
    stopMagnetCheckPolling()
    const tick = async () => {
      try { await pollMagnetCheckJob() } catch (e) { console.error('磁力检测轮询出错:', e) }
      finally { if (activeMagnetCheckJob.value) magnetCheckTimer = setTimeout(tick, 1000) }
    }
    magnetCheckTimer = setTimeout(tick, 1000)
  }

  function watchMagnetCheckJob(job: MagnetCheckJob) {
    activeMagnetCheckJob.value = job
    startMagnetCheckPolling()
    pollMagnetCheckJob()
  }

  async function pollMagnetCheckJob() {
    if (!activeMagnetCheckJob.value) return
    const res = await apiFetchJson<ApiResponse<MagnetCheckJob>>(`/api/magnet_check_jobs/${encodeURIComponent(String(activeMagnetCheckJob.value.job_id))}`)
    if (res.code !== 200) return
    activeMagnetCheckJob.value = res.data || null
    if (activeMagnetCheckJob.value?.running) {
      if (onCheckTick) onCheckTick()
    } else {
      stopMagnetCheckPolling()
      const finished = activeMagnetCheckJob.value as MagnetCheckJob
      activeMagnetCheckJob.value = null
      if (onCheckDone) onCheckDone(finished)
    }
  }

  async function restoreMagnetCheckJob() {
    const res = await apiFetchJson<ApiResponse<MagnetCheckJob>>('/api/magnet_check_jobs/current')
    if (res.code !== 200 || !res.data) return
    watchMagnetCheckJob(res.data)
  }

  // 启动检测：scope movie/collection/all/ranking
  async function startMagnetCheck(scope: string, target: string | number, failedOnly = false): Promise<void> {
    const suffix = failedOnly ? '?failed_only=1' : ''
    let path = ''
    if (scope === 'movie') path = `/api/movies/${encodeURIComponent(String(target))}/check_magnets${suffix}`
    else if (scope === 'collection') path = `/api/collections/${encodeURIComponent(String(target))}/check_magnets${suffix}`
    else if (scope === 'all') path = `/api/magnets/check_all${suffix}`
    else if (scope === 'ranking') {
      const [cat, period] = String(target).split(':')
      path = rankingApiPath(cat, period, `/check_magnets${suffix}`)
    }
    const res = await apiFetch(path, { method: 'POST' }).then((r: Response) => r.json())
    if (res.code !== 200 && res.code !== 409) throw new Error(res.msg || '检测启动失败')
    if (res.code === 409) throw Object.assign(new Error(res.msg || '磁力检测任务正在运行'), { running: true, data: res.data })
    watchMagnetCheckJob(res.data)
  }

  async function cancelMagnetCheck(jobId: string): Promise<void> {
    const res = await apiFetchJson<ApiResponse<MagnetCheckJob>>(`/api/magnet_check_jobs/${encodeURIComponent(jobId)}/cancel`, { method: 'POST' })
    if (res.code !== 200) throw new Error(res.msg || '终止检测失败')
    activeMagnetCheckJob.value = res.data || null
  }

  return {
    collections, collectionMovieCache, rankingMovieCache,
    top250Options, top250Stale, tagFilters, excludeFilters,
    activeMagnetCheckJob,
    loadCollections, getCollection, totalMovies,
    ensureMovies, collectionData, invalidateMovieCaches, movieById,
    deleteCollection, batchDeleteCollections, autoSelectMagnets, enqueueCollectionIncremental,
    loadMovieMagnets, selectMagnet, syncSelectedMagnetToCache,
    rankingCacheKey, rankingFilterKey, rankingData,
    ensureRankingMovies, rankingMovieById,
    getRankingMagnetLinks, getRankingDownloadUrl, createRankingUpdateTask, clearRankingList,
    loadTop250Options,
    getTagFilter, getExcludeFilter, toggleTag, toggleExclude, clearExclude, tagsQuerySuffix,
    getCollectionDownloadUrl, getCollectionMagnetLinks,
    registerCheckCallbacks, watchMagnetCheckJob, restoreMagnetCheckJob,
    startMagnetCheck, cancelMagnetCheck,
    displayName,
  }
})
