<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { apiDownloadBlob } from '@/api'
import { useDatabaseStore, RANKING_CATEGORIES, RANKING_PERIODS, HEALTH_ITEMS } from '@/stores/database'
import { useTasksStore } from '@/stores/tasks'
import { useToast } from '@/composables/useToast'
import { useClipboard } from '@/composables/useClipboard'
import { fitMovieTags } from '@/composables/useMovieTags'
import MagnetCheckButton from '@/components/MagnetCheckButton.vue'
import MagnetTable from '@/components/MagnetTable.vue'
import MovieListItem from '@/components/MovieListItem.vue'
import MovieMagnetHeader from '@/components/MovieMagnetHeader.vue'
import TagFilterDropdown from '@/components/TagFilterDropdown.vue'
import { displayName } from '@/utils/format'
import { toErrMsg } from '@/utils/error'
import type { Collection, Movie, Magnet, MagnetCheckJob } from '@/types'

const route = useRoute()
const router = useRouter()
const db = useDatabaseStore()
const tasks = useTasksStore()
const { showToast } = useToast()
const { copyText } = useClipboard()

// ===== 路由解析（演员 3 级 / 排行 4 级）=====
const segments = computed(() => {
  // route.path 形如 /database 或 /database/actor/xxx
  const parts = route.path.replace(/^\/+|\/+$/g, '').split('/')
  return parts.slice(1).map((p) => decodeURIComponent(p)) // 去掉 'database'
})
const routeType = computed(() => segments.value[0] || null)
const routeCategory = computed(() => (routeType.value === 'actor' || routeType.value === 'ranking' ? segments.value[1] || null : null))
// 演员：seg[2]=movieId；排行：seg[2]=period, seg[3]=movieId
const routePeriod = computed(() => (routeType.value === 'ranking' ? segments.value[2] || null : null))
const routeMovieId = computed(() => {
  if (routeType.value === 'actor') return segments.value[2] || null
  if (routeType.value === 'ranking') return segments.value[3] || null
  return null
})

type PageMode = 'type-select' | 'collection-list' | 'movie-list' | 'magnet-list' |
  'ranking-category' | 'ranking-period' | 'ranking-movie-list' | 'ranking-magnet-list'

const pageMode = computed<PageMode>(() => {
  const t = routeType.value
  if (!t) return 'type-select'
  if (t === 'actor') {
    if (!routeCategory.value) return 'collection-list'
    if (!routeMovieId.value) return 'movie-list'
    return 'magnet-list'
  }
  if (t === 'ranking') {
    if (!routeCategory.value) return 'ranking-category'
    if (!routePeriod.value) return 'ranking-period'
    if (!routeMovieId.value) return 'ranking-movie-list'
    return 'ranking-magnet-list'
  }
  return 'type-select'
})

// ===== 本地状态 =====
const loading = ref(false)
const searchQuery = ref('')
const selectedCollections = ref<Set<string>>(new Set())
const magnets = ref<Magnet[]>([])
// 标签/排除下拉互斥开关：'tag' | 'exclude' | 'check' + key，null 全关
const openMenu = ref<string | null>(null)
const top250Error = ref('')
const databaseCardEl = ref<HTMLElement | null>(null)
const databaseCardHeight = ref('auto')
let databaseLayoutFrame: number | null = null

// ===== 集合列表 =====
const filteredCollections = computed<Collection[]>(() => {
  const q = searchQuery.value.trim().toLowerCase()
  if (!q) return db.collections
  return db.collections.filter((c: Collection) =>
    displayName(c.name).toLowerCase().includes(q) || String(c.name).toLowerCase().includes(q)
  )
})

function toggleSelect(name: string) {
  const s = new Set(selectedCollections.value)
  if (s.has(name)) s.delete(name); else s.add(name)
  selectedCollections.value = s
}
function toggleSelectAll(checked: boolean) {
  if (checked) selectedCollections.value = new Set(filteredCollections.value.map((c) => c.name))
  else selectedCollections.value = new Set()
}
const allSelected = computed(() =>
  filteredCollections.value.length > 0 && filteredCollections.value.every((c) => selectedCollections.value.has(c.name))
)

// ===== 当前集合/排行影片数据 =====
const currentMovieData = computed(() => {
  if (routeType.value === 'actor' && routeCategory.value) return db.collectionData(routeCategory.value)
  if (routeType.value === 'ranking' && routeCategory.value && routePeriod.value) return db.rankingData(routeCategory.value, routePeriod.value)
  return { movies: [], available_tags: [], total_count: 0 }
})

// 当前过滤 key
const currentFilterKey = computed(() => {
  if (routeType.value === 'ranking' && routeCategory.value && routePeriod.value) return db.rankingFilterKey(routeCategory.value, routePeriod.value)
  return routeCategory.value || ''
})

function movieMatchesTags(movie: Movie, selected: string[], excluded: string[]): boolean {
  const movieTags = new Set(movie.tags || [])
  if (excluded.length && excluded.some((t) => movieTags.has(t))) return false
  if (!selected.length) return true
  return selected.every((t) => movieTags.has(t))
}

const filteredMovies = computed<Movie[]>(() => {
  const key = currentFilterKey.value
  const selected = db.getTagFilter(key)
  const excluded = db.getExcludeFilter(key)
  return (currentMovieData.value.movies || []).filter((m) => movieMatchesTags(m, selected, excluded))
})

// 健康度四宫格统计（按影片 magnet_health 计数，等价旧版 collectionHealthCounts）
function healthByMovieHealth(movies: Movie[]) {
  const counts: Record<string, number> = { active: 0, weak: 0, dead: 0, failed: 0 }
  for (const m of movies) {
    if (m.magnet_health && counts[m.magnet_health] !== undefined) counts[m.magnet_health] += 1
  }
  return counts
}
const healthMap = computed(() => healthByMovieHealth(filteredMovies.value))
function healthValue(itemKey: string): string {
  const map: Record<string, string> = { active_count: 'active', weak_count: 'weak', dead_count: 'dead', failed_count: 'failed' }
  const v = healthMap.value[map[itemKey]] || 0
  return v ? String(v) : '-'
}

const currentCollection = computed(() => routeCategory.value ? db.getCollection(routeCategory.value) : undefined)

// 当前影片（磁力页头部）
const currentMovie = computed<Movie | undefined>(() => {
  if (!routeMovieId.value) return undefined
  if (routeType.value === 'actor' && routeCategory.value) return db.movieById(routeCategory.value, routeMovieId.value)
  if (routeType.value === 'ranking' && routeCategory.value && routePeriod.value) return db.rankingMovieById(routeCategory.value, routePeriod.value, routeMovieId.value)
  return undefined
})

// ===== 排行 meta =====
function rankingCategoryMeta(key: string) { return RANKING_CATEGORIES.find((c) => c.key === key) || null }
function rankingPeriodMeta(key: string) { return RANKING_PERIODS.find((p) => p.key === key) || null }
function rankingPeriodForCategory(catKey: string, periodKey: string) {
  const cat = rankingCategoryMeta(catKey)
  if (!cat) return null
  if ((cat as { dynamicOptions?: boolean }).dynamicOptions) {
    const opt = (db.top250Options || []).find((o) => o.key === periodKey)
    if (opt) return opt
    return /^[A-Za-z0-9_-]{1,32}$/.test(periodKey || '') ? { key: periodKey, label: periodKey } : null
  }
  return rankingPeriodMeta(periodKey)
}

// ===== 导航 =====
function goType(type: string) { router.push(`/database/${type}`) }
function goCollectionList() { router.push('/database/actor') }
function goCollection(name: string) { router.push(`/database/actor/${encodeURIComponent(name)}`) }
function goMovie(movieId: string | number) { router.push(`/database/actor/${encodeURIComponent(routeCategory.value!)}/${encodeURIComponent(String(movieId))}`) }
function goRankingCategory() { router.push('/database/ranking') }
function goRankingPeriod(catKey: string) { router.push(`/database/ranking/${encodeURIComponent(catKey)}`) }
function goRankingMovieList(catKey: string, periodKey: string) { router.push(`/database/ranking/${encodeURIComponent(catKey)}/${encodeURIComponent(periodKey)}`) }
function goRankingMagnet(movieId: string | number) { router.push(`/database/ranking/${encodeURIComponent(routeCategory.value!)}/${encodeURIComponent(routePeriod.value!)}/${encodeURIComponent(String(movieId))}`) }

// ===== 面包屑 =====
const breadcrumbs = computed(() => {
  const items: { label: string; onClick?: () => void }[] = [
    { label: '数据库', onClick: () => router.push('/database') },
  ]
  const t = routeType.value
  if (t === 'actor') {
    items.push({ label: '演员', onClick: goCollectionList })
    if (routeCategory.value) {
      items.push({ label: displayName(routeCategory.value), onClick: () => goCollection(routeCategory.value!) })
      if (routeMovieId.value && currentMovie.value) items.push({ label: currentMovie.value.code || String(currentMovie.value.id) })
    }
  } else if (t === 'ranking') {
    items.push({ label: '排行榜', onClick: goRankingCategory })
    if (routeCategory.value) {
      const cat = rankingCategoryMeta(routeCategory.value)
      items.push({ label: cat?.label || routeCategory.value, onClick: () => goRankingPeriod(routeCategory.value!) })
      if (routePeriod.value) {
        const period = rankingPeriodForCategory(routeCategory.value, routePeriod.value)
        items.push({ label: period?.label || routePeriod.value, onClick: () => goRankingMovieList(routeCategory.value!, routePeriod.value!) })
        if (routeMovieId.value && currentMovie.value) items.push({ label: currentMovie.value.code || String(currentMovie.value.id) })
      }
    }
  }
  return items
})

// ===== 数据加载 =====
async function loadData() {
  loading.value = true
  top250Error.value = ''
  try {
    const mode = pageMode.value
    if (mode === 'movie-list') {
      await db.ensureMovies(routeCategory.value!)
    } else if (mode === 'magnet-list') {
      await db.ensureMovies(routeCategory.value!)
      magnets.value = await db.loadMovieMagnets(routeMovieId.value!)
    } else if (mode === 'ranking-period' && rankingCategoryMeta(routeCategory.value!)?.dynamicOptions) {
      try { if (!db.top250Options) await db.loadTop250Options() }
      catch (e: unknown) { top250Error.value = toErrMsg(e, 'TOP250 分类加载失败') }
    } else if (mode === 'ranking-movie-list') {
      if (rankingCategoryMeta(routeCategory.value!)?.dynamicOptions && !db.top250Options) {
        try { await db.loadTop250Options() } catch { /* ignore */ }
      }
      await db.ensureRankingMovies(routeCategory.value!, routePeriod.value!)
    } else if (mode === 'ranking-magnet-list') {
      await db.ensureRankingMovies(routeCategory.value!, routePeriod.value!)
      magnets.value = await db.loadMovieMagnets(routeMovieId.value!)
    }
  } catch (err: unknown) {
    showToast(toErrMsg(err, '加载失败'))
  } finally {
    loading.value = false
    await nextTick()
    scheduleFitDatabaseLayout()
    fitMovieTags(databaseCardEl.value ?? document)
  }
}

watch(() => route.fullPath, () => { openMenu.value = null; loadData() })

onMounted(async () => {
  if (!db.collections.length) await db.loadCollections()
  // 注册磁力检测轮询回调：tick 刷新当前磁力行，done 重载目标
  db.registerCheckCallbacks(onMagnetCheckTick, onMagnetCheckDone)
  await db.restoreMagnetCheckJob()
  await loadData()
  window.addEventListener('resize', onResize)
  window.addEventListener('click', onGlobalClick)
})

onBeforeUnmount(() => {
  db.registerCheckCallbacks(null, null)
  window.removeEventListener('resize', onResize)
  window.removeEventListener('click', onGlobalClick)
  if (databaseLayoutFrame !== null) cancelAnimationFrame(databaseLayoutFrame)
})

function fitDatabaseLayout() {
  const card = databaseCardEl.value
  if (!card) return
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0
  const cardTop = card.getBoundingClientRect().top
  const main = card.closest('main')
  const mainBottomPadding = main ? Number(window.getComputedStyle(main).paddingBottom.replace('px', '')) || 0 : 0
  const available = Math.floor(viewportHeight - cardTop - mainBottomPadding)
  if (available > 0) databaseCardHeight.value = `${available}px`
}

function scheduleFitDatabaseLayout() {
  if (databaseLayoutFrame !== null) return
  databaseLayoutFrame = requestAnimationFrame(() => {
    databaseLayoutFrame = null
    fitDatabaseLayout()
  })
}

function onResize() {
  scheduleFitDatabaseLayout()
  fitMovieTags(databaseCardEl.value ?? document)
}
function onGlobalClick() { openMenu.value = null }

// SSE 集合变化联动
async function refreshAfterCollectionsChanged() {
  await db.loadCollections()
  if (routeType.value === 'actor' && routeCategory.value) {
    await db.ensureMovies(routeCategory.value, true)
  } else if (routeType.value === 'ranking' && routeCategory.value && routePeriod.value) {
    await db.ensureRankingMovies(routeCategory.value, routePeriod.value, true)
  }
  if ((pageMode.value === 'magnet-list' || pageMode.value === 'ranking-magnet-list') && routeMovieId.value) {
    magnets.value = await db.loadMovieMagnets(routeMovieId.value)
    db.syncSelectedMagnetToCache(routeMovieId.value, magnets.value)
  }
  await nextTick()
  scheduleFitDatabaseLayout()
  fitMovieTags(databaseCardEl.value ?? document)
}

watch(() => tasks.collectionsChanged, async (changed) => {
  if (!changed) return
  tasks.clearCollectionsChanged()
  db.invalidateMovieCaches()
  try {
    await refreshAfterCollectionsChanged()
  } catch (err: unknown) {
    showToast(toErrMsg(err, '刷新数据失败'))
  }
}, { immediate: true })

// ===== 磁力检测轮询回调 =====
async function onMagnetCheckTick() {
  // 当前在磁力页且 job 命中当前影片/集合/排行 → 刷新磁力行
  const job = db.activeMagnetCheckJob
  if (!job) return
  if ((pageMode.value === 'magnet-list' || pageMode.value === 'ranking-magnet-list') && routeMovieId.value) {
    magnets.value = await db.loadMovieMagnets(routeMovieId.value)
  }
}
async function onMagnetCheckDone(job: MagnetCheckJob) {
  // 检测完成：按 scope 重载对应数据
  if (job.scope === 'collection' && routeCategory.value) {
    await db.ensureMovies(routeCategory.value, true)
  } else if (job.scope === 'ranking' && routeCategory.value && routePeriod.value) {
    await db.ensureRankingMovies(routeCategory.value, routePeriod.value, true)
  } else if (job.scope === 'all') {
    await db.loadCollections()
    if (routeType.value === 'actor' && routeCategory.value) await db.ensureMovies(routeCategory.value, true)
  } else if (job.scope === 'movie') {
    if (routeType.value === 'actor' && routeCategory.value) await db.ensureMovies(routeCategory.value, true)
    if (routeType.value === 'ranking' && routeCategory.value && routePeriod.value) await db.ensureRankingMovies(routeCategory.value, routePeriod.value, true)
  }
  if ((pageMode.value === 'magnet-list' || pageMode.value === 'ranking-magnet-list') && routeMovieId.value) {
    magnets.value = await db.loadMovieMagnets(routeMovieId.value)
  }
  await nextTick()
  fitMovieTags(databaseCardEl.value ?? document)
}

// ===== 操作 =====
async function selectMagnet(magnetId: number) {
  if (!routeMovieId.value) return
  try {
    await db.selectMagnet(routeMovieId.value, magnetId)
    magnets.value = await db.loadMovieMagnets(routeMovieId.value)
    db.syncSelectedMagnetToCache(routeMovieId.value, magnets.value)
  } catch (err: unknown) {
    showToast(toErrMsg(err, '更新失败'))
  }
}

async function autoSelect(collectionName?: string) {
  const names = collectionName ? [collectionName] : (selectedCollections.value.size ? Array.from(selectedCollections.value) : db.collections.map((c) => c.name))
  if (!names.length) { showToast('暂无可自动选择的集合'); return }
  const scopeText = collectionName ? `「${displayName(collectionName)}」` : (selectedCollections.value.size ? `${selectedCollections.value.size} 个已选集合` : '全部集合')
  if (!confirm(`按评分自动选择 ${scopeText} 的推荐磁力？`)) return
  try {
    const msg = await db.autoSelectMagnets(names)
    showToast(msg)
    await db.loadCollections()
    if (collectionName) await db.ensureMovies(collectionName, true)
  } catch (err: unknown) {
    showToast(toErrMsg(err, '自动选择失败'))
  }
}

async function deleteCollection(name: string) {
  if (!confirm(`确定删除 1 个数据库集合吗？`)) return
  try {
    const msg = await db.deleteCollection(name)
    showToast(msg)
    if (pageMode.value !== 'collection-list') goCollectionList()
  } catch (err: unknown) {
    showToast(toErrMsg(err, '删除失败'))
  }
}

function batchDeleteSelected() {
  const names = Array.from(selectedCollections.value)
  if (!names.length) return
  if (!confirm(`确定删除 ${names.length} 个数据库集合吗？`)) return
  db.batchDeleteCollections(names).then((msg) => {
    selectedCollections.value = new Set()
    showToast(msg)
  }).catch((err: unknown) => showToast(toErrMsg(err, '批量删除失败')))
}

async function copyCollectionMagnets(name: string) {
  try {
    const links = await db.getCollectionMagnetLinks(name)
    if (!links.length) { showToast('暂无磁力链接可复制'); return }
    const ok = await copyText(links.join('\n'))
    showToast(ok ? `已复制 ${links.length} 条磁力链接` : '自动复制失败，请在弹窗中手动复制磁力链接')
  } catch (err: unknown) {
    showToast(toErrMsg(err, '复制失败'))
  }
}

async function downloadCsv(name: string) {
  try { await apiDownloadBlob(db.getCollectionDownloadUrl(name), name) }
  catch (err: unknown) { showToast(toErrMsg(err, '下载失败')) }
}

async function enqueueIncremental(name: string) {
  if (!confirm(`确定对「${displayName(name)}」执行增量爬取吗？`)) return
  try {
    const msg = await db.enqueueCollectionIncremental(name)
    showToast(msg)
    await tasks.refresh()
  } catch (err: unknown) {
    showToast(toErrMsg(err, '添加增量任务失败'))
  }
}

// ===== 排行操作 =====
async function copyRankingMagnets(catKey: string, periodKey: string) {
  try {
    const links = await db.getRankingMagnetLinks(catKey, periodKey)
    if (!links.length) { showToast('暂无磁力链接可复制'); return }
    const ok = await copyText(links.join('\n'))
    showToast(ok ? `已复制 ${links.length} 条磁力链接` : '自动复制失败，请在弹窗中手动复制磁力链接')
  } catch (err: unknown) {
    showToast(toErrMsg(err, '复制失败'))
  }
}

async function downloadRankingCsv(catKey: string, periodKey: string) {
  try { await apiDownloadBlob(db.getRankingDownloadUrl(catKey, periodKey), `ranking_${catKey}_${periodKey}.csv`) }
  catch (err: unknown) { showToast(toErrMsg(err, '下载失败')) }
}

async function updateRankingList(catKey: string, periodKey: string) {
  try {
    const res = await db.createRankingUpdateTask(catKey, periodKey)
    if (res.code !== 200) { showToast(res.msg || '添加更新任务失败'); return }
    showToast(res.msg || '榜单更新任务已加入队列')
    await tasks.refresh()
  } catch (err: unknown) {
    showToast(toErrMsg(err, '添加更新任务失败'))
  }
}

async function clearRankingList(catKey: string, periodKey: string) {
  if (!confirm('确定清空当前榜单吗？')) return
  try {
    const msg = await db.clearRankingList(catKey, periodKey)
    showToast(msg)
    await db.ensureRankingMovies(catKey, periodKey, true)
  } catch (err: unknown) {
    showToast(toErrMsg(err, '清空失败'))
  }
}

async function refreshTop250() {
  try {
    await db.loadTop250Options(true)
    showToast('TOP250 分类已刷新')
    top250Error.value = ''
  } catch (err: unknown) {
    const msg = toErrMsg(err, '刷新分类失败')
    top250Error.value = msg
    showToast(msg)
  }
}

</script>

<template>
  <section class="space-y-4">
    <section ref="databaseCardEl" class="card flex min-h-0 min-w-0 flex-col overflow-hidden" :style="{ height: databaseCardHeight }">
      <!-- 面包屑 -->
      <div class="shrink-0 border-b border-soft px-5 py-3 text-sm text-muted">
        <div class="flex min-w-0 flex-wrap items-center gap-2">
          <template v-for="(bc, idx) in breadcrumbs" :key="idx">
            <span v-if="idx > 0" class="text-subtle">/</span>
            <button v-if="bc.onClick" type="button" @click="bc.onClick" class="font-bold text-[color:var(--c-primary-text)] hover:underline">{{ bc.label }}</button>
            <span v-else class="font-bold text-[color:var(--c-text)] max-w-[42vw] truncate">{{ bc.label }}</span>
          </template>
        </div>
      </div>

      <!-- 集合列表工具栏（仅集合列表页可见）-->
      <div v-if="pageMode === 'collection-list'" class="shrink-0 border-b border-soft px-5 pb-2 pt-2">
        <div class="flex items-center justify-between gap-1">
          <div class="shrink-0 whitespace-nowrap text-[11px] text-muted">{{ db.collections.length }} 个集合 · {{ db.totalMovies() }} 部影片</div>
          <div class="flex shrink-0 items-center gap-1">
            <button type="button" title="按评分自动选择磁力" aria-label="按评分自动选择磁力" class="btn btn-sm btn-warning" @click="autoSelect()">★ 自动选择</button>
            <MagnetCheckButton scope="all" target="all" compact />
            <button
              v-if="selectedCollections.size > 0"
              type="button"
              :title="`批量删除 ${selectedCollections.size} 个数据集合`"
              :aria-label="`批量删除 ${selectedCollections.size} 个数据集合`"
              class="btn btn-icon-sm btn-danger relative"
              @click="batchDeleteSelected"
            >
              <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 15h10l1-15"></path><path d="M10 11v6"></path><path d="M14 11v6"></path>
              </svg>
              <span class="absolute -right-1 -top-1 min-w-4 rounded-full bg-red-600 px-1 text-center text-[10px] leading-4 text-white">{{ selectedCollections.size }}</span>
            </button>
            <button type="button" title="刷新" aria-label="刷新数据集合" class="btn btn-icon-sm btn-soft" @click="db.loadCollections()">
              <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 12a9 9 0 1 1-2.64-6.36"></path><path d="M21 3v6h-6"></path>
              </svg>
            </button>
          </div>
        </div>
      </div>

      <!-- 内容区 -->
      <div class="flex min-h-0 flex-1 flex-col overflow-hidden">
        <!-- 加载中 -->
        <div v-if="loading" class="empty-state flex-1 flex-col gap-3" role="status" aria-busy="true">
          <span class="spinner-ring" aria-hidden="true"></span>
          <span>加载中...</span>
        </div>

        <!-- 类型选择 -->
        <template v-else-if="pageMode === 'type-select'">
          <div class="shrink-0 border-b border-soft px-5 py-3">
            <div class="text-sm font-bold text-[color:var(--c-text)]">类型</div>
          </div>
          <div class="min-h-0 flex-1 overflow-y-auto bg-surface-sunken p-4">
            <div class="grid gap-3 md:grid-cols-2">
              <button type="button" @click="goType('ranking')" class="group flex min-h-[56px] items-center justify-between gap-3 rounded border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
                <span class="shrink-0 text-sm font-bold text-[color:var(--c-text)] md:text-base">排行榜</span>
                <span class="min-w-0 truncate text-xs font-bold text-subtle group-hover:text-primary-text">{{ RANKING_CATEGORIES.length }} 个分类</span>
              </button>
              <button type="button" @click="goType('actor')" class="group flex min-h-[56px] items-center justify-between gap-3 rounded border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
                <span class="shrink-0 text-sm font-bold text-[color:var(--c-text)] md:text-base">演员</span>
                <span class="min-w-0 truncate text-xs font-bold text-subtle group-hover:text-primary-text">{{ db.collections.length }} 个集合 · {{ db.totalMovies() }} 部影片</span>
              </button>
            </div>
          </div>
        </template>

        <!-- 集合列表 -->
        <template v-else-if="pageMode === 'collection-list'">
          <div class="shrink-0 border-b border-soft px-4 pb-4 pt-3">
            <div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <input v-model="searchQuery" type="search" class="input md:max-w-sm" placeholder="搜索数据集合" />
              <label v-if="db.collections.length" class="flex items-center gap-2 text-xs font-bold text-muted">
                <input type="checkbox" class="accent-[color:var(--c-primary)]" :checked="allSelected" @change="toggleSelectAll(($event.target as HTMLInputElement).checked)" />
                <span>全选当前列表</span>
              </label>
            </div>
          </div>
          <div class="min-h-0 flex-1 divide-y divide-[color:var(--c-border)] overflow-y-auto bg-surface-sunken">
            <div v-if="!filteredCollections.length" class="empty-state px-6 py-10">
              {{ db.collections.length ? '没有匹配的数据集合' : '暂无数据库集合' }}
            </div>
            <div v-for="item in filteredCollections" :key="item.name" class="group flex items-start gap-3 px-4 py-3 hover:bg-surface-sunken">
              <input type="checkbox" class="mt-1 accent-[color:var(--c-primary)]" :checked="selectedCollections.has(item.name)" @change="toggleSelect(item.name)" @click.stop />
              <button type="button" @click="goCollection(item.name)" class="min-w-0 flex-1 text-left">
                <div class="flex min-w-0 items-center justify-between gap-2">
                  <div class="truncate text-sm font-bold text-[color:var(--c-text)] md:text-base" :title="displayName(item.name)">{{ displayName(item.name) }}</div>
                  <span class="badge badge-info shrink-0 text-[11px]">{{ item.count }}</span>
                </div>
                <div class="mt-1 truncate text-xs text-subtle">{{ item.time }} · {{ (item.tags || []).length }} 个标签</div>
              </button>
            </div>
          </div>
        </template>
        <!-- 影片列表（演员集合）-->
        <template v-else-if="pageMode === 'movie-list'">
          <!-- 集合工具栏头：影片/标签数 + 健康四宫格 + 集合级检测 -->
          <div class="shrink-0 border-b border-soft px-5 pb-2 pt-2">
            <div class="flex min-w-0 items-center justify-between gap-2 text-xs text-muted">
              <div class="min-w-0 flex-1">
                <div class="flex min-w-0 flex-wrap items-center gap-2">
                  <span class="shrink-0">{{ Number(currentCollection?.count || 0) }} 部影片 · {{ (currentCollection?.tags || []).length }} 个标签</span>
                  <div class="flex shrink-0 items-center gap-1" aria-label="磁力检测影片统计">
                    <span v-for="h in HEALTH_ITEMS" :key="h.key" :title="h.title" class="badge min-w-[4ch] px-1 text-[10px]" :class="h.badge">{{ healthValue(h.key) }}</span>
                  </div>
                </div>
                <div class="mt-0.5 truncate text-[11px] leading-none text-subtle">{{ currentCollection?.time || '-' }}</div>
              </div>
              <MagnetCheckButton scope="collection" :target="routeCategory!" />
            </div>
          </div>
          <!-- 过滤行 + 影片列表 -->
          <div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface-sunken px-4 pb-4 pt-3 text-sm text-muted">
            <div class="mb-3 flex shrink-0 flex-wrap items-center gap-2">
              <TagFilterDropdown
                v-model:open-menu="openMenu"
                :filter-key="currentFilterKey"
                :available-tags="currentMovieData.available_tags"
                :filtered-count="filteredMovies.length"
                :total-count="currentMovieData.movies.length"
              />
              <!-- 工具栏动作 -->
              <div class="ml-auto flex shrink-0 items-center gap-1">
                <button type="button" title="复制集合磁力" aria-label="复制集合磁力" class="btn btn-icon-sm btn-info text-xs" @click="copyCollectionMagnets(routeCategory!)">⧉</button>
                <button type="button" title="下载 CSV" aria-label="下载 CSV" class="btn btn-icon-sm btn-success text-xs" @click="downloadCsv(routeCategory!)">⇩</button>
                <button v-if="currentCollection?.has_source_url" type="button" title="增量爬取此集合" aria-label="增量爬取此集合" class="btn btn-icon-sm btn-info text-xs" @click="enqueueIncremental(routeCategory!)">⟳</button>
                <button v-else type="button" disabled title="缺少原始 URL，无法快捷增量" aria-label="缺少原始 URL，无法快捷增量" class="btn btn-icon-sm btn-soft text-xs">⟳</button>
                <button type="button" title="删除集合" aria-label="删除集合" class="btn btn-icon-sm btn-danger" @click="deleteCollection(routeCategory!)">
                  <svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 15h10l1-15"></path><path d="M10 11v6"></path><path d="M14 11v6"></path>
                  </svg>
                </button>
              </div>
            </div>
            <!-- 影片列表 -->
            <div v-if="!filteredMovies.length" class="empty-state">暂无匹配影片记录</div>
            <div v-else class="min-h-0 flex-1 max-w-full divide-y divide-[color:var(--c-border)] overflow-y-auto rounded-lg border border-[color:var(--c-border)] bg-surface">
              <MovieListItem v-for="movie in filteredMovies" :key="movie.id" :movie="movie" @open="goMovie" />
            </div>
          </div>
        </template>

        <!-- 候选磁力表（演员）-->
        <template v-else-if="pageMode === 'magnet-list'">
          <MovieMagnetHeader :movie="currentMovie" />
          <div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface-sunken p-4">
            <MagnetTable :movie-id="routeMovieId!" :magnets="magnets" @select="selectMagnet" />
          </div>
        </template>
        <!-- 排行榜分类 -->
        <template v-else-if="pageMode === 'ranking-category'">
          <div class="shrink-0 border-b border-soft px-5 py-3">
            <div class="text-sm font-bold text-[color:var(--c-text)]">排行榜分类</div>
          </div>
          <div class="min-h-0 flex-1 overflow-y-auto bg-surface-sunken p-4">
            <div class="grid gap-3 md:grid-cols-2">
              <button v-for="cat in RANKING_CATEGORIES" :key="cat.key" type="button" @click="goRankingPeriod(cat.key)" class="group flex min-h-[56px] items-center gap-3 rounded border border-[color:var(--c-border)] bg-surface px-4 py-3 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
                <span class="shrink-0 text-sm font-bold leading-none text-[color:var(--c-text)] md:text-base">{{ cat.label }}</span>
                <span class="text-xs font-bold leading-none text-subtle group-hover:text-primary-text">{{ (cat as { subLabel?: string }).subLabel || '日榜 · 周榜 · 月榜' }}</span>
              </button>
            </div>
          </div>
        </template>

        <!-- 排行榜周期 / TOP250 动态选项 -->
        <template v-else-if="pageMode === 'ranking-period'">
          <template v-if="rankingCategoryMeta(routeCategory!)?.dynamicOptions">
            <div class="shrink-0 border-b border-soft px-5 py-3">
              <div class="flex items-center justify-between gap-3">
                <div class="min-w-0">
                  <div class="text-sm font-bold text-[color:var(--c-text)]">{{ rankingCategoryMeta(routeCategory!)?.label }}</div>
                  <div class="mt-1 truncate text-xs text-subtle">{{ db.top250Stale ? '使用本地缓存' : '动态分类' }} · {{ (db.top250Options || []).length }} 个选项</div>
                </div>
                <button type="button" class="btn btn-sm btn-info shrink-0 text-xs" @click="refreshTop250">刷新分类</button>
              </div>
            </div>
            <div class="min-h-0 flex-1 overflow-y-auto bg-surface-sunken p-4">
              <div v-if="top250Error" class="empty-state flex-1 flex-col gap-3 px-6 py-10">
                <div>{{ top250Error }}</div>
                <button type="button" class="btn btn-sm btn-info text-xs" @click="refreshTop250">刷新分类</button>
              </div>
              <div v-else-if="!(db.top250Options || []).length" class="empty-state flex-1 flex-col gap-3 px-6 py-10">
                <div>暂无 TOP250 分类，请点击刷新分类</div>
                <button type="button" class="btn btn-sm btn-info text-xs" @click="refreshTop250">刷新分类</button>
              </div>
              <div v-else class="grid gap-3 md:grid-cols-3">
                <button v-for="opt in db.top250Options" :key="opt.key" type="button" @click="goRankingMovieList(routeCategory!, opt.key)" class="group flex min-h-[56px] items-center justify-between gap-3 rounded border border-[color:var(--c-border)] bg-surface px-4 py-3 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
                  <span class="shrink-0 text-sm font-bold leading-none text-[color:var(--c-text)] md:text-base">{{ opt.label }}</span>
                  <span class="min-w-0 truncate text-xs font-bold leading-none text-subtle group-hover:text-primary-text">影片列表</span>
                </button>
              </div>
            </div>
          </template>
          <template v-else>
            <div class="shrink-0 border-b border-soft px-5 py-3">
              <div class="text-sm font-bold text-[color:var(--c-text)]">{{ rankingCategoryMeta(routeCategory!)?.label }}</div>
            </div>
            <div class="min-h-0 flex-1 overflow-y-auto bg-surface-sunken p-4">
              <div class="grid gap-3 md:grid-cols-3">
                <button v-for="period in RANKING_PERIODS" :key="period.key" type="button" @click="goRankingMovieList(routeCategory!, period.key)" class="group flex min-h-[56px] items-center justify-between gap-3 rounded border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
                  <span class="shrink-0 text-sm font-bold text-[color:var(--c-text)] md:text-base">{{ period.label }}</span>
                  <span class="min-w-0 truncate text-xs font-bold text-subtle group-hover:text-primary-text">影片列表</span>
                </button>
              </div>
            </div>
          </template>
        </template>

        <!-- 排行榜影片列表 -->
        <template v-else-if="pageMode === 'ranking-movie-list'">
          <div class="shrink-0 border-b border-soft px-5 pb-2 pt-2">
            <div class="flex min-w-0 flex-wrap items-center justify-between gap-2 text-xs text-muted">
              <div class="flex min-w-0 flex-wrap items-center gap-2">
                <span class="min-w-0 truncate">{{ rankingCategoryMeta(routeCategory!)?.label }} · {{ rankingPeriodForCategory(routeCategory!, routePeriod!)?.label || routePeriod }} · {{ Number(currentMovieData.total_count || currentMovieData.movies.length) }} 部影片</span>
                <div class="flex shrink-0 items-center gap-1" aria-label="磁力检测影片统计">
                  <span v-for="h in HEALTH_ITEMS" :key="h.key" :title="h.title" class="badge min-w-[4ch] px-1 text-[10px]" :class="h.badge">{{ healthValue(h.key) }}</span>
                </div>
              </div>
              <MagnetCheckButton scope="ranking" :target="`${routeCategory}:${routePeriod}`" />
            </div>
          </div>
          <div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface-sunken px-4 pb-4 pt-3 text-sm text-muted">
            <div class="mb-3 flex shrink-0 flex-wrap items-center gap-2">
              <TagFilterDropdown
                v-model:open-menu="openMenu"
                :filter-key="currentFilterKey"
                :available-tags="currentMovieData.available_tags"
                :filtered-count="filteredMovies.length"
                :total-count="Number(currentMovieData.total_count || currentMovieData.movies.length)"
              />
              <!-- 榜单工具栏动作 -->
              <div class="ml-auto flex shrink-0 items-center gap-1">
                <button type="button" title="复制榜单磁力" aria-label="复制榜单磁力" class="btn btn-icon-sm btn-info text-xs" @click="copyRankingMagnets(routeCategory!, routePeriod!)">⧉</button>
                <button type="button" title="下载榜单 CSV" aria-label="下载榜单 CSV" class="btn btn-icon-sm btn-success text-xs" @click="downloadRankingCsv(routeCategory!, routePeriod!)">⇩</button>
                <button type="button" title="更新榜单" aria-label="更新榜单" class="btn btn-icon-sm btn-info text-xs" @click="updateRankingList(routeCategory!, routePeriod!)">⟳</button>
                <button type="button" title="清空榜单" aria-label="清空榜单" class="btn btn-icon-sm btn-danger" @click="clearRankingList(routeCategory!, routePeriod!)">
                  <svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 15h10l1-15"></path><path d="M10 11v6"></path><path d="M14 11v6"></path>
                  </svg>
                </button>
              </div>
            </div>
            <!-- 榜单影片列表 -->
            <div v-if="!filteredMovies.length" class="min-h-0 flex-1 max-w-full overflow-y-auto rounded-lg border border-[color:var(--c-border)] bg-surface">
              <div class="empty-state px-6 py-10">暂无榜单影片</div>
            </div>
            <div v-else class="min-h-0 flex-1 max-w-full divide-y divide-[color:var(--c-border)] overflow-y-auto rounded-lg border border-[color:var(--c-border)] bg-surface">
              <MovieListItem v-for="movie in filteredMovies" :key="movie.id" :movie="movie" highlight-on-hover @open="goRankingMagnet" />
            </div>
          </div>
        </template>

        <!-- 候选磁力表（排行榜）-->
        <template v-else-if="pageMode === 'ranking-magnet-list'">
          <MovieMagnetHeader :movie="currentMovie" />
          <div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface-sunken p-4">
            <MagnetTable :movie-id="routeMovieId!" :magnets="magnets" @select="selectMagnet" />
          </div>
        </template>
      </div>
    </section>
  </section>
</template>
