<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useDatabaseStore, RANKING_CATEGORIES, RANKING_PERIODS } from '@/stores/database'
import { useTasksStore } from '@/stores/tasks'
import { useToast } from '@/composables/useToast'
import { useClipboard } from '@/composables/useClipboard'
import { apiFetch } from '@/api'
import type { Collection, Movie, Magnet } from '@/types'

const route = useRoute()
const router = useRouter()
const db = useDatabaseStore()
const tasks = useTasksStore()
const { showToast } = useToast()
const { copyText } = useClipboard()

// Route interpretation
const routeType = computed(() => (route.params.type as string) || null)
const routeCategory = computed(() => (route.params.category as string) || null)
const routePeriod = computed(() => (route.params.period as string) || null)
const routeMovieId = computed(() => (route.params.movieId as string) || null)

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

// Data
const movies = ref<Movie[]>([])
const magnets = ref<Magnet[]>([])
const loading = ref(false)
const searchQuery = ref('')
const selectedCollections = ref<Set<string>>(new Set())

const filteredCollections = computed(() => {
  const q = searchQuery.value.trim().toLowerCase()
  if (!q) return db.collections
  return db.collections.filter((c: Record<string, unknown>) => {
    const n = String(c.name || '').toLowerCase()
    return n.includes(q)
  })
})

const totalCount = computed(() =>
  db.collections.reduce((s: number, c: Record<string, unknown>) => s + Number((c as { count?: number }).count || 0), 0)
)

function displayName(val: string) { return String(val || '').replace(/\.csv$/i, '') }

function rankingCategoryMeta(key: string) { return RANKING_CATEGORIES.find((c: { key: string }) => c.key === key) || null }
function rankingPeriodMeta(key: string) { return RANKING_PERIODS.find((p: { key: string }) => p.key === key) || null }

function rankingPeriodForCategory(catKey: string, periodKey: string) {
  const cat = rankingCategoryMeta(catKey)
  if (!cat) return null
  if ((cat as { dynamicOptions?: boolean }).dynamicOptions) {
    const opt = (db.top250Options || []).find((o: { key: string; label: string }) => o.key === periodKey)
    if (opt) return opt
    return /^[A-Za-z0-9_-]{1,32}$/.test(periodKey || '') ? { key: periodKey, label: periodKey } : null
  }
  return rankingPeriodMeta(periodKey)
}

// Navigation
function goType(type: string) { router.push(`/database/${type}`) }
function goCollection(name: string) { router.push(`/database/actor/${encodeURIComponent(name)}`) }
function goMovie(collectionName: string, movieId: string | number) {
  router.push(`/database/actor/${encodeURIComponent(collectionName)}/${encodeURIComponent(String(movieId))}`)
}
function goBack() { router.go(-1) }
function goCollectionList() { router.push('/database/actor') }
function goRankingCategory() { router.push('/database/ranking') }
function goRankingPeriod(catKey: string) { router.push(`/database/ranking/${encodeURIComponent(catKey)}`) }
function goRankingMovieList(catKey: string, periodKey: string) {
  router.push(`/database/ranking/${encodeURIComponent(catKey)}/${encodeURIComponent(periodKey)}`)
}
function goRankingMagnet(catKey: string, periodKey: string, movieId: string | number) {
  router.push(`/database/ranking/${encodeURIComponent(catKey)}/${encodeURIComponent(periodKey)}/${encodeURIComponent(String(movieId))}`)
}

// Breadcrumb items
const breadcrumbs = computed(() => {
  const items: { label: string; onClick?: () => void }[] = [
    { label: '数据库', onClick: () => router.push('/database') },
  ]
  const t = routeType.value
  if (t === 'actor') {
    items.push({ label: '演员', onClick: goCollectionList })
    const cn = routeCategory.value
    if (cn) {
      items.push({ label: displayName(cn), onClick: () => goCollection(cn) })
      const mid = routeMovieId.value
      if (mid && movies.value.length) {
        const m = movies.value.find((mv: Movie) => String(mv.id) === mid)
        if (m) items.push({ label: m.code || String(m.id) })
      }
    }
  } else if (t === 'ranking') {
    items.push({ label: '排行榜', onClick: goRankingCategory })
    const catKey = routeCategory.value
    if (catKey) {
      const cat = rankingCategoryMeta(catKey)
      items.push({ label: cat?.label || catKey, onClick: () => goRankingPeriod(catKey) })
      const periodKey = routePeriod.value
      if (periodKey) {
        const period = rankingPeriodForCategory(catKey, periodKey)
        items.push({ label: period?.label || periodKey, onClick: () => goRankingMovieList(catKey, periodKey) })
        const mid = routeMovieId.value
        if (mid && movies.value.length) {
          const m = movies.value.find((mv: Movie) => String(mv.id) === mid)
          if (m) items.push({ label: m.code || String(m.id) })
        }
      }
    }
  }
  return items
})

// Data loading
async function loadData() {
  loading.value = true
  try {
    const mode = pageMode.value
    if (mode === 'movie-list') {
      const cn = routeCategory.value!
      const ok = await db.ensureMovies(cn)
      if (ok) movies.value = db.collectionMovies[cn] || []
    } else if (mode === 'magnet-list') {
      const cn = routeCategory.value!
      const mid = routeMovieId.value!
      const ok = await db.ensureMovies(cn)
      if (ok) movies.value = db.collectionMovies[cn] || []
      magnets.value = await db.loadMovieMagnets(cn, mid)
    } else if (mode === 'ranking-movie-list') {
      movies.value = await db.loadRankingMovies(routeCategory.value!, routePeriod.value!)
    } else if (mode === 'ranking-magnet-list') {
      movies.value = await db.loadRankingMovies(routeCategory.value!, routePeriod.value!)
      magnets.value = await db.loadMovieMagnets(
        `ranking:${routeCategory.value}:${routePeriod.value}`,
        routeMovieId.value!
      )
    } else if (mode === 'ranking-period' && rankingCategoryMeta(routeCategory.value!)?.dynamicOptions) {
      if (!db.top250Options) await db.loadTop250Options()
    }
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '加载失败')
  } finally {
    loading.value = false
  }
}

watch(() => route.fullPath, loadData, { immediate: false })

onMounted(async () => {
  if (!db.collections.length) await db.loadCollections()
  await loadData()
})

// Tasks store watch for cross-view update
// SSE 方式：collectionsChanged 由 SSE 事件直接设置；轮询兜底：监听任务完成
watch(() => tasks.collectionsChanged, async (changed) => {
  if (changed) {
    await db.loadCollections()
    tasks.clearCollectionsChanged()
  }
})
// 轮询兜底（SSE 未连接时）
watch(() => tasks.tasks.length, async () => {
  if (tasks.sseConnected) return
  const finishedTask = tasks.tasks.find((t: { state: string }) => t.state === 'finished')
  if (finishedTask) await db.loadCollections()
})

// Collection actions
async function deleteCollection(name: string) {
  if (!confirm(`确定删除集合 ${displayName(name)} 吗？此操作不可恢复。`)) return
  try {
    await db.deleteCollection(name)
    showToast('已删除')
    if (pageMode.value !== 'collection-list') goCollectionList()
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '删除失败')
  }
}

async function autoSelect(collectionName?: string) {
  try {
    const msg = await db.autoSelectMagnets(collectionName)
    showToast(msg)
    if (pageMode.value === 'movie-list' && routeCategory.value) {
      const ok = await db.ensureMovies(routeCategory.value)
      if (ok) movies.value = db.collectionMovies[routeCategory.value] || []
    }
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '自动选择失败')
  }
}

async function selectMagnet(movieId: string | number, magnetId: number) {
  if (!routeCategory.value) return
  await db.selectMagnet(routeCategory.value, movieId, magnetId)
  magnets.value = await db.loadMovieMagnets(routeCategory.value, movieId)
}

async function copyCollectionMagnets(name: string) {
  try {
    const res = await apiFetch(`/api/magnets?name=${encodeURIComponent(name)}`).then((r: Response) => r.json())
    if (res.code !== 200) { showToast(res.msg || '读取失败'); return }
    const links: string[] = res.data || []
    if (!links.length) { showToast('该集合暂无已选磁力'); return }
    const copied = await copyText(links.join('\n'))
    showToast(copied ? `已复制 ${links.length} 条磁力` : '自动复制失败，请手动复制')
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '复制失败')
  }
}

async function copyRankingMagnets(catKey: string, periodKey: string) {
  try {
    const links = await db.getRankingMagnets(catKey, periodKey)
    if (!links.length) { showToast('该排行榜暂无已选磁力'); return }
    const copied = await copyText(links.join('\n'))
    showToast(copied ? `已复制 ${links.length} 条磁力` : '自动复制失败，请手动复制')
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '复制失败')
  }
}

async function handleRankingUpdateTask(catKey: string, periodKey: string) {
  try {
    const res = await db.createRankingUpdateTask(catKey, periodKey)
    if (res.code === 409 && res.needs_mode) {
      const useIncremental = confirm(`检测到已有数据：${res.filename || ''}\n确定使用增量，取消使用覆盖。`)
      if (useIncremental) {
        // 重试增量
        await db.createRankingUpdateTask(catKey, periodKey)
      }
      return
    }
    if (res.code !== 200) { showToast(res.msg || '添加任务失败'); return }
    showToast(res.msg || '排行榜更新任务已加入队列')
    await tasks.refresh()
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '添加任务失败')
  }
}

function batchDeleteSelected() {
  const names = Array.from(selectedCollections.value)
  if (!names.length) return
  if (!confirm(`确定删除选中的 ${names.length} 个集合吗？此操作不可恢复。`)) return
  db.batchDeleteCollections(names).then(() => {
    selectedCollections.value.clear()
    showToast('已批量删除')
  }).catch((err: unknown) => showToast(err instanceof Error ? err.message : '批量删除失败'))
}

function toggleSelect(name: string) {
  const s = new Set(selectedCollections.value)
  if (s.has(name)) s.delete(name)
  else s.add(name)
  selectedCollections.value = s
}

// Magnet display
const MAGNET_STATUS: Record<string, { icon: string; title: string; cls: string }> = {
  active: { icon: '🟢', title: '有效', cls: 'text-success-text' },
  weak:   { icon: '🟡', title: '弱',   cls: 'text-warning-text' },
  dead:   { icon: '🔴', title: '无效', cls: 'text-danger-text' },
}
function magnetStatusMeta(m: Magnet) {
  if (m.check_error && !m.check_status) return { icon: '❌', title: m.check_error, cls: 'text-[color:var(--c-text-muted)]' }
  if (!m.checked_at) return { icon: '⚪', title: '未检测', cls: 'text-[color:var(--c-text-subtle)]' }
  const meta = MAGNET_STATUS[m.check_status || '']
  if (meta) return m.check_status === 'dead' ? { ...meta, title: m.check_error || meta.title } : meta
  return { icon: '❌', title: m.check_error || '检测失败', cls: 'text-[color:var(--c-text-muted)]' }
}

function formatGb(mb: number) { return `${(Number(mb || 0) / 1024).toFixed(1)} GB` }
</script>

<template>
  <section class="space-y-4">
    <section class="card flex h-[calc(100dvh-190px)] min-h-0 min-w-0 flex-col overflow-hidden">
      <!-- 面包屑 -->
      <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 py-3 text-sm text-[color:var(--c-text-muted)]">
        <div class="flex min-w-0 flex-wrap items-center gap-2">
          <template v-for="(bc, idx) in breadcrumbs" :key="idx">
            <span v-if="idx > 0" class="text-[color:var(--c-text-subtle)]">/</span>
            <button v-if="bc.onClick" type="button" @click="bc.onClick" class="font-bold text-[color:var(--c-primary-text)] hover:underline">{{ bc.label }}</button>
            <span v-else class="font-bold text-[color:var(--c-text)] max-w-[42vw] truncate">{{ bc.label }}</span>
          </template>
        </div>
      </div>

      <!-- 集合列表工具栏 -->
      <div v-if="pageMode === 'collection-list'" class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 pb-2 pt-2">
        <div class="mb-3 text-xs text-[color:var(--c-text-muted)]">{{ db.collections.length }} 个集合 · {{ totalCount }} 部影片</div>
        <div class="flex flex-wrap gap-2">
          <button @click="autoSelect()" class="btn btn-md btn-warning">★ 自动选择</button>
          <button v-if="selectedCollections.size > 0" @click="batchDeleteSelected" class="btn btn-icon-md btn-danger relative">
            <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 15h10l1-15"></path><path d="M10 11v6"></path><path d="M14 11v6"></path>
            </svg>
            <span class="absolute -right-1 -top-1 min-w-4 rounded-full bg-red-600 px-1 text-center text-[10px] leading-4 text-white">{{ selectedCollections.size }}</span>
          </button>
          <button @click="db.loadCollections()" class="btn btn-icon-md btn-soft">
            <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <path d="M21 12a9 9 0 1 1-2.64-6.36"></path><path d="M21 3v6h-6"></path>
            </svg>
          </button>
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
          <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 py-3">
            <div class="text-sm font-bold text-[color:var(--c-text)]">类型</div>
          </div>
          <div class="min-h-0 flex-1 overflow-y-auto p-4">
            <div class="grid gap-3 md:grid-cols-2">
              <button type="button" @click="goType('ranking')" class="group flex min-h-[92px] flex-col items-start justify-between rounded border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
                <span class="text-base font-bold text-[color:var(--c-text)]">排行榜</span>
                <span class="text-xs font-bold text-[color:var(--c-text-subtle)] group-hover:text-primary-text">{{ RANKING_CATEGORIES.length }} 个分类</span>
              </button>
              <button type="button" @click="goType('actor')" class="group flex min-h-[92px] flex-col items-start justify-between rounded border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
                <span class="text-base font-bold text-[color:var(--c-text)]">演员</span>
                <span class="text-xs font-bold text-[color:var(--c-text-subtle)] group-hover:text-primary-text">{{ db.collections.length }} 个集合 · {{ totalCount }} 部影片</span>
              </button>
            </div>
          </div>
        </template>

        <!-- 集合列表 -->
        <template v-else-if="pageMode === 'collection-list'">
          <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-4 pb-4 pt-3">
            <div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <input v-model="searchQuery" type="search" class="input md:max-w-sm" placeholder="搜索数据集合" />
              <label class="flex items-center gap-2 text-xs font-bold text-[color:var(--c-text-muted)]">
                <input type="checkbox" class="accent-[color:var(--c-primary)]"
                  :checked="selectedCollections.size === filteredCollections.length && filteredCollections.length > 0"
                  @change="(e) => { if ((e.target as HTMLInputElement).checked) { filteredCollections.forEach((c: { name: string }) => selectedCollections.add(c.name)) } else { selectedCollections.clear() } }"
                />
                全选当前列表
              </label>
            </div>
          </div>
          <div class="min-h-0 flex-1 divide-y divide-[color:var(--c-border)] overflow-y-auto">
            <div v-if="!filteredCollections.length" class="empty-state px-6 py-10">
              {{ db.collections.length ? '没有匹配的数据集合' : '暂无数据库集合' }}
            </div>
            <div
              v-for="item in filteredCollections"
              :key="item.name"
              class="group flex items-start gap-3 px-4 py-3 hover:bg-surface-sunken"
            >
              <input
                type="checkbox"
                class="mt-1 accent-[color:var(--c-primary)]"
                :checked="selectedCollections.has(item.name)"
                @change="toggleSelect(item.name)"
                @click.stop
              />
              <button type="button" @click="goCollection(item.name)" class="min-w-0 flex-1 text-left">
                <div class="flex min-w-0 items-center justify-between gap-2">
                  <div class="truncate font-bold text-[color:var(--c-text)]" :title="displayName(item.name)">{{ displayName(item.name) }}</div>
                  <span class="badge badge-info shrink-0 text-[11px]">{{ (item as { count?: number }).count || 0 }}</span>
                </div>
                <div class="mt-1 truncate text-xs text-[color:var(--c-text-subtle)]">
                  {{ (item as { time?: string }).time || '' }} · {{ ((item as { tags?: unknown[] }).tags || []).length }} 个标签
                </div>
              </button>
            </div>
          </div>
        </template>

        <!-- 影片列表 -->
        <template v-else-if="pageMode === 'movie-list'">
          <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 py-2 flex items-center justify-between gap-2 text-xs text-[color:var(--c-text-muted)]">
            <span>{{ movies.length }} 部影片</span>
            <div class="flex gap-1">
              <button @click="autoSelect(routeCategory!)" class="btn btn-sm btn-warning">★ 自动选择</button>
              <button @click="copyCollectionMagnets(routeCategory!)" class="btn btn-icon-sm btn-info text-xs" title="复制磁力">⧉</button>
              <button @click="deleteCollection(routeCategory!)" class="btn btn-icon-sm btn-danger" title="删除集合">
                <svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 15h10l1-15"></path>
                </svg>
              </button>
            </div>
          </div>
          <div class="min-h-0 flex-1 divide-y divide-[color:var(--c-border)] overflow-y-auto">
            <div v-if="!movies.length" class="empty-state">暂无影片数据</div>
            <div
              v-for="movie in movies"
              :key="movie.id"
              @click="goMovie(routeCategory!, movie.id)"
              class="flex cursor-pointer items-start gap-3 px-4 py-3 hover:bg-surface-sunken text-sm"
            >
              <div class="min-w-0 flex-1">
                <div class="flex items-center gap-2">
                  <span class="font-bold">{{ movie.code }}</span>
                  <span v-if="movie.magnet_health" :class="['badge text-[10px]',
                    movie.magnet_health === 'active' ? 'badge-success' :
                    movie.magnet_health === 'weak' ? 'badge-warning' :
                    movie.magnet_health === 'dead' ? 'badge-danger' : 'badge-neutral'
                  ]">{{ ({ active: '有效', weak: '弱', dead: '无效' } as Record<string, string>)[movie.magnet_health as string] || movie.magnet_health }}</span>
                </div>
                <div v-if="movie.title" class="mt-0.5 truncate text-xs text-[color:var(--c-text-subtle)]">{{ movie.title }}</div>
                <div v-if="movie.tags?.length" class="mt-1 flex flex-wrap gap-0.5">
                  <span v-for="tag in movie.tags.slice(0, 4)" :key="tag" class="px-1.5 py-0.5 rounded bg-neutral-soft text-neutral-text text-[11px]">{{ tag }}</span>
                </div>
              </div>
            </div>
          </div>
        </template>

        <!-- 磁力列表 -->
        <template v-else-if="pageMode === 'magnet-list'">
          <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 py-3">
            <div v-if="movies.find((m: Movie) => String(m.id) === routeMovieId)" class="text-sm">
              <div class="font-bold">{{ movies.find((m: Movie) => String(m.id) === routeMovieId)?.code }}</div>
              <div class="text-xs text-[color:var(--c-text-muted)]">{{ movies.find((m: Movie) => String(m.id) === routeMovieId)?.title }}</div>
            </div>
          </div>
          <div class="min-h-0 flex-1 overflow-y-auto p-4 space-y-2">
            <div v-if="!magnets.length" class="empty-state">暂无磁力数据</div>
            <div
              v-for="mag in magnets"
              :key="mag.id"
              :class="['rounded border p-3 text-sm cursor-pointer transition-colors',
                mag.is_selected ? 'border-[color:var(--c-success)] bg-success-soft' : 'border-[color:var(--c-border)] bg-surface hover:bg-surface-sunken'
              ]"
              @click="selectMagnet(routeMovieId!, mag.id)"
            >
              <div class="flex items-start gap-2">
                <span :class="['shrink-0 mt-0.5', magnetStatusMeta(mag).cls]" :title="magnetStatusMeta(mag).title">{{ magnetStatusMeta(mag).icon }}</span>
                <div class="min-w-0 flex-1">
                  <div class="truncate text-xs font-mono">{{ mag.url }}</div>
                  <div class="mt-1 flex flex-wrap gap-2 text-xs text-[color:var(--c-text-muted)]">
                    <span v-if="mag.size_mb">{{ formatGb(mag.size_mb) }}</span>
                    <span v-if="mag.is_hd" class="badge badge-info">HD</span>
                    <span v-if="mag.has_subtitle" class="badge badge-success">字幕</span>
                    <span v-if="mag.is_selected" class="badge badge-success">已选</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </template>

        <!-- 排行榜分类 -->
        <template v-else-if="pageMode === 'ranking-category'">
          <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 py-3">
            <div class="text-sm font-bold text-[color:var(--c-text)]">排行榜分类</div>
          </div>
          <div class="min-h-0 flex-1 overflow-y-auto p-4">
            <div class="grid gap-3 md:grid-cols-2">
              <button
                v-for="cat in RANKING_CATEGORIES"
                :key="cat.key"
                type="button"
                @click="goRankingPeriod(cat.key)"
                class="group flex min-h-[56px] items-center gap-3 rounded border border-[color:var(--c-border)] bg-surface px-4 py-3 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft"
              >
                <span class="shrink-0 text-base font-bold text-[color:var(--c-text)]">{{ cat.label }}</span>
                <span class="text-xs font-bold text-[color:var(--c-text-subtle)] group-hover:text-primary-text">{{ (cat as { subLabel?: string }).subLabel || '日榜 · 周榜 · 月榜' }}</span>
              </button>
            </div>
          </div>
        </template>

        <!-- 排行榜周期 -->
        <template v-else-if="pageMode === 'ranking-period'">
          <div class="min-h-0 flex-1 overflow-y-auto p-4">
            <template v-if="rankingCategoryMeta(routeCategory!)?.dynamicOptions">
              <div class="mb-3 flex items-center justify-between">
                <span class="text-sm font-bold">TOP250 分类</span>
                <button @click="db.loadTop250Options(true).catch(err => showToast(err.message || '刷新失败'))" class="btn btn-sm btn-soft">刷新分类</button>
              </div>
              <div v-if="!db.top250Options?.length" class="empty-state">加载中...</div>
              <div v-else class="grid gap-3 md:grid-cols-2">
                <button
                  v-for="opt in db.top250Options"
                  :key="opt.key"
                  type="button"
                  @click="goRankingMovieList(routeCategory!, opt.key)"
                  class="rounded border border-[color:var(--c-border)] bg-surface px-4 py-3 text-left text-sm font-bold hover:bg-primary-soft"
                >{{ opt.label }}</button>
              </div>
            </template>
            <template v-else>
              <div class="grid gap-3 md:grid-cols-2">
                <button
                  v-for="period in RANKING_PERIODS"
                  :key="period.key"
                  type="button"
                  @click="goRankingMovieList(routeCategory!, period.key)"
                  class="rounded border border-[color:var(--c-border)] bg-surface px-4 py-3 text-left text-sm font-bold hover:bg-primary-soft"
                >{{ period.label }}</button>
              </div>
            </template>
          </div>
        </template>

        <!-- 排行榜影片列表 -->
        <template v-else-if="pageMode === 'ranking-movie-list'">
          <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 py-2 flex items-center justify-end gap-1">
            <button @click="handleRankingUpdateTask(routeCategory!, routePeriod!)" class="btn btn-sm btn-info" title="更新排行榜">↻ 更新</button>
            <button @click="copyRankingMagnets(routeCategory!, routePeriod!)" class="btn btn-icon-sm btn-info text-xs" title="复制磁力">⧉</button>
            <a :href="db.getRankingDownloadUrl(routeCategory!, routePeriod!)" class="btn btn-icon-sm btn-success text-xs" title="下载 CSV">⇩</a>
          </div>
          <div class="min-h-0 flex-1 divide-y divide-[color:var(--c-border)] overflow-y-auto">
            <div v-if="!movies.length" class="empty-state">暂无排行榜数据</div>
            <div
              v-for="(movie, idx) in movies"
              :key="movie.id"
              @click="goRankingMagnet(routeCategory!, routePeriod!, movie.id)"
              class="flex cursor-pointer items-start gap-3 px-4 py-3 hover:bg-surface-sunken text-sm"
            >
              <span class="shrink-0 w-6 text-center text-xs font-bold text-[color:var(--c-text-muted)]">{{ idx + 1 }}</span>
              <div class="min-w-0 flex-1">
                <div class="flex items-center gap-2">
                  <span class="font-bold">{{ movie.code }}</span>
                </div>
                <div v-if="movie.title" class="truncate text-xs text-[color:var(--c-text-subtle)]">{{ movie.title }}</div>
              </div>
            </div>
          </div>
        </template>

        <!-- 排行榜磁力列表 -->
        <template v-else-if="pageMode === 'ranking-magnet-list'">
          <div class="min-h-0 flex-1 overflow-y-auto p-4 space-y-2">
            <div v-if="!magnets.length" class="empty-state">暂无磁力数据</div>
            <div
              v-for="mag in magnets"
              :key="mag.id"
              :class="['rounded border p-3 text-sm',
                mag.is_selected ? 'border-[color:var(--c-success)] bg-success-soft' : 'border-[color:var(--c-border)] bg-surface'
              ]"
            >
              <div class="flex items-start gap-2">
                <span :class="['shrink-0 mt-0.5', magnetStatusMeta(mag).cls]" :title="magnetStatusMeta(mag).title">{{ magnetStatusMeta(mag).icon }}</span>
                <div class="min-w-0 flex-1">
                  <div class="truncate text-xs font-mono">{{ mag.url }}</div>
                  <div class="mt-1 flex flex-wrap gap-2 text-xs text-[color:var(--c-text-muted)]">
                    <span v-if="mag.size_mb">{{ formatGb(mag.size_mb) }}</span>
                    <span v-if="mag.is_hd" class="badge badge-info">HD</span>
                    <span v-if="mag.has_subtitle" class="badge badge-success">字幕</span>
                    <span v-if="mag.is_selected" class="badge badge-success">已选</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </template>
      </div>
    </section>
  </section>
</template>