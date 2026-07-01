<script setup lang="ts">
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useDatabaseStore, RANKING_CATEGORIES, RANKING_PERIODS } from '@/stores/database'
import { useTasksStore } from '@/stores/tasks'
import { useToast } from '@/composables/useToast'
import { fitMovieTags } from '@/composables/useMovieTags'
import TypeSelect from '@/views/database/TypeSelect.vue'
import CollectionList from '@/views/database/CollectionList.vue'
import MovieList from '@/views/database/MovieList.vue'
import MagnetList from '@/views/database/MagnetList.vue'
import RankingCategory from '@/views/database/RankingCategory.vue'
import RankingPeriod from '@/views/database/RankingPeriod.vue'
import RankingMovieList from '@/views/database/RankingMovieList.vue'
import RankingMagnetList from '@/views/database/RankingMagnetList.vue'
import { displayName } from '@/utils/format'
import { toErrMsg } from '@/utils/error'
import type { Movie, Magnet, MagnetCheckJob } from '@/types'

const route = useRoute()
const router = useRouter()
const db = useDatabaseStore()
const tasks = useTasksStore()
const { showToast } = useToast()

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
const magnets = ref<Magnet[]>([])
const top250Error = ref('')
const databaseCardEl = ref<HTMLElement | null>(null)
const databaseCardHeight = ref('auto')
let databaseLayoutFrame: number | null = null

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
function goCollectionList() { router.push('/database/actor') }
function goCollection(name: string) { router.push(`/database/actor/${encodeURIComponent(name)}`) }
function goRankingCategory() { router.push('/database/ranking') }
function goRankingPeriod(catKey: string) { router.push(`/database/ranking/${encodeURIComponent(catKey)}`) }
function goRankingMovieList(catKey: string, periodKey: string) { router.push(`/database/ranking/${encodeURIComponent(catKey)}/${encodeURIComponent(periodKey)}`) }

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

watch(() => route.fullPath, () => { loadData() })

onMounted(async () => {
  if (!db.collections.length) await db.loadCollections()
  // 注册磁力检测轮询回调：tick 刷新当前磁力行，done 重载目标
  db.registerCheckCallbacks(onMagnetCheckTick, onMagnetCheckDone)
  await db.restoreMagnetCheckJob()
  await loadData()
  window.addEventListener('resize', onResize)
})

onBeforeUnmount(() => {
  db.registerCheckCallbacks(null, null)
  window.removeEventListener('resize', onResize)
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

      <!-- 内容区 -->
      <div class="flex min-h-0 flex-1 flex-col overflow-hidden">
        <!-- 加载中 -->
        <div v-if="loading" class="empty-state flex-1 flex-col gap-3" role="status" aria-busy="true">
          <span class="spinner-ring" aria-hidden="true"></span>
          <span>加载中...</span>
        </div>

        <TypeSelect v-else-if="pageMode === 'type-select'" />

        <CollectionList v-else-if="pageMode === 'collection-list'" />
        <MovieList v-else-if="pageMode === 'movie-list'" :collection-name="routeCategory!" />

        <MagnetList
          v-else-if="pageMode === 'magnet-list'"
          :movie-id="routeMovieId!"
          :movie="currentMovie"
          :magnets="magnets"
          @select="selectMagnet"
        />
        <RankingCategory v-else-if="pageMode === 'ranking-category'" />

        <RankingPeriod
          v-else-if="pageMode === 'ranking-period'"
          :category-key="routeCategory!"
          :top250-error="top250Error"
          @refresh-top250="refreshTop250"
        />

        <RankingMovieList
          v-else-if="pageMode === 'ranking-movie-list'"
          :category-key="routeCategory!"
          :period-key="routePeriod!"
        />

        <RankingMagnetList
          v-else-if="pageMode === 'ranking-magnet-list'"
          :movie-id="routeMovieId!"
          :movie="currentMovie"
          :magnets="magnets"
          @select="selectMagnet"
        />
      </div>
    </section>
  </section>
</template>
