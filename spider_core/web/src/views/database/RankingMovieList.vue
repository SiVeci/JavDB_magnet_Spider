<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'
import { apiDownloadBlob } from '@/api'
import { HEALTH_ITEMS, RANKING_CATEGORIES, RANKING_PERIODS, useDatabaseStore } from '@/stores/database'
import { useTasksStore } from '@/stores/tasks'
import { useToast } from '@/composables/useToast'
import { useClipboard } from '@/composables/useClipboard'
import { toErrMsg } from '@/utils/error'
import MagnetCheckButton from '@/components/MagnetCheckButton.vue'
import MovieListItem from '@/components/MovieListItem.vue'
import TagFilterDropdown from '@/components/TagFilterDropdown.vue'
import type { Movie } from '@/types'

const props = defineProps<{
  categoryKey: string
  periodKey: string
}>()

const router = useRouter()
const db = useDatabaseStore()
const tasks = useTasksStore()
const { showToast } = useToast()
const { copyText } = useClipboard()
const openMenu = ref<string | null>(null)

const currentMovieData = computed(() => db.rankingData(props.categoryKey, props.periodKey))
const filterKey = computed(() => db.rankingFilterKey(props.categoryKey, props.periodKey))
const availableTags = computed(() => {
  if (currentMovieData.value.available_tags.length) return currentMovieData.value.available_tags
  return tagsFromMovies(currentMovieData.value.movies || [])
})
const filteredMovies = computed<Movie[]>(() => {
  const selected = db.getTagFilter(filterKey.value)
  const excluded = db.getExcludeFilter(filterKey.value)
  return (currentMovieData.value.movies || []).filter((movie) => movieMatchesTags(movie, selected, excluded))
})
const healthMap = computed(() => healthByMovieHealth(filteredMovies.value))

function tagsFromMovies(movies: Movie[]): string[] {
  const seen = new Set<string>()
  const tags: string[] = []
  for (const movie of movies) {
    for (const tag of movie.tags || []) {
      if (seen.has(tag)) continue
      seen.add(tag)
      tags.push(tag)
    }
  }
  return tags
}

function rankingCategoryMeta(key: string) {
  return RANKING_CATEGORIES.find((c) => c.key === key) || null
}

function rankingPeriodMeta(key: string) {
  return RANKING_PERIODS.find((p) => p.key === key) || null
}

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

function movieMatchesTags(movie: Movie, selected: string[], excluded: string[]): boolean {
  const movieTags = new Set(movie.tags || [])
  if (excluded.length && excluded.some((t) => movieTags.has(t))) return false
  if (!selected.length) return true
  return selected.every((t) => movieTags.has(t))
}

function healthByMovieHealth(movies: Movie[]) {
  const counts: Record<string, number> = { active: 0, weak: 0, dead: 0, failed: 0 }
  for (const movie of movies) {
    if (movie.magnet_health && counts[movie.magnet_health] !== undefined) counts[movie.magnet_health] += 1
  }
  return counts
}

function healthValue(itemKey: string): string {
  const map: Record<string, string> = { active_count: 'active', weak_count: 'weak', dead_count: 'dead', failed_count: 'failed' }
  const value = healthMap.value[map[itemKey]] || 0
  return value ? String(value) : '-'
}

function goRankingMagnet(movieId: string | number) {
  router.push(`/database/ranking/${encodeURIComponent(props.categoryKey)}/${encodeURIComponent(props.periodKey)}/${encodeURIComponent(String(movieId))}`)
}

async function copyRankingMagnets() {
  try {
    const links = await db.getRankingMagnetLinks(props.categoryKey, props.periodKey)
    if (!links.length) { showToast('暂无磁力链接可复制'); return }
    const ok = await copyText(links.join('\n'))
    showToast(ok ? `已复制 ${links.length} 条磁力链接` : '自动复制失败，请在弹窗中手动复制磁力链接')
  } catch (err: unknown) {
    showToast(toErrMsg(err, '复制失败'))
  }
}

async function downloadRankingCsv() {
  try { await apiDownloadBlob(db.getRankingDownloadUrl(props.categoryKey, props.periodKey), `ranking_${props.categoryKey}_${props.periodKey}.csv`) }
  catch (err: unknown) { showToast(toErrMsg(err, '下载失败')) }
}

async function updateRankingList() {
  try {
    const res = await db.createRankingUpdateTask(props.categoryKey, props.periodKey)
    if (res.code !== 200) { showToast(res.msg || '添加更新任务失败'); return }
    showToast(res.msg || '榜单更新任务已加入队列')
    await tasks.refresh()
  } catch (err: unknown) {
    showToast(toErrMsg(err, '添加更新任务失败'))
  }
}

async function clearRankingList() {
  if (!confirm('确定清空当前榜单吗？')) return
  try {
    const msg = await db.clearRankingList(props.categoryKey, props.periodKey)
    showToast(msg)
    await db.ensureRankingMovies(props.categoryKey, props.periodKey, true)
  } catch (err: unknown) {
    showToast(toErrMsg(err, '清空失败'))
  }
}
</script>

<template>
  <div class="shrink-0 border-b border-soft px-5 pb-2 pt-2">
    <div class="flex min-w-0 flex-wrap items-center justify-between gap-2 text-xs text-muted">
      <div class="flex min-w-0 flex-wrap items-center gap-2">
        <span class="min-w-0 truncate">{{ rankingCategoryMeta(categoryKey)?.label }} · {{ rankingPeriodForCategory(categoryKey, periodKey)?.label || periodKey }} · {{ Number(currentMovieData.total_count || currentMovieData.movies.length) }} 部影片</span>
        <div class="flex shrink-0 items-center gap-1" aria-label="磁力检测影片统计">
          <span v-for="h in HEALTH_ITEMS" :key="h.key" :title="h.title" class="badge min-w-[4ch] px-1 text-[10px]" :class="h.badge">{{ healthValue(h.key) }}</span>
        </div>
      </div>
      <MagnetCheckButton scope="ranking" :target="`${categoryKey}:${periodKey}`" />
    </div>
  </div>
  <div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface-sunken px-4 pb-4 pt-3 text-sm text-muted">
    <div class="mb-3 flex shrink-0 flex-wrap items-center gap-2">
      <TagFilterDropdown
        v-model:open-menu="openMenu"
        :filter-key="filterKey"
        :available-tags="availableTags"
        :filtered-count="filteredMovies.length"
        :total-count="Number(currentMovieData.total_count || currentMovieData.movies.length)"
      />
      <div class="ml-auto flex shrink-0 items-center gap-1">
        <button type="button" title="复制榜单磁力" aria-label="复制榜单磁力" class="btn btn-icon-sm btn-info text-xs" @click="copyRankingMagnets">⧉</button>
        <button type="button" title="下载榜单 CSV" aria-label="下载榜单 CSV" class="btn btn-icon-sm btn-success text-xs" @click="downloadRankingCsv">⇩</button>
        <button type="button" title="更新榜单" aria-label="更新榜单" class="btn btn-icon-sm btn-info text-xs" @click="updateRankingList">⟳</button>
        <button type="button" title="清空榜单" aria-label="清空榜单" class="btn btn-icon-sm btn-danger" @click="clearRankingList">
          <svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 15h10l1-15"></path><path d="M10 11v6"></path><path d="M14 11v6"></path>
          </svg>
        </button>
      </div>
    </div>
    <div v-if="!filteredMovies.length" class="min-h-0 flex-1 max-w-full overflow-y-auto rounded-lg border border-[color:var(--c-border)] bg-surface">
      <div class="empty-state px-6 py-10">暂无榜单影片</div>
    </div>
    <div v-else class="min-h-0 flex-1 max-w-full divide-y divide-[color:var(--c-border)] overflow-y-auto rounded-lg border border-[color:var(--c-border)] bg-surface">
      <MovieListItem v-for="movie in filteredMovies" :key="movie.id" :movie="movie" highlight-on-hover @open="goRankingMagnet" />
    </div>
  </div>
</template>
