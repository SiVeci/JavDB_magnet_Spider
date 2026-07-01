<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'
import { apiDownloadBlob } from '@/api'
import { HEALTH_ITEMS, useDatabaseStore } from '@/stores/database'
import { useTasksStore } from '@/stores/tasks'
import { useToast } from '@/composables/useToast'
import { useClipboard } from '@/composables/useClipboard'
import { displayName } from '@/utils/format'
import { toErrMsg } from '@/utils/error'
import MagnetCheckButton from '@/components/MagnetCheckButton.vue'
import MovieListItem from '@/components/MovieListItem.vue'
import TagFilterDropdown from '@/components/TagFilterDropdown.vue'
import type { Movie } from '@/types'

const props = defineProps<{
  collectionName: string
}>()

const router = useRouter()
const db = useDatabaseStore()
const tasks = useTasksStore()
const { showToast } = useToast()
const { copyText } = useClipboard()
const openMenu = ref<string | null>(null)

const currentCollection = computed(() => db.getCollection(props.collectionName))
const currentMovieData = computed(() => db.collectionData(props.collectionName))
const filteredMovies = computed<Movie[]>(() => {
  const selected = db.getTagFilter(props.collectionName)
  const excluded = db.getExcludeFilter(props.collectionName)
  return (currentMovieData.value.movies || []).filter((movie) => movieMatchesTags(movie, selected, excluded))
})
const healthMap = computed(() => healthByMovieHealth(filteredMovies.value))

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

function goMovie(movieId: string | number) {
  router.push(`/database/actor/${encodeURIComponent(props.collectionName)}/${encodeURIComponent(String(movieId))}`)
}

async function copyCollectionMagnets() {
  try {
    const links = await db.getCollectionMagnetLinks(props.collectionName)
    if (!links.length) { showToast('暂无磁力链接可复制'); return }
    const ok = await copyText(links.join('\n'))
    showToast(ok ? `已复制 ${links.length} 条磁力链接` : '自动复制失败，请在弹窗中手动复制磁力链接')
  } catch (err: unknown) {
    showToast(toErrMsg(err, '复制失败'))
  }
}

async function downloadCsv() {
  try { await apiDownloadBlob(db.getCollectionDownloadUrl(props.collectionName), props.collectionName) }
  catch (err: unknown) { showToast(toErrMsg(err, '下载失败')) }
}

async function enqueueIncremental() {
  if (!confirm(`确定对「${displayName(props.collectionName)}」执行增量爬取吗？`)) return
  try {
    const msg = await db.enqueueCollectionIncremental(props.collectionName)
    showToast(msg)
    await tasks.refresh()
  } catch (err: unknown) {
    showToast(toErrMsg(err, '添加增量任务失败'))
  }
}

async function deleteCollection() {
  if (!confirm('确定删除 1 个数据库集合吗？')) return
  try {
    const msg = await db.deleteCollection(props.collectionName)
    showToast(msg)
    router.push('/database/actor')
  } catch (err: unknown) {
    showToast(toErrMsg(err, '删除失败'))
  }
}
</script>

<template>
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
      <MagnetCheckButton scope="collection" :target="collectionName" />
    </div>
  </div>
  <div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface-sunken px-4 pb-4 pt-3 text-sm text-muted">
    <div class="mb-3 flex shrink-0 flex-wrap items-center gap-2">
      <TagFilterDropdown
        v-model:open-menu="openMenu"
        :filter-key="collectionName"
        :available-tags="currentMovieData.available_tags"
        :filtered-count="filteredMovies.length"
        :total-count="currentMovieData.movies.length"
      />
      <div class="ml-auto flex shrink-0 items-center gap-1">
        <button type="button" title="复制集合磁力" aria-label="复制集合磁力" class="btn btn-icon-sm btn-info text-xs" @click="copyCollectionMagnets">⧉</button>
        <button type="button" title="下载 CSV" aria-label="下载 CSV" class="btn btn-icon-sm btn-success text-xs" @click="downloadCsv">⇩</button>
        <button v-if="currentCollection?.has_source_url" type="button" title="增量爬取此集合" aria-label="增量爬取此集合" class="btn btn-icon-sm btn-info text-xs" @click="enqueueIncremental">⟳</button>
        <button v-else type="button" disabled title="缺少原始 URL，无法快捷增量" aria-label="缺少原始 URL，无法快捷增量" class="btn btn-icon-sm btn-soft text-xs">⟳</button>
        <button type="button" title="删除集合" aria-label="删除集合" class="btn btn-icon-sm btn-danger" @click="deleteCollection">
          <svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M3 6h18"></path><path d="M8 6V4h8v2"></path><path d="M6 6l1 15h10l1-15"></path><path d="M10 11v6"></path><path d="M14 11v6"></path>
          </svg>
        </button>
      </div>
    </div>
    <div v-if="!filteredMovies.length" class="empty-state">暂无匹配影片记录</div>
    <div v-else class="min-h-0 flex-1 max-w-full divide-y divide-[color:var(--c-border)] overflow-y-auto rounded-lg border border-[color:var(--c-border)] bg-surface">
      <MovieListItem v-for="movie in filteredMovies" :key="movie.id" :movie="movie" @open="goMovie" />
    </div>
  </div>
</template>
