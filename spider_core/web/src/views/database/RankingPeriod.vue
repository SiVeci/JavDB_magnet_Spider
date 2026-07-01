<script setup lang="ts">
import { useRouter } from 'vue-router'
import { RANKING_CATEGORIES, RANKING_PERIODS, useDatabaseStore } from '@/stores/database'

defineProps<{
  categoryKey: string
  top250Error: string
}>()

const emit = defineEmits<{
  refreshTop250: []
}>()

const router = useRouter()
const db = useDatabaseStore()

function rankingCategoryMeta(key: string) {
  return RANKING_CATEGORIES.find((c) => c.key === key) || null
}

function goRankingMovieList(catKey: string, periodKey: string) {
  router.push(`/database/ranking/${encodeURIComponent(catKey)}/${encodeURIComponent(periodKey)}`)
}
</script>

<template>
  <template v-if="rankingCategoryMeta(categoryKey)?.dynamicOptions">
    <div class="shrink-0 border-b border-soft px-5 py-3">
      <div class="flex items-center justify-between gap-3">
        <div class="min-w-0">
          <div class="text-sm font-bold text-[color:var(--c-text)]">{{ rankingCategoryMeta(categoryKey)?.label }}</div>
          <div class="mt-1 truncate text-xs text-subtle">{{ db.top250Stale ? '使用本地缓存' : '动态分类' }} · {{ (db.top250Options || []).length }} 个选项</div>
        </div>
        <button type="button" class="btn btn-sm btn-info shrink-0 text-xs" @click="emit('refreshTop250')">刷新分类</button>
      </div>
    </div>
    <div class="min-h-0 flex-1 overflow-y-auto bg-surface-sunken p-4">
      <div v-if="top250Error" class="empty-state flex-1 flex-col gap-3 px-6 py-10">
        <div>{{ top250Error }}</div>
        <button type="button" class="btn btn-sm btn-info text-xs" @click="emit('refreshTop250')">刷新分类</button>
      </div>
      <div v-else-if="!(db.top250Options || []).length" class="empty-state flex-1 flex-col gap-3 px-6 py-10">
        <div>暂无 TOP250 分类，请点击刷新分类</div>
        <button type="button" class="btn btn-sm btn-info text-xs" @click="emit('refreshTop250')">刷新分类</button>
      </div>
      <div v-else class="grid gap-3 md:grid-cols-3">
        <button v-for="opt in db.top250Options" :key="opt.key" type="button" @click="goRankingMovieList(categoryKey, opt.key)" class="group flex min-h-[56px] items-center justify-between gap-3 rounded border border-[color:var(--c-border)] bg-surface px-4 py-3 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
          <span class="shrink-0 text-sm font-bold leading-none text-[color:var(--c-text)] md:text-base">{{ opt.label }}</span>
          <span class="min-w-0 truncate text-xs font-bold leading-none text-subtle group-hover:text-primary-text">影片列表</span>
        </button>
      </div>
    </div>
  </template>
  <template v-else>
    <div class="shrink-0 border-b border-soft px-5 py-3">
      <div class="text-sm font-bold text-[color:var(--c-text)]">{{ rankingCategoryMeta(categoryKey)?.label }}</div>
    </div>
    <div class="min-h-0 flex-1 overflow-y-auto bg-surface-sunken p-4">
      <div class="grid gap-3 md:grid-cols-3">
        <button v-for="period in RANKING_PERIODS" :key="period.key" type="button" @click="goRankingMovieList(categoryKey, period.key)" class="group flex min-h-[56px] items-center justify-between gap-3 rounded border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-primary-soft">
          <span class="shrink-0 text-sm font-bold text-[color:var(--c-text)] md:text-base">{{ period.label }}</span>
          <span class="min-w-0 truncate text-xs font-bold text-subtle group-hover:text-primary-text">影片列表</span>
        </button>
      </div>
    </div>
  </template>
</template>
