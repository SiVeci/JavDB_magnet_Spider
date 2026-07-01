<script setup lang="ts">
import { useRouter } from 'vue-router'
import { RANKING_CATEGORIES, useDatabaseStore } from '@/stores/database'

const router = useRouter()
const db = useDatabaseStore()

function goType(type: string) {
  router.push(`/database/${type}`)
}
</script>

<template>
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
