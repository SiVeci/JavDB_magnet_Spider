<script setup lang="ts">
import { useRouter } from 'vue-router'
import { RANKING_CATEGORIES } from '@/stores/database'

const router = useRouter()

function goRankingPeriod(catKey: string) {
  router.push(`/database/ranking/${encodeURIComponent(catKey)}`)
}
</script>

<template>
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
