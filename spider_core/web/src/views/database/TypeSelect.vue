<script setup lang="ts">
import { useRouter } from 'vue-router'
import { RANKING_CATEGORIES, useDatabaseStore } from '@/stores/database'
import { useToast } from '@/composables/useToast'
import { toErrMsg } from '@/utils/error'

const router = useRouter()
const db = useDatabaseStore()
const { showToast } = useToast()

function goType(type: string) {
  router.push(`/database/${type}`)
}

async function autoSelect() {
  const names = db.collections.map((c) => c.name)
  if (!names.length) { showToast('暂无可自动选择的集合'); return }
  if (!confirm(`按当前已保存的评分规则重新计算全部候选并自动选择推荐磁力？
该操作会覆盖目标影片此前的人工选择。
作用范围：全部集合`)) return
  try {
    const msg = await db.autoSelectMagnets(names)
    showToast(msg)
    await db.loadCollections()
  } catch (err: unknown) {
    showToast(toErrMsg(err, '自动选择失败'))
  }
}
</script>

<template>
  <div class="shrink-0 border-b border-soft px-5 py-3">
    <div class="flex items-center justify-between gap-3">
      <div class="text-sm font-bold text-[color:var(--c-text)]">类型</div>
      <button type="button" title="按当前规则重新评分并自动选择磁力" aria-label="按当前规则重新评分并自动选择磁力" class="btn btn-sm btn-warning shrink-0" @click="autoSelect">★ 按规则重选</button>
    </div>
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
