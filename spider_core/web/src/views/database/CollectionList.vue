<script setup lang="ts">
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'
import { useDatabaseStore } from '@/stores/database'
import { useToast } from '@/composables/useToast'
import { displayName } from '@/utils/format'
import { toErrMsg } from '@/utils/error'
import MagnetCheckButton from '@/components/MagnetCheckButton.vue'
import type { Collection } from '@/types'

const router = useRouter()
const db = useDatabaseStore()
const { showToast } = useToast()

const searchQuery = ref('')
const selectedCollections = ref<Set<string>>(new Set())

const filteredCollections = computed<Collection[]>(() => {
  const q = searchQuery.value.trim().toLowerCase()
  if (!q) return db.collections
  return db.collections.filter((c: Collection) =>
    displayName(c.name).toLowerCase().includes(q) || String(c.name).toLowerCase().includes(q)
  )
})

const allSelected = computed(() =>
  filteredCollections.value.length > 0 && filteredCollections.value.every((c) => selectedCollections.value.has(c.name))
)

function goCollection(name: string) {
  router.push(`/database/actor/${encodeURIComponent(name)}`)
}

function toggleSelect(name: string) {
  const s = new Set(selectedCollections.value)
  if (s.has(name)) s.delete(name)
  else s.add(name)
  selectedCollections.value = s
}

function toggleSelectAll(checked: boolean) {
  selectedCollections.value = checked ? new Set(filteredCollections.value.map((c) => c.name)) : new Set()
}

async function autoSelect() {
  const names = selectedCollections.value.size ? Array.from(selectedCollections.value) : db.collections.map((c) => c.name)
  if (!names.length) { showToast('暂无可自动选择的集合'); return }
  const scopeText = selectedCollections.value.size ? `${selectedCollections.value.size} 个已选集合` : '全部集合'
  if (!confirm(`按当前已保存的评分规则重新计算全部候选并自动选择推荐磁力？
该操作会覆盖目标影片此前的人工选择。
作用范围：${scopeText}`)) return
  try {
    const msg = await db.autoSelectMagnets(names)
    showToast(msg)
    await db.loadCollections()
  } catch (err: unknown) {
    showToast(toErrMsg(err, '自动选择失败'))
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
</script>

<template>
  <div class="shrink-0 border-b border-soft px-5 pb-2 pt-2">
    <div class="flex items-center justify-between gap-1">
      <div class="shrink-0 whitespace-nowrap text-[11px] text-muted">{{ db.collections.length }} 个集合 · {{ db.totalMovies() }} 部影片</div>
      <div class="flex shrink-0 items-center gap-1">
        <button type="button" title="按当前规则重新评分并自动选择磁力" aria-label="按当前规则重新评分并自动选择磁力" class="btn btn-sm btn-warning" @click="autoSelect">★ 自动选择</button>
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
