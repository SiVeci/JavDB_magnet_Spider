<script setup lang="ts">
import { useDatabaseStore } from '@/stores/database'

const props = defineProps<{
  filterKey: string
  availableTags: string[]
  filteredCount: number
  totalCount: number
  openMenu: string | null
}>()

const emit = defineEmits<{
  'update:openMenu': [value: string | null]
}>()

const db = useDatabaseStore()

function menuId(kind: string) {
  return `${kind}:${props.filterKey}`
}
function toggleMenu(kind: string) {
  const id = menuId(kind)
  emit('update:openMenu', props.openMenu === id ? null : id)
}
function isMenuOpen(kind: string) {
  return props.openMenu === menuId(kind)
}
</script>

<template>
  <div class="relative min-w-0">
    <button type="button" class="flex h-7 min-w-[104px] items-center justify-between gap-2 rounded border border-[color:var(--c-border)] bg-surface px-2 text-left text-xs font-bold text-[color:var(--c-neutral-text)] transition-colors hover:bg-surface-sunken" @click.stop="toggleMenu('tag')">
      <span class="min-w-0 truncate">筛选: {{ filteredCount }}/{{ totalCount }}</span>
      <span class="shrink-0">{{ isMenuOpen('tag') ? '▲' : '▼' }}</span>
    </button>
    <div v-if="isMenuOpen('tag')" class="menu w-64 max-h-72 overflow-y-auto" @click.stop>
      <label class="menu-item">
        <input type="checkbox" class="accent-[color:var(--c-primary)]" :checked="db.getTagFilter(filterKey).length === 0" @change="db.toggleTag(filterKey, 'all')" />
        <span class="truncate">全部</span>
      </label>
      <label v-for="tag in availableTags" :key="tag" class="menu-item">
        <input type="checkbox" class="accent-[color:var(--c-primary)]" :checked="db.getTagFilter(filterKey).includes(tag)" @change="db.toggleTag(filterKey, tag)" />
        <span class="truncate" :title="tag">{{ tag }}</span>
      </label>
    </div>
  </div>
  <div class="relative shrink-0">
    <button
      type="button"
      class="flex h-7 min-w-[68px] items-center justify-between gap-1 rounded border px-2 text-left text-xs font-bold transition-colors"
      :class="db.getExcludeFilter(filterKey).length ? 'border-[color:var(--c-danger)] bg-danger-soft text-danger-text' : 'border-[color:var(--c-border)] bg-surface text-muted hover:bg-surface-sunken'"
      @click.stop="toggleMenu('exclude')"
    >
      <span class="min-w-0 truncate">{{ db.getExcludeFilter(filterKey).length ? `排除: ${db.getExcludeFilter(filterKey).length}个` : '排除' }}</span>
      <span class="shrink-0">{{ isMenuOpen('exclude') ? '▲' : '▼' }}</span>
    </button>
    <div v-if="isMenuOpen('exclude')" class="menu w-64 max-h-72 overflow-y-auto" @click.stop>
      <label class="menu-item text-danger-text font-bold">
        <input type="checkbox" @change="db.clearExclude(filterKey)" />
        <span>清除排除</span>
      </label>
      <label v-for="tag in availableTags" :key="tag" class="menu-item hover:bg-danger-soft">
        <input type="checkbox" class="accent-[color:var(--c-danger)]" :checked="db.getExcludeFilter(filterKey).includes(tag)" @change="db.toggleExclude(filterKey, tag)" />
        <span class="truncate" :class="db.getExcludeFilter(filterKey).includes(tag) ? 'text-danger-text font-bold' : ''" :title="tag">{{ tag }}</span>
      </label>
    </div>
  </div>
</template>
