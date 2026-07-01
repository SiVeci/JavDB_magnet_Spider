<script setup lang="ts">
import type { Movie } from '@/types'

defineProps<{
  movie: Movie
  highlightOnHover?: boolean
}>()

const emit = defineEmits<{
  open: [movieId: number]
}>()
</script>

<template>
  <div class="p-3">
    <button
      type="button"
      class="block w-full min-w-0 text-left"
      :class="highlightOnHover ? 'transition-colors hover:text-primary-text' : ''"
      @click="emit('open', movie.id)"
    >
      <div class="truncate font-bold" :title="`${movie.code} ${movie.title || ''}`"><span>{{ movie.code }}</span> <span class="font-normal text-muted">{{ movie.title || '' }}</span></div>
      <div class="mt-1 flex min-w-0 items-center gap-2 text-xs text-muted">
        <span class="badge badge-info shrink-0 whitespace-nowrap">候选 {{ movie.candidate_count || 0 }}</span>
        <span class="min-w-0 truncate" :title="movie.best_magnet_name || '未选中磁力'">{{ movie.best_magnet_name || '未选中磁力' }}</span>
      </div>
      <div v-if="(movie.tags || []).length" class="movie-tags mt-2 flex max-w-full flex-nowrap gap-0.5 overflow-hidden" :title="(movie.tags || []).join(', ')">
        <span v-for="tag in movie.tags" :key="tag" data-role="tag" class="shrink-0 max-w-[104px] truncate px-1.5 py-0.5 rounded bg-neutral-soft text-neutral-text text-[10px]">{{ tag }}</span>
        <span data-role="more" class="badge badge-info hidden shrink-0 text-[10px]" style="display:none">+0</span>
      </div>
    </button>
  </div>
</template>
