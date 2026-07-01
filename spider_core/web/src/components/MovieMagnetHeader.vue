<script setup lang="ts">
import MagnetCheckButton from '@/components/MagnetCheckButton.vue'
import type { Movie } from '@/types'

defineProps<{
  movie?: Movie
}>()
</script>

<template>
  <div class="shrink-0 border-b border-soft px-5 pb-4 pt-2">
    <div class="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
      <div class="min-w-0">
        <div class="truncate text-xs text-muted" :title="movie?.title || movie?.code || ''">{{ movie?.title || movie?.code || '' }}</div>
        <div class="mt-1 flex min-w-0 items-center gap-2 text-xs text-muted">
          <MagnetCheckButton v-if="movie" scope="movie" :target="movie.id" />
          <div class="min-w-0 truncate" :title="movie?.best_magnet_name || '未选中磁力'">{{ movie?.best_magnet_name || '未选中磁力' }}</div>
        </div>
        <div v-if="(movie?.tags || []).length" class="movie-tags mt-2 flex max-w-full flex-nowrap gap-0.5 overflow-hidden" :title="(movie?.tags || []).join(', ')">
          <span v-for="tag in movie?.tags" :key="tag" data-role="tag" class="shrink-0 max-w-[104px] truncate px-1.5 py-0.5 rounded bg-neutral-soft text-neutral-text text-[10px]">{{ tag }}</span>
          <span data-role="more" class="badge badge-info hidden shrink-0 text-[10px]" style="display:none">+0</span>
        </div>
      </div>
    </div>
  </div>
</template>
