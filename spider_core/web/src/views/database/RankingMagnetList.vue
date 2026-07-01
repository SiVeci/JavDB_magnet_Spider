<script setup lang="ts">
import MovieMagnetHeader from '@/components/MovieMagnetHeader.vue'
import MagnetTable from '@/components/MagnetTable.vue'
import type { Magnet, Movie } from '@/types'

defineProps<{
  movieId: string
  movie?: Movie
  magnets: Magnet[]
}>()

const emit = defineEmits<{
  select: [magnetId: number]
}>()
</script>

<template>
  <MovieMagnetHeader :movie="movie" />
  <div class="flex min-h-0 flex-1 flex-col overflow-hidden bg-surface-sunken p-4">
    <MagnetTable :movie-id="movieId" :magnets="magnets" @select="emit('select', $event)" />
  </div>
</template>
