<script setup lang="ts">
import { useToast } from '@/composables/useToast'
import { useClipboard } from '@/composables/useClipboard'
import { magnetStatusMeta, formatGb } from '@/composables/useMagnetMeta'
import type { Magnet } from '@/types'

/*
 * MagnetTable — 候选磁力 5 列表格（还原旧版 magnet-table.js）
 * 列：状态 / 文件名 / 分数 / 大小 / 操作
 */
const props = defineProps<{
  movieId: string | number
  magnets: Magnet[]
}>()
const emit = defineEmits<{ (e: 'select', magnetId: number): void }>()

const { showToast } = useToast()
const { copyText } = useClipboard()

async function copyLink(link: string) {
  const ok = await copyText(link)
  showToast(ok ? '已复制磁力链接' : '自动复制失败，请手动复制')
}
</script>

<template>
  <div v-if="!magnets.length" class="empty-state flex-1">暂无候选磁力</div>
  <div v-else class="min-h-0 flex-1 overflow-auto rounded-lg border border-[color:var(--c-border)]">
    <table class="w-full table-fixed text-xs">
      <caption class="sr-only">候选磁力列表</caption>
      <colgroup>
        <col class="w-14" /><col /><col class="w-10" /><col class="w-16" /><col class="w-16" />
      </colgroup>
      <thead class="sticky top-0 border-b border-[color:var(--c-border)] bg-surface-sunken text-[color:var(--c-text-muted)]">
        <tr>
          <th class="p-2 text-center whitespace-nowrap font-bold">状态</th>
          <th class="p-2 text-left font-bold">文件名</th>
          <th class="p-2 text-center font-bold">分数</th>
          <th class="p-2 text-center font-bold">大小</th>
          <th class="p-2 text-center font-bold">操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="mag in magnets"
          :key="mag.id"
          class="border-t border-[color:var(--c-border)] transition-colors"
          :class="mag.is_selected ? 'bg-success-soft' : 'hover:bg-surface-sunken'"
        >
          <!-- 状态 -->
          <td class="p-2 text-center align-middle" :class="mag.is_selected ? 'border-l-2 border-[color:var(--c-success)]' : ''">
            <div>
              <span :class="magnetStatusMeta(mag).text" :title="magnetStatusMeta(mag).title">{{ magnetStatusMeta(mag).icon }}</span>
            </div>
            <div class="mt-1 text-[10px] leading-none text-[color:var(--c-text-subtle)]">
              {{ mag.checked_at ? `${mag.seeders ?? 0}/${mag.leechers ?? 0}` : '-/-' }}
            </div>
          </td>
          <!-- 文件名 -->
          <td
            class="min-w-0 p-2"
            :class="mag.is_selected ? '' : 'cursor-pointer'"
            :title="mag.link"
            @click="mag.is_selected ? null : emit('select', mag.id)"
          >
            <div class="truncate">
              <span v-if="mag.is_selected" class="mr-1 text-success-text">✓</span>{{ mag.name }}
            </div>
            <div class="mt-1 inline-flex max-w-full rounded bg-neutral-soft px-1.5 py-0.5 text-[10px] leading-none text-[color:var(--c-text-muted)]">
              {{ mag.magnet_date || '-' }}
            </div>
          </td>
          <!-- 分数 -->
          <td class="p-2 text-center align-middle">{{ mag.priority_score }}</td>
          <!-- 大小 -->
          <td class="p-2 text-center align-middle whitespace-nowrap">{{ formatGb(mag.size_mb) }}</td>
          <!-- 操作 -->
          <td class="p-2 text-center align-middle">
            <div class="flex justify-center gap-1">
              <button type="button" title="复制磁力链接" aria-label="复制磁力链接" class="btn btn-icon-sm btn-info" @click="copyLink(mag.link)">
                <span class="text-sm leading-none">⧉</span>
              </button>
            </div>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
