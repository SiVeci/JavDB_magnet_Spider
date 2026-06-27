<script setup lang="ts">
import { ref, computed } from 'vue'
import { useDatabaseStore } from '@/stores/database'
import { useToast } from '@/composables/useToast'
import type { MagnetCheckJob } from '@/types'

/*
 * MagnetCheckButton — 磁力验活拆分按钮（还原旧版 magnets.js）
 * scope: movie | collection | all | ranking
 * 主按钮：空闲=雷达图标启动；运行中=进度 completed/total（movie 为 spinner）
 * 副按钮：空闲=▼/▲ 下拉「check failed」；运行中=红色终止按钮
 */
const props = defineProps<{
  scope: 'movie' | 'collection' | 'all' | 'ranking'
  target: string | number
  compact?: boolean
}>()

const db = useDatabaseStore()
const { showToast } = useToast()
const menuOpen = ref(false)

// 尺寸档：mini(movie) / std(all) / toolbar(collection,ranking)
const SIZE = {
  mini: { primary: 'h-5 w-6 text-[11px] leading-none', toggle: 'h-5 w-5 text-[10px] leading-none', spinner: 'h-2.5 w-2.5' },
  std: { primary: 'h-7 w-11 text-xs shadow-sm', toggle: 'h-7 w-6 text-xs shadow-sm', spinner: 'h-3 w-3' },
  toolbar: { primary: 'h-7 w-7 text-[11px] leading-none', toggle: 'h-7 w-6 text-[10px] leading-none', spinner: 'h-3 w-3' },
}
const sizeKey = computed<'mini' | 'std' | 'toolbar'>(() =>
  props.scope === 'movie' ? 'mini' : props.compact ? 'toolbar' : props.scope === 'all' ? 'std' : 'toolbar'
)
const size = computed(() => SIZE[sizeKey.value])

const job = computed<MagnetCheckJob | null>(() => db.activeMagnetCheckJob)
const hasRunningJob = computed(() => !!(job.value && job.value.running))
const isRunningTarget = computed(() =>
  !!(hasRunningJob.value && job.value!.scope === props.scope && String(job.value!.target) === String(props.target))
)
const isCancelling = computed(() => !!(isRunningTarget.value && job.value!.cancelled))
const progress = computed(() =>
  isRunningTarget.value ? `${Number(job.value!.completed || 0)}/${Number(job.value!.total || 0)}` : 'check'
)

const primaryDisabled = computed(() => (hasRunningJob.value && !isRunningTarget.value) || isCancelling.value)
const toggleDisabled = computed(() => hasRunningJob.value && !isRunningTarget.value)

const primaryTitle = computed(() => {
  if (!isRunningTarget.value) return '检测磁力'
  if (props.scope === 'movie') return isCancelling.value ? '正在终止检测' : '检测中'
  return '检测进度'
})

async function start(failedOnly = false) {
  menuOpen.value = false
  try {
    await db.startMagnetCheck(props.scope, props.target, failedOnly)
  } catch (err: unknown) {
    const e = err as { running?: boolean; data?: MagnetCheckJob; message?: string }
    if (e.running) {
      showToast(e.message || '磁力检测任务正在运行')
      if (e.data) db.watchMagnetCheckJob(e.data)
    } else {
      showToast(e.message || '检测启动失败')
    }
  }
}

async function cancel() {
  if (!job.value) return
  if (!confirm('确定终止当前磁力检测吗？已完成的检测结果会保留。')) return
  try {
    await db.cancelMagnetCheck(String(job.value.job_id))
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '终止检测失败')
  }
}

function toggleMenu() {
  if (toggleDisabled.value) return
  menuOpen.value = !menuOpen.value
}
</script>

<template>
  <div class="relative shrink-0" @click.stop>
    <div class="inline-flex">
      <!-- 主按钮 -->
      <button
        type="button"
        :disabled="primaryDisabled"
        :title="primaryTitle"
        :aria-label="primaryTitle"
        class="btn-split-primary"
        :class="size.primary"
        @click="start(false)"
      >
        <span class="inline-flex items-center justify-center gap-1">
          <!-- 运行中：movie=spinner，其它=进度文本 -->
          <template v-if="isRunningTarget">
            <span
              v-if="scope === 'movie'"
              class="inline-block animate-spin rounded-full border-2 border-[color:var(--c-border)] border-t-[color:var(--c-success-text)]"
              :class="size.spinner"
            ></span>
            <span v-else>{{ progress }}</span>
          </template>
          <!-- 空闲：雷达图标 -->
          <svg v-else aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="12" r="9"></circle>
            <path d="M12 12l6-4"></path>
            <path d="M12 3v2"></path>
            <path d="M12 19v2"></path>
            <path d="M3 12h2"></path>
            <path d="M19 12h2"></path>
            <path d="M8.5 8.5a5 5 0 0 1 7 0"></path>
          </svg>
        </span>
      </button>

      <!-- 副按钮：运行中=终止，空闲=下拉开关 -->
      <button
        v-if="isRunningTarget"
        type="button"
        :disabled="isCancelling"
        :title="isCancelling ? '正在终止检测' : '终止检测'"
        :aria-label="isCancelling ? '正在终止检测' : '终止检测'"
        class="btn-split-stop"
        :class="size.toggle"
        @click="cancel"
      >
        <span
          v-if="isCancelling"
          class="inline-block animate-spin rounded-full border-2 border-[color:var(--c-border)] border-t-[color:var(--c-danger-text)]"
          :class="size.spinner"
        ></span>
        <svg v-else aria-hidden="true" viewBox="0 0 24 24" class="h-3 w-3" fill="currentColor">
          <rect x="6" y="6" width="12" height="12" rx="1.5"></rect>
        </svg>
      </button>
      <button
        v-else
        type="button"
        :disabled="toggleDisabled"
        title="更多检测选项"
        aria-label="更多检测选项"
        class="btn-split-toggle"
        :class="size.toggle"
        @click="toggleMenu"
      >{{ menuOpen ? '▲' : '▼' }}</button>
    </div>

    <!-- check failed 下拉 -->
    <div
      v-if="menuOpen && !hasRunningJob"
      class="menu right-0 w-28 text-xs"
      @click.stop
    >
      <button type="button" class="menu-item font-bold text-[color:var(--c-neutral-text)]" @click="start(true)">check failed</button>
    </div>
  </div>
</template>
