<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { useActorsStore } from '@/stores/actors'
import { useTasksStore } from '@/stores/tasks'
import { useToast } from '@/composables/useToast'
import type { Actor } from '@/types'

const router = useRouter()
const actorsStore = useActorsStore()
const tasksStore = useTasksStore()
const { showToast } = useToast()

const refreshing = ref(false)

const refreshInfo = computed(() => {
  const actors = actorsStore.filtered
  if (!actorsStore.loaded) return '尚未刷新'
  if (!actors.length) return '该分类暂无收藏演员'
  const latest = actors.reduce((m: number, a: Actor) => Math.max(m, a.refreshed_at || 0), 0)
  return latest ? new Date(latest * 1000).toLocaleString() : '—'
})

const failedBannerMsg = ref('')

async function handleRefresh() {
  refreshing.value = true
  failedBannerMsg.value = ''
  try {
    const { failed } = await actorsStore.refresh(actorsStore.category)
    if (failed?.length) {
      failedBannerMsg.value = `刷新失败的分类：${failed.map((f: { label?: string; category: string; msg?: string }) => `${f.label || f.category}（${f.msg || ''}）`).join('；')}`
      showToast(`部分分类刷新失败：${failed.map((f: { label?: string; category: string; msg?: string }) => f.label || f.category).join('、')}`)
    } else {
      showToast('刷新完成')
    }
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '刷新失败')
  } finally {
    refreshing.value = false
  }
}

async function handleAddTask(actorId: string, crawlMode = '') {
  try {
    const res = await actorsStore.addTask(actorId, crawlMode)
    if (res.code === 409 && res.needs_mode) {
      const useIncremental = confirm(`检测到已有数据库集合：${res.filename || ''}\n点击"确定"使用增量，点击"取消"使用覆盖。`)
      return handleAddTask(actorId, useIncremental ? 'incremental' : 'overwrite')
    }
    if (res.code !== 200) { showToast(res.msg || '添加任务失败'); return }
    showToast(res.msg || '任务已加入队列')
    await tasksStore.refresh()
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '添加任务失败')
  }
}

function goToCollection(filename: string) {
  router.push(`/database/actor/${encodeURIComponent(filename)}`)
}

onMounted(() => {
  if (!actorsStore.loaded) actorsStore.load()
})
</script>

<template>
  <section class="space-y-4">
    <section class="card flex h-[calc(100dvh-190px)] min-h-0 min-w-0 flex-col overflow-hidden">
      <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 py-3 text-sm font-semibold">收藏演员</div>
      <div class="shrink-0 border-b border-[color:var(--c-border-soft)] px-5 pb-2 pt-2 space-y-2">
        <div class="flex min-w-0 items-center gap-2">
          <div class="w-[130px] shrink-0">
            <label class="sr-only">收藏演员分类</label>
            <select
              :value="actorsStore.category"
              @change="actorsStore.selectCategory(($event.target as HTMLSelectElement).value)"
              class="h-9 w-full rounded border border-[color:var(--c-border)] bg-surface px-3 text-sm font-semibold text-[color:var(--c-text)] focus:border-primary focus:outline-none focus:ring-2 focus:ring-[color:var(--c-primary-ring)]"
            >
              <option
                v-for="cat in actorsStore.categories"
                :key="cat.key"
                :value="cat.key"
              >{{ cat.label }} {{ cat.key === 'all' ? actorsStore.data.actors.length : actorsStore.data.actors.filter((a: { category: string }) => a.category === cat.key).length }}</option>
            </select>
          </div>
          <button
            @click="handleRefresh"
            :disabled="refreshing"
            class="btn btn-icon-md btn-info"
            title="刷新当前分类" aria-label="刷新当前分类"
          >
            <svg :class="['w-4 h-4', refreshing ? 'animate-spin' : '']" viewBox="0 0 24 24" fill="none" aria-hidden="true">
              <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M3 16v5h5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
              <path d="M21 3v5h-5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <span class="sr-only">{{ refreshing ? '刷新中' : '刷新当前分类' }}</span>
          </button>
          <span class="min-w-0 truncate text-xs text-[color:var(--c-text-muted)]">{{ refreshInfo }}</span>
        </div>
        <div v-if="failedBannerMsg" class="rounded border border-[color:var(--c-warning)] bg-warning-soft px-3 py-2 text-xs text-warning-text">
          {{ failedBannerMsg }}
        </div>
      </div>

      <div class="min-h-0 flex-1 divide-y divide-[color:var(--c-border)] overflow-y-auto overscroll-contain">
        <div v-if="!actorsStore.filtered.length" class="empty-state flex-1">
          {{ actorsStore.loaded ? '该分类暂无收藏演员，点击刷新获取' : '点击刷新按钮从 JavDB 获取收藏演员' }}
        </div>
        <template v-else>
          <div
            v-for="actor in actorsStore.filtered"
            :key="actor.actor_id"
            class="px-4 py-2 text-sm"
          >
            <div class="flex items-start gap-2">
              <div class="min-w-0 flex-1">
                <div class="flex flex-wrap items-center gap-2">
                  <span class="inline-flex min-w-0 items-center font-bold" :title="actor.actor_name">
                    <button
                      v-if="actor.has_collection"
                      type="button"
                      @click="goToCollection(actor.collection_filename || actor.actor_id)"
                      title="已存在数据集合，点击查看"
                      aria-label="已入库，点击查看数据集合"
                      class="mr-1 inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full bg-success-soft text-[11px] font-bold leading-none text-success-text"
                    >✓</button>
                    <span class="truncate">{{ actor.actor_name }}</span>
                  </span>
                  <span class="badge badge-neutral shrink-0">{{ actorsStore.categoryLabel(actor.category) }}</span>
                </div>
                <div v-if="actor.last_task_tags?.length" class="mt-1 flex flex-wrap gap-1">
                  <span v-for="t in actor.last_task_tags" :key="t.value" class="badge badge-info">{{ t.name || t.value }}</span>
                </div>
              </div>
              <div class="flex shrink-0 items-center gap-1">
                <button
                  type="button"
                  @click="actorsStore.toggleActorTags(actor.actor_id)"
                  class="btn btn-sm btn-soft"
                >{{ actorsStore.expandedActorId === actor.actor_id ? '收起' : '标签' }}</button>
                <button
                  type="button"
                  @click="handleAddTask(actor.actor_id)"
                  class="btn btn-icon-sm btn-primary"
                  title="加入队列" aria-label="加入队列"
                >
                  <svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                    <path d="M8 6h8M8 12h5M8 18h8" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                    <path d="M18 9v6M15 12h6" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                  </svg>
                  <span class="sr-only">加入队列</span>
                </button>
              </div>
            </div>

            <!-- 标签面板 -->
            <div v-if="actorsStore.expandedActorId === actor.actor_id" class="mt-2 rounded-lg border border-[color:var(--c-primary-soft)] p-3">
              <div v-if="actorsStore.tagStates[actor.actor_id]?.loading" class="text-xs text-[color:var(--c-text-muted)]">加载标签中...</div>
              <div v-else-if="actorsStore.tagStates[actor.actor_id]?.error" class="text-xs text-danger-text">{{ actorsStore.tagStates[actor.actor_id]?.error }}</div>
              <div v-else class="max-h-[130px] overflow-y-auto overscroll-contain flex flex-wrap gap-2">
                <span v-if="!actorsStore.tagStates[actor.actor_id]?.tags?.length" class="text-xs text-[color:var(--c-text-subtle)]">该演员页无可选标签，可直接加入任务</span>
                <button
                  v-for="t in actorsStore.tagStates[actor.actor_id]?.tags"
                  :key="t.value"
                  type="button"
                  @click="actorsStore.toggleTag(actor.actor_id, t.value)"
                  :class="[
                    'px-2.5 py-1 rounded text-xs border transition-colors',
                    actorsStore.tagStates[actor.actor_id]?.selected?.has(t.value)
                      ? 'bg-primary text-white border-primary'
                      : 'bg-surface text-[color:var(--c-text-muted)] border-[color:var(--c-border)] hover:bg-surface-sunken'
                  ]"
                >{{ t.name }}</button>
              </div>
            </div>
          </div>
        </template>
      </div>
    </section>
  </section>
</template>
