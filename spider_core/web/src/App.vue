<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useAuthStore } from '@/stores/auth'
import { useTasksStore } from '@/stores/tasks'
import { useSettingsStore } from '@/stores/settings'
import { useDatabaseStore } from '@/stores/database'
import { useTheme } from '@/composables/useTheme'

const router = useRouter()
const route = useRoute()
const auth = useAuthStore()
const tasksStore = useTasksStore()
const settings = useSettingsStore()
const dbStore = useDatabaseStore()
const { theme, cycleTheme } = useTheme()

const authTokenInput = ref('')
const authError = ref('')
const authSubmitting = ref(false)
const appVersion = ref('加载中...')

const activeView = computed(() => {
  const path = route.path
  if (path.startsWith('/database')) return 'database'
  if (path.startsWith('/actors')) return 'actors'
  if (path.startsWith('/settings')) return 'settings'
  return 'tasks'
})

function navigateTo(view: string) {
  router.push(`/${view}`)
}

async function submitAuth() {
  if (!authTokenInput.value.trim()) { authError.value = '请输入访问令牌'; return }
  authError.value = ''
  authSubmitting.value = true
  try {
    const result = await auth.login(authTokenInput.value)
    if (!result.ok) { authError.value = result.error || '验证失败'; return }
    await startApp()
  } finally {
    authSubmitting.value = false
  }
}

function handleAuthKeydown(e: KeyboardEvent) {
  if (e.key === 'Enter') submitAuth()
}

async function startApp() {
  await Promise.all([
    settings.load(),
    tasksStore.refresh(),
    dbStore.loadCollections(),
  ])
  // 使用 SSE 推送任务状态，运行中进度由后端主动广播。
  tasksStore.startSSE(() => sessionStorage.getItem('javdb_auth_token') || '')
}

const themeLabel = computed(() => theme.value === 'dark' ? '切换到亮色' : '切换到暗色')

onMounted(async () => {
  const { ok, version } = await auth.checkInitialAuth()
  appVersion.value = version || 'v?.?.?'
  if (ok) await startApp()
})
</script>

<template>
  <!-- 鉴权遮罩 -->
  <section
    v-if="auth.locked"
    class="fixed inset-0 z-50 flex items-center justify-center bg-[color:var(--c-bg)] p-4 text-[color:var(--c-text)]"
  >
    <div class="w-full max-w-md rounded-xl border border-[color:var(--c-border)] bg-surface p-6 shadow-pop">
      <div class="flex flex-col items-center gap-3 text-center">
        <img src="/favicon.png" alt="Logo" class="h-12 w-12 rounded-lg" />
        <div>
          <h1 class="font-display text-3xl font-medium leading-tight text-[color:var(--c-text-strong)]">JavDB Magnet Spider</h1>
          <p class="mt-2 text-sm text-[color:var(--c-text-muted)]">输入访问令牌后继续</p>
        </div>
      </div>
      <div class="mt-6 space-y-3">
        <input
          v-model="authTokenInput"
          type="password"
          autocomplete="off"
          placeholder="访问令牌"
          @keydown="handleAuthKeydown"
          class="input py-3"
        />
        <button
          type="button"
          :disabled="authSubmitting"
          @click="submitAuth"
          class="btn btn-primary w-full px-4 py-3"
        >{{ authSubmitting ? '验证中...' : '验证' }}</button>
        <div v-if="auth.lockMessage || authError" class="text-center text-sm text-danger-text">
          {{ auth.lockMessage || authError }}
        </div>
      </div>
    </div>
  </section>

  <!-- 主应用壳 -->
  <main v-else class="mx-auto max-w-7xl space-y-6 p-4 md:p-8">
    <header class="rounded-xl border border-[color:var(--c-border)] bg-surface/95 px-4 py-4 shadow-sm md:px-5">
      <div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
      <div class="flex flex-col items-center gap-2 md:items-start">
        <div class="flex items-center gap-3">
          <img src="/favicon.png" alt="Logo" class="h-10 w-10 rounded-lg" />
          <h1 class="font-display text-2xl font-medium leading-none text-[color:var(--c-text-strong)] md:text-3xl">JavDB Magnet Spider</h1>
        </div>
        <div class="flex items-center justify-center gap-2 md:justify-start">
          <span class="badge badge-neutral">{{ appVersion }}</span>
          <button
            type="button"
            @click="cycleTheme"
            :title="themeLabel"
            :aria-label="themeLabel"
            :aria-pressed="theme === 'dark' ? 'true' : 'false'"
            :data-theme-state="theme"
            class="theme-switch"
          >
            <span class="theme-switch__icon theme-switch__icon--light" aria-hidden="true">☀</span>
            <span class="theme-switch__icon theme-switch__icon--dark" aria-hidden="true">☾</span>
            <span class="theme-switch__thumb" aria-hidden="true"></span>
          </button>
        </div>
      </div>
      <nav
        class="grid w-full grid-cols-4 rounded-lg border border-[color:var(--c-border)] bg-surface-sunken p-1 text-sm shadow-sm md:w-[26rem]"
        aria-label="主导航"
      >
        <button
          v-for="item in ['tasks','database','actors','settings']"
          :key="item"
          type="button"
          @click="navigateTo(item)"
          :class="['nav-seg', activeView === item ? 'nav-seg--active' : '']"
          :aria-current="activeView === item ? 'page' : undefined"
        >{{ { tasks: '任务', database: '数据库', actors: '收藏演员', settings: '设置' }[item] }}</button>
      </nav>
      </div>
    </header>

    <router-view v-slot="{ Component }">
      <Transition name="fade" mode="out-in">
        <component :is="Component" />
      </Transition>
    </router-view>
  </main>
</template>

<style scoped>
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.12s ease;
}
.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>

