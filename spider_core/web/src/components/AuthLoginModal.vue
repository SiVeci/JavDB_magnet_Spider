<script setup lang="ts">
import { ref, watch, nextTick, onUnmounted } from 'vue'
import { useAuthBrowser } from '@/composables/useAuthBrowser'
import { toErrMsg } from '@/utils/error'
import { STORAGE_KEYS } from '@/constants/storageKeys'

const props = defineProps<{ open: boolean; rememberCookie?: boolean }>()
const emit = defineEmits<{
  (e: 'update:open', value: boolean): void
  (e: 'success', msg: string): void
}>()

const authBrowser = useAuthBrowser()
const email = ref('')
const password = ref('')
const captcha = ref('')
const errorMsg = ref('')
const rememberCreds = ref(localStorage.getItem(STORAGE_KEYS.loginRemember) === '1')

const dialogEl = ref<HTMLElement | null>(null)
let lastFocused: HTMLElement | null = null

function onKeydown(e: KeyboardEvent) {
  if (e.key === 'Escape') { closeModal(); return }
  if (e.key === 'Tab' && dialogEl.value) {
    const focusable = dialogEl.value.querySelectorAll<HTMLElement>(
      'a[href],button:not([disabled]),input:not([disabled]),[tabindex]:not([tabindex="-1"])'
    )
    if (!focusable.length) return
    const first = focusable[0], last = focusable[focusable.length - 1]
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus() }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus() }
  }
}

onUnmounted(() => document.removeEventListener('keydown', onKeydown))

// 记住的账号密码存在 localStorage（密码仅 base64 编码防肩窥，非加密）。
const REMEMBER_KEY = STORAGE_KEYS.loginRemember
const EMAIL_KEY = STORAGE_KEYS.loginEmail
const PASSWORD_KEY = STORAGE_KEYS.loginPassword

function loadSavedCreds() {
  email.value = localStorage.getItem(EMAIL_KEY) || ''
  const saved = localStorage.getItem(PASSWORD_KEY)
  try {
    password.value = saved ? atob(saved) : ''
  } catch {
    password.value = ''
  }
}

function persistCreds() {
  if (rememberCreds.value) {
    localStorage.setItem(REMEMBER_KEY, '1')
    localStorage.setItem(EMAIL_KEY, email.value)
    localStorage.setItem(PASSWORD_KEY, btoa(password.value))
  } else {
    localStorage.removeItem(REMEMBER_KEY)
    localStorage.removeItem(EMAIL_KEY)
    localStorage.removeItem(PASSWORD_KEY)
  }
}

// 弹窗打开时初始化登录会话；关闭时清理。
watch(
  () => props.open,
  async (isOpen) => {
    if (isOpen) {
      lastFocused = document.activeElement as HTMLElement | null
      document.addEventListener('keydown', onKeydown)
      if (rememberCreds.value) {
        loadSavedCreds()
      } else {
        email.value = ''
        password.value = ''
      }
      captcha.value = ''
      errorMsg.value = ''
      try {
        await authBrowser.start()
      } catch (err: unknown) {
        errorMsg.value = toErrMsg(err, '无法开始登录')
      }
      await nextTick()
      dialogEl.value?.querySelector<HTMLElement>('input')?.focus()
    } else {
      document.removeEventListener('keydown', onKeydown)
      await authBrowser.close()
      lastFocused?.focus()
    }
  },
)

function closeModal() {
  emit('update:open', false)
}

async function refreshCaptcha() {
  try {
    await authBrowser.refreshCaptcha()
  } catch (err: unknown) {
    errorMsg.value = toErrMsg(err, '刷新验证码失败')
  }
}

async function submit() {
  errorMsg.value = ''
  if (!email.value || !password.value) {
    errorMsg.value = '请填写账号和密码'
    return
  }
  try {
    const res = await authBrowser.login(email.value, password.value, captcha.value, props.rememberCookie ?? true)
    persistCreds()
    emit('success', res.msg || '登录成功，Cookie 已保存')
    emit('update:open', false)
  } catch (err: unknown) {
    errorMsg.value = toErrMsg(err, '登录失败')
    captcha.value = ''
    try { await authBrowser.refreshCaptcha() } catch { /* 忽略刷新失败 */ }
  }
}
</script>

<template>
  <div
    v-if="open"
    class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
    @click.self="closeModal"
    role="presentation"
  >
    <div
      ref="dialogEl"
      role="dialog"
      aria-modal="true"
      aria-labelledby="auth-modal-title"
      class="w-full max-w-sm rounded-xl border border-[color:var(--c-border)] bg-surface p-5 shadow-pop"
    >
      <div class="mb-4 flex items-center justify-between">
        <h3 id="auth-modal-title" class="text-base font-semibold text-strong">登录 JavDB 获取 Cookie</h3>
        <button type="button" @click="closeModal" class="btn btn-sm btn-soft" aria-label="关闭">✕</button>
      </div>
      <div class="space-y-3">
        <div>
          <label class="field-label">账号 / 邮箱</label>
          <input
            v-model="email"
            type="text"
            autocomplete="username"
            class="input"
            placeholder="用户名或邮箱"
            @keyup.enter="submit"
          />
        </div>
        <div>
          <label class="field-label">密码</label>
          <input
            v-model="password"
            type="password"
            autocomplete="current-password"
            class="input"
            placeholder="密码"
            @keyup.enter="submit"
          />
        </div>
        <div v-if="authBrowser.needsCaptcha.value">
          <label class="field-label">验证码</label>
          <div class="flex items-center gap-2">
            <img
              v-if="authBrowser.captchaImage.value"
              :src="authBrowser.captchaImage.value"
              alt="验证码"
              class="h-10 cursor-pointer rounded border border-soft"
              title="点击刷新验证码"
              @click="refreshCaptcha"
            />
            <span v-else class="text-xs text-muted">加载中…</span>
            <input
              v-model="captcha"
              type="text"
              maxlength="5"
              autocomplete="off"
              class="input input-mono flex-1"
              placeholder="图中字符"
              @keyup.enter="submit"
            />
          </div>
          <p class="mt-1 text-xs text-muted">点击图片可刷新验证码</p>
        </div>
        <label class="flex items-center gap-2 text-sm text-[color:var(--c-text)] cursor-pointer select-none">
          <input v-model="rememberCreds" type="checkbox" class="h-4 w-4 cursor-pointer" />
          记住账号密码
        </label>
        <p v-if="rememberCreds" class="text-xs text-warning-text">
          ⚠ 密码以 base64 明文等价方式存于浏览器 localStorage，他人可读，请仅在私人设备使用。
        </p>
        <p v-if="errorMsg" class="text-xs text-danger-text">{{ errorMsg }}</p>
        <div class="flex justify-end gap-2 pt-1">
          <button type="button" @click="closeModal" class="btn btn-sm btn-soft">取消</button>
          <button
            type="button"
            @click="submit"
            :disabled="authBrowser.loading.value || !authBrowser.sessionId.value"
            class="btn btn-sm btn-info"
          >{{ authBrowser.loading.value ? '登录中…' : '登录' }}</button>
        </div>
      </div>
    </div>
  </div>
</template>
