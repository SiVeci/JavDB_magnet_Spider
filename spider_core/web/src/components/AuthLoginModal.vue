<script setup lang="ts">
import { ref, watch } from 'vue'
import { useAuthBrowser } from '@/composables/useAuthBrowser'

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

// 弹窗打开时初始化登录会话；关闭时清理。
watch(
  () => props.open,
  async (isOpen) => {
    if (isOpen) {
      email.value = ''
      password.value = ''
      captcha.value = ''
      errorMsg.value = ''
      try {
        await authBrowser.start()
      } catch (err: unknown) {
        errorMsg.value = err instanceof Error ? err.message : '无法开始登录'
      }
    } else {
      await authBrowser.close()
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
    errorMsg.value = err instanceof Error ? err.message : '刷新验证码失败'
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
    emit('success', res.msg || '登录成功，Cookie 已保存')
    emit('update:open', false)
  } catch (err: unknown) {
    errorMsg.value = err instanceof Error ? err.message : '登录失败'
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
  >
    <div class="w-full max-w-sm rounded-xl border border-[color:var(--c-border)] bg-surface p-5 shadow-pop">
      <div class="mb-4 flex items-center justify-between">
        <h3 class="text-base font-semibold text-[color:var(--c-text-strong)]">登录 JavDB 获取 Cookie</h3>
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
              class="h-10 cursor-pointer rounded border border-[color:var(--c-border-soft)]"
              title="点击刷新验证码"
              @click="refreshCaptcha"
            />
            <span v-else class="text-xs text-[color:var(--c-text-muted)]">加载中…</span>
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
          <p class="mt-1 text-xs text-[color:var(--c-text-muted)]">点击图片可刷新验证码</p>
        </div>
        <p v-if="errorMsg" class="text-xs" style="color:#dc2626">{{ errorMsg }}</p>
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
