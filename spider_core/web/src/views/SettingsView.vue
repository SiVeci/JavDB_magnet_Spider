<script setup lang="ts">
import { ref, computed } from 'vue'
import { useSettingsStore } from '@/stores/settings'
import { useToast } from '@/composables/useToast'

const settings = useSettingsStore()
const { showToast } = useToast()

const proxyParsed = computed(() => settings.parseProxy())
const proxyHost = ref(proxyParsed.value.host)
const proxyPort = ref(proxyParsed.value.port)
const trackerText = computed({
  get: () => settings.config.trackers.join('\n'),
  set: (v) => { settings.config.trackers = v.split(/\r?\n/).map(s => s.trim()).filter(Boolean) },
})
const hostError = ref(false)
const portError = ref(false)

function validateProxy(): boolean {
  const h = proxyHost.value.trim()
  const p = proxyPort.value.trim()
  hostError.value = false
  portError.value = false
  if (!h && !p) { settings.setProxy('', ''); return true }
  if (!h) { hostError.value = true }
  if (!p) { portError.value = true }
  if (hostError.value || portError.value) { showToast('代理地址和端口必须同时填写'); return false }
  settings.setProxy(h, p)
  return true
}

async function handleSave() {
  if (!validateProxy()) return
  try {
    const msg = await settings.save(true)
    if (msg) showToast(msg)
  } catch (err: unknown) {
    showToast(err instanceof Error ? err.message : '保存失败')
  }
}
</script>

<template>
  <section class="max-w-3xl mx-auto">
    <section class="card overflow-hidden">
      <div class="card-head">
        <h2 class="card-title">全局运行配置</h2>
      </div>
      <div class="p-5 space-y-4 text-sm">
        <div>
          <div class="flex justify-between items-center mb-1">
            <label class="text-[color:var(--c-text-muted)]">Cookie</label>
            <a href="https://javdb.com" target="_blank" class="text-xs text-info-text underline">打开 JavDB</a>
          </div>
          <textarea
            v-model="settings.config.cookie"
            rows="4"
            class="input input-mono"
          ></textarea>
          <label class="mt-2 flex items-center gap-2 text-xs text-[color:var(--c-text-muted)]">
            <input
              v-model="settings.config.remember_cookie"
              type="checkbox"
              class="w-4 h-4 accent-[color:var(--c-primary)]"
            />
            <span>记住 Cookie（写入后端数据库）</span>
          </label>
        </div>
        <div>
          <label class="field-label">User-Agent</label>
          <input v-model="settings.config.user_agent" type="text" class="input input-mono" />
        </div>
        <div>
          <label class="field-label">HTTP 代理</label>
          <div class="flex gap-2">
            <input
              v-model="proxyHost"
              type="text"
              :class="['input input-mono w-2/3', hostError ? 'is-invalid' : '']"
              placeholder="127.0.0.1"
            />
            <input
              v-model="proxyPort"
              type="text"
              :class="['input input-mono w-1/3', portError ? 'is-invalid' : '']"
              placeholder="7890"
            />
          </div>
        </div>
        <div>
          <label class="field-label">Tracker 列表</label>
          <textarea
            v-model="trackerText"
            rows="4"
            class="input input-mono"
            placeholder="一行一个 tracker URL"
          ></textarea>
        </div>
        <button
          type="button"
          @click="handleSave"
          class="btn btn-lg w-full bg-[color:var(--c-neutral)] text-white hover:brightness-90"
        >保存全局配置</button>
      </div>
    </section>
  </section>
</template>
