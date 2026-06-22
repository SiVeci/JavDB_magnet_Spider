export function useClipboard() {
  async function copyText(text: string): Promise<boolean> {
    const value = String(text || '')
    if (navigator.clipboard && window.isSecureContext) {
      try { await navigator.clipboard.writeText(value); return true } catch { /* fallback */ }
    }
    const area = document.createElement('textarea')
    area.value = value
    area.setAttribute('readonly', '')
    area.style.position = 'fixed'
    area.style.top = '0'
    area.style.left = '-9999px'
    document.body.appendChild(area)
    area.focus()
    area.select()
    area.setSelectionRange(0, area.value.length)
    let copied = false
    try { copied = document.execCommand('copy') } finally { document.body.removeChild(area) }
    if (!copied) window.prompt('自动复制失败，请手动复制：', value)
    return copied
  }
  return { copyText }
}
