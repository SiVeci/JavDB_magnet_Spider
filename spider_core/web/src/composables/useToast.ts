export type ToastType = 'info' | 'success' | 'error'

export function useToast() {
  function showToast(message: string, type: ToastType = 'info') {
    const text = String(message ?? '')
    const container = document.getElementById('toast-container')
    if (!container) { console.log(`[toast:${type}] ${text}`); return }
    const toast = document.createElement('div')
    toast.className = `toast toast-${type}`
    toast.textContent = text
    container.appendChild(toast)
    requestAnimationFrame(() => toast.classList.add('toast-show'))
    setTimeout(() => {
      toast.classList.remove('toast-show')
      setTimeout(() => toast.remove(), 300)
    }, 3000)
  }
  return { showToast }
}
