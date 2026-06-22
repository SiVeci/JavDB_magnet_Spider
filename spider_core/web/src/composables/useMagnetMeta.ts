/*
 * useMagnetMeta — 磁力检测状态映射（还原旧版 meta.js magnetStatusMeta）
 * 单条磁力 → { icon(emoji), title, text(文字色类) }
 */
import type { Magnet } from '@/types'

const MAGNET_STATUS_META: Record<string, { icon: string; title: string; text: string }> = {
  active: { icon: '🟢', title: '有效', text: 'text-success-text' },
  weak: { icon: '🟡', title: '弱', text: 'text-warning-text' },
  dead: { icon: '🔴', title: '无效', text: 'text-danger-text' },
}

export function magnetStatusMeta(magnet: Magnet): { icon: string; title: string; text: string } {
  if (magnet.check_error && !magnet.check_status) {
    return { icon: '❌', title: magnet.check_error, text: 'text-[color:var(--c-text-muted)]' }
  }
  if (!magnet.checked_at) {
    return { icon: '⚪', title: '未检测', text: 'text-[color:var(--c-text-subtle)]' }
  }
  const meta = MAGNET_STATUS_META[magnet.check_status || '']
  if (meta) {
    if (magnet.check_status === 'dead') return { ...meta, title: magnet.check_error || meta.title }
    return meta
  }
  return { icon: '❌', title: magnet.check_error || '检测失败', text: 'text-[color:var(--c-text-muted)]' }
}

export function formatGb(sizeMb?: number): string {
  return `${(Number(sizeMb || 0) / 1024).toFixed(1)} GB`
}
