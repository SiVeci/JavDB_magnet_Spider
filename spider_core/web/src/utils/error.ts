/** 从未知异常中提取可读消息，无则用 fallback。 */
export function toErrMsg(err: unknown, fallback = '操作失败'): string {
  return err instanceof Error ? err.message : fallback
}
