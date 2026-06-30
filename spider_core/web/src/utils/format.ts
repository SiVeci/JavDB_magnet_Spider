/** 去掉集合文件名末尾的 .csv 后缀，用于展示。 */
export function displayName(val: string): string {
  return String(val || '').replace(/\.csv$/i, '')
}
