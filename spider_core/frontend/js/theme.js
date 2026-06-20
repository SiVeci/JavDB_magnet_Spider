/*
 * theme.js — 明暗模式切换
 * 二档：'light' | 'dark'。
 * 持久化到 localStorage，页面加载前由 <head> 内联脚本同步初始化，避免 FOUC。
 * 本文件提供切换 UI。
 */

const THEME_KEY = 'javdb_theme';
const THEMES = ['light', 'dark'];

/* 读取已保存的用户偏好；旧 auto 或无效值按亮色处理。 */
function getSavedTheme() {
    const saved = localStorage.getItem(THEME_KEY);
    return THEMES.includes(saved) ? saved : 'light';
}

/* 将 data-theme 设置到 <html> 上 */
function applyTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    updateThemeSwitch(theme);
}

/* 保存偏好并应用 */
function setTheme(theme) {
    localStorage.setItem(THEME_KEY, theme);
    applyTheme(theme);
}

/* 二态切换：light ↔ dark */
function cycleTheme() {
    const current = getSavedTheme();
    const next = current === 'dark' ? 'light' : 'dark';
    setTheme(next);
}

/* 更新滑动开关状态 */
function updateThemeSwitch(theme) {
    const btn = document.getElementById('themeToggleBtn');
    if (!btn) return;

    const nextLabel = theme === 'dark' ? '切换到亮色' : '切换到暗色';
    btn.dataset.themeState = theme;
    btn.title = nextLabel;
    btn.setAttribute('aria-label', nextLabel);
    btn.setAttribute('aria-pressed', theme === 'dark' ? 'true' : 'false');
}

/* 初始化（由 <head> 内联脚本同步设置 data-theme，此处同步开关状态） */
function initTheme() {
    const theme = getSavedTheme();
    applyTheme(theme);
}
