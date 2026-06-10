/*
 * app.js — 应用入口
 * 鉴权流程、全局菜单关闭、初始化与首屏加载。
 * 依赖其它模块（utils/api/state/settings/tasks/movies/magnets）已在本文件之前加载。
 */

/* ===== 鉴权面板 ===== */

function showAuthPanel(show) {
    document.getElementById('authGate').classList.toggle('hidden', !show);
    document.getElementById('appShell').classList.toggle('hidden', show);
    if (show) setTimeout(() => document.getElementById('auth_token').focus(), 0);
}

function setAuthError(message = '') {
    const box = document.getElementById('auth_error');
    box.innerText = message;
    box.classList.toggle('hidden', !message);
}

function setAuthSubmitting(submitting) {
    const button = document.getElementById('auth_submit');
    const input = document.getElementById('auth_token');
    button.disabled = submitting;
    input.disabled = submitting;
    button.innerText = submitting ? '验证中...' : '验证';
}

function lockAppForAuth(message = '') {
    apiToken = '';
    sessionStorage.removeItem('javdb_auth_token');
    stopMonitorPolling();
    stopMagnetCheckPolling();
    setAuthError(message);
    setAuthSubmitting(false);
    showAuthPanel(true);
}

async function verifyAuthToken(token) {
    const response = await fetch('/api/status', { headers: { 'X-JavDB-Token': token } });
    if (response.status === 401) return false;
    if (!response.ok) throw new Error('验证失败，请稍后重试');
    return true;
}

function handleAuthTokenKeydown(event) {
    if (event.key === 'Enter') saveAuthToken();
}

async function saveAuthToken() {
    const token = document.getElementById('auth_token').value.trim();
    if (!token) {
        setAuthError('请输入访问令牌');
        return;
    }
    setAuthError('');
    setAuthSubmitting(true);
    try {
        const ok = await verifyAuthToken(token);
        if (!ok) {
            setAuthError('访问令牌缺失或无效');
            return;
        }
        apiToken = token;
        sessionStorage.setItem('javdb_auth_token', apiToken);
        showAuthPanel(false);
        await startApp();
    } catch (err) {
        setAuthError(err.message || '验证失败，请稍后重试');
    } finally {
        setAuthSubmitting(false);
    }
}

/* ===== 全局下拉菜单关闭 ===== */

function closeOpenMenus() {
    if (!openTagDropdown && !openExcludeDropdown && !openMagnetCheckMenu) return;
    const collectionName = expandedCollectionName;
    openTagDropdown = null;
    openExcludeDropdown = null;
    openMagnetCheckMenu = null;
    if (collectionName) renderCollectionBody(collectionName);
}

function handleDocumentClick(event) {
    if (!openTagDropdown && !openExcludeDropdown && !openMagnetCheckMenu) return;
    const target = event.target;
    if (target.closest('[data-menu-root="tag-filter"]') || target.closest('[data-menu-root="exclude-filter"]') || target.closest('[data-menu-root="magnet-check"]')) return;
    closeOpenMenus();
}

/* ===== 初始化与首屏加载 ===== */

async function refreshAll() {
    await loadRuntimeConfig();
    await refreshMonitor();
    await loadCollections();
    await restoreMagnetCheckJob();
}

async function startApp() {
    showAuthPanel(false);
    await refreshAll();
    startMonitorPolling(2500);
}

window.onload = async function() {
    renderRuntimePanelState();
    renderLogPanelState();
    window.addEventListener('resize', () => fitMovieTags());
    document.addEventListener('click', handleDocumentClick);
    document.getElementById('start_url').value = localStorage.getItem('javdb_url') || '';
    document.getElementById('filename').value = localStorage.getItem('javdb_filename') || '';
    document.getElementById('user_agent').value = localStorage.getItem('javdb_ua') || defaultUA();
    try {
        const version = await fetch('/api/version').then(r => r.json());
        document.getElementById('app_version').innerText = version.version || 'v?.?.?';
        authRequired = !!version.auth_required;
    } catch {
        document.getElementById('app_version').innerText = 'v?.?.?';
        lockAppForAuth('无法读取鉴权状态，请刷新重试');
        return;
    }
    if (!authRequired) {
        await startApp();
        return;
    }
    if (!apiToken) {
        lockAppForAuth();
        return;
    }
    try {
        if (await verifyAuthToken(apiToken)) {
            await startApp();
        } else {
            lockAppForAuth('访问令牌缺失或无效');
        }
    } catch (err) {
        lockAppForAuth(err.message || '验证失败，请稍后重试');
    }
};
