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

/*
 * 互斥菜单状态管理：三个菜单变量（标签过滤 / 排除标签 / 磁力检测）在任一时刻至多开一个。
 * 统一在此切换——先清空全部，再按 kind 设值（若点击的是已开项则保持关闭），最后调用重渲染回调。
 * 重渲染目标因菜单位置不同而不同，由调用方传入 rerender。
 */
function openExclusiveMenu(kind, value, rerender) {
    const same = (kind === 'tag' && openTagDropdown === value)
        || (kind === 'exclude' && openExcludeDropdown === value)
        || (kind === 'check' && openMagnetCheckMenu === value);
    openTagDropdown = null;
    openExcludeDropdown = null;
    openMagnetCheckMenu = null;
    if (!same) {
        if (kind === 'tag') openTagDropdown = value;
        else if (kind === 'exclude') openExcludeDropdown = value;
        else if (kind === 'check') openMagnetCheckMenu = value;
    }
    if (typeof rerender === 'function') rerender();
}

function closeOpenMenus() {
    if (!openTagDropdown && !openExcludeDropdown && !openMagnetCheckMenu) return;
    const collectionName = expandedCollectionName;
    openTagDropdown = null;
    openExcludeDropdown = null;
    openMagnetCheckMenu = null;
    if (collectionName) renderCollectionBody(collectionName);
    else if (activeView === 'database' && typeof renderDatabaseRoute === 'function') renderDatabaseRoute();
}

function handleDocumentClick(event) {
    if (!openTagDropdown && !openExcludeDropdown && !openMagnetCheckMenu) return;
    const target = event.target;
    if (target.closest('[data-menu-root="tag-filter"]') || target.closest('[data-menu-root="exclude-filter"]') || target.closest('[data-menu-root="magnet-check"]')) return;
    closeOpenMenus();
}

/* ===== 工作区导航 ===== */

function normalizeView(value) {
    const root = String(value || '').split('/')[0];
    return ['tasks', 'database', 'actors', 'settings'].includes(root) ? root : 'tasks';
}

function setActiveView(view) {
    activeView = normalizeView(view);
    const lockViewport = activeView === 'actors';
    document.documentElement.classList.toggle('app-view-locked', lockViewport);
    document.body.classList.toggle('app-view-locked', lockViewport);
    document.querySelectorAll('[data-view]').forEach(section => {
        section.classList.toggle('hidden', section.dataset.view !== activeView);
    });
    ['tasks', 'database', 'actors', 'settings'].forEach(item => {
        const button = document.getElementById(`nav-${item}`);
        if (!button) return;
        const active = item === activeView;
        button.classList.toggle('nav-seg--active', active);
        if (active) {
            button.setAttribute('aria-current', 'page');
        } else {
            button.removeAttribute('aria-current');
        }
    });
    if (activeView === 'database') {
        if (typeof renderDatabaseRoute === 'function') renderDatabaseRoute();
        fitMovieTags();
    }
    if (activeView === 'actors') {
        window.scrollTo({ top: 0, left: 0 });
        if (typeof renderActorsView === 'function') renderActorsView();
        if (typeof fitActorsLayout === 'function') requestAnimationFrame(fitActorsLayout);
    }
    if (activeView === 'settings') {
    }
    if (activeView === 'tasks' && typeof fitTasksLayout === 'function') {
        requestAnimationFrame(fitTasksLayout);
    }
}

function viewFromHash() {
    return normalizeView((window.location.hash || '#/tasks').replace(/^#\/?/, ''));
}

function navigateToView(view) {
    const next = normalizeView(view);
    if (window.location.hash !== `#/${next}`) {
        window.location.hash = `#/${next}`;
    } else {
        setActiveView(next);
    }
}

function handleHashChange() {
    setActiveView(viewFromHash());
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
    initTheme();
    showAuthPanel(false);
    window.addEventListener('hashchange', handleHashChange);
    setActiveView(viewFromHash());
    renderLogPanelState();
    window.addEventListener('resize', debounce(() => {
        fitMovieTags();
        if (typeof fitTasksLayout === 'function') fitTasksLayout();
        if (activeView === 'actors' && typeof fitActorsLayout === 'function') fitActorsLayout();
    }, 150));
    document.addEventListener('click', handleDocumentClick);
    document.getElementById('start_url').value = localStorage.getItem('javdb_url') || '';
    document.getElementById('filename').value = localStorage.getItem('javdb_filename') || '';
    document.getElementById('user_agent').value = localStorage.getItem('javdb_ua') || defaultUA();
    try {
        const version = await fetch('/api/version').then(r => r.json());
        const payload = version.data || version;
        document.getElementById('app_version').innerText = payload.version || 'v?.?.?';
        authRequired = !!payload.auth_required;
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
