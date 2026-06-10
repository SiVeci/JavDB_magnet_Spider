/*
 * settings.js — 全局运行配置（Cookie / UA / 代理 / Tracker）
 * 对应「全局运行配置」面板的读取、保存与浏览器端缓存。
 */

function defaultUA() {
    return 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0';
}

function getProxyValue() {
    const host = document.getElementById('proxy_host').value.trim();
    const port = document.getElementById('proxy_port').value.trim();
    if (!host && !port) return '';
    if (!host || !port) throw new Error('代理地址和端口必须同时填写');
    return `http://${host.replace(/^https?:\/\//, '')}:${port}`;
}

function setProxyInputs(proxy) {
    const value = proxy || '';
    const clean = value.replace(/^https?:\/\//, '');
    const index = clean.lastIndexOf(':');
    document.getElementById('proxy_host').value = index > 0 ? clean.slice(0, index) : clean;
    document.getElementById('proxy_port').value = index > 0 ? clean.slice(index + 1) : '';
}

function getTrackerList() {
    return document.getElementById('tracker_list').value
        .split(/\r?\n/)
        .map(item => item.trim())
        .filter(Boolean);
}

function setTrackerList(trackers) {
    document.getElementById('tracker_list').value = (trackers || []).join('\n');
}

function renderRuntimePanelState() {
    document.getElementById('runtime-config-body').classList.toggle('hidden', runtimeConfigCollapsed);
    document.getElementById('runtime-toggle-icon').innerText = runtimeConfigCollapsed ? '▼' : '▲';
}

function toggleRuntimePanel() {
    runtimeConfigCollapsed = !runtimeConfigCollapsed;
    renderRuntimePanelState();
}

function saveCookieBrowserCache() {
    const cookie = document.getElementById('cookie').value;
    if (document.getElementById('remember_cookie').checked) {
        localStorage.setItem('javdb_remember_cookie', '1');
        localStorage.setItem('javdb_cookie', cookie);
        sessionStorage.removeItem('javdb_cookie');
    } else {
        localStorage.setItem('javdb_remember_cookie', '0');
        localStorage.removeItem('javdb_cookie');
        sessionStorage.setItem('javdb_cookie', cookie);
    }
}

async function loadRuntimeConfig() {
    const remember = localStorage.getItem('javdb_remember_cookie') === '1';
    document.getElementById('remember_cookie').checked = remember;
    document.getElementById('cookie').value = remember ? (localStorage.getItem('javdb_cookie') || '') : (sessionStorage.getItem('javdb_cookie') || '');

    const res = await apiFetch('/api/runtime_config').then(r => r.json());
    if (res.code !== 200) return;
    const data = res.data;
    document.getElementById('remember_cookie').checked = !!data.remember_cookie;
    document.getElementById('user_agent').value = data.user_agent || localStorage.getItem('javdb_ua') || defaultUA();
    setProxyInputs(data.proxies || localStorage.getItem('javdb_proxy') || '');
    setTrackerList(data.trackers || []);
}

async function saveRuntimeConfig(showMessage = false) {
    let proxy = '';
    try {
        proxy = getProxyValue();
    } catch (err) {
        return showToast(err.message);
    }
    const payload = {
        cookie: document.getElementById('cookie').value.trim(),
        remember_cookie: document.getElementById('remember_cookie').checked,
        user_agent: document.getElementById('user_agent').value.trim(),
        proxies: proxy,
        trackers: getTrackerList()
    };
    saveCookieBrowserCache();
    localStorage.setItem('javdb_ua', payload.user_agent);
    localStorage.setItem('javdb_proxy', proxy);
    const res = await apiFetch('/api/runtime_config', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    }).then(r => r.json());
    if (showMessage) showToast(res.msg || '已保存');
}
