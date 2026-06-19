/*
 * api.js — 统一 API 客户端
 * 封装鉴权头注入与 401 统一处理，所有后端请求都经过 apiFetch。
 */

function apiHeaders(extraHeaders = {}) {
    const headers = { ...extraHeaders };
    if (apiToken) headers['X-JavDB-Token'] = apiToken;
    return headers;
}

async function apiFetch(url, options = {}) {
    const response = await fetch(url, { ...options, headers: apiHeaders(options.headers || {}) });
    if (response.status === 401 && authRequired) {
        lockAppForAuth();
        throw new Error('访问令牌缺失或无效');
    }
    return response;
}

async function apiFetchJson(url, options = {}) {
    const response = await apiFetch(url, options);
    if (!response.ok) {
        let body;
        try {
            body = await response.json();
        } catch {
            body = {};
        }
        const msg = body.msg || body.message || `请求失败 (${response.status})`;
        throw Object.assign(new Error(msg), { status: response.status, body });
    }
    return response.json();
}

async function apiPost(url, body = null) {
    const options = { method: 'POST' };
    if (body !== null && body !== undefined) {
        options.headers = { 'Content-Type': 'application/json' };
        options.body = JSON.stringify(body);
    }
    return apiFetchJson(url, options);
}

async function apiDownloadBlob(url, filename) {
    const response = await apiFetch(url);
    if (!response.ok) {
        throw new Error(`下载失败 (${response.status})`);
    }
    const blob = await response.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    URL.revokeObjectURL(a.href);
    a.remove();
}
