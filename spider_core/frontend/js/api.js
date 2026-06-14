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
