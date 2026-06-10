/*
 * utils.js — 通用工具函数
 * 转义、格式化、剪贴板复制、Toast 提示。
 * 以普通脚本加载（非 ES Module），所有函数挂在全局作用域，
 * 供 index.html 内联 onclick 与其它模块直接调用。
 */

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

function escapeJs(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'");
}

function displayName(value) {
    return String(value || '').replace(/\.csv$/i, '');
}

function formatGb(sizeMb) {
    return `${(Number(sizeMb || 0) / 1024).toFixed(1)} GB`;
}

function filterKey(name) {
    return String(name || '');
}

/*
 * Toast 提示：替代原先阻塞 UI 的 window.alert()。
 * 非阻塞、自动消失，不会冻结轮询线程。
 */
function showToast(message, type = 'info') {
    const text = String(message ?? '');
    const container = document.getElementById('toast-container');
    if (!container) {
        // 容器缺失时兜底，至少不丢失信息
        console.log(`[toast:${type}] ${text}`);
        return;
    }
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = text;
    container.appendChild(toast);
    // 进场动画
    requestAnimationFrame(() => toast.classList.add('toast-show'));
    // 3 秒后退场并移除
    setTimeout(() => {
        toast.classList.remove('toast-show');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

async function copyText(text) {
    const value = String(text || '');
    if (navigator.clipboard && window.isSecureContext) {
        try {
            await navigator.clipboard.writeText(value);
            return true;
        } catch (err) {
            console.warn('Clipboard API failed, falling back to textarea copy.', err);
        }
    }
    const area = document.createElement('textarea');
    area.value = value;
    area.setAttribute('readonly', '');
    area.style.position = 'fixed';
    area.style.top = '0';
    area.style.left = '-9999px';
    document.body.appendChild(area);
    area.focus();
    area.select();
    area.setSelectionRange(0, area.value.length);
    let copied = false;
    try {
        copied = document.execCommand('copy');
    } finally {
        document.body.removeChild(area);
    }
    if (!copied) {
        window.prompt('自动复制失败，请手动复制：', value);
    }
    return copied;
}

/* 复制并给出 Toast 反馈（用于单条磁力等需要即时提示的场景）。 */
async function copyTextWithToast(text, okMsg = '已复制') {
    const copied = await copyText(text);
    showToast(copied ? okMsg : '自动复制失败，请手动复制');
    return copied;
}
