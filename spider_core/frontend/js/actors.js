/*
 * actors.js — 收藏演员页
 * 读取本地缓存的收藏演员清单（进入页面不自动请求远端），手动刷新分类快照，
 * 每行可展开选择标签并一键加入任务队列（复用 /api/get_tags 与任务队列）。
 */

const DEFAULT_ACTOR_CATEGORIES = [
    { key: 'all', label: '全部' },
    { key: 'g0t0', label: '有码女优' },
    { key: 'g1t0', label: '有码男优' },
    { key: 'g0t1', label: '无码演员' },
    { key: 'g0t2', label: '欧美女优' },
    { key: 'g1t2', label: '欧美男优' },
];

let actorsData = { categories: DEFAULT_ACTOR_CATEGORIES, actors: [] };
let actorCategory = 'all';
let actorsLoaded = false;
let expandedActorId = null;
const actorTagState = {}; // actorId -> { loading, tags:[{name,value}], selected:Set, error }
let actorsLayoutFrame = null;

function actorRefreshLabel() {
    return actorCategory === 'all' ? '刷新全部' : '刷新当前分类';
}

function actorRefreshIcon(loading) {
    const spin = loading ? ' class="animate-spin"' : '';
    return `
        <svg${spin} width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M3 16v5h5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            <path d="M21 3v5h-5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>
        <span class="sr-only">${loading ? '刷新中' : actorRefreshLabel()}</span>`;
}

function setActorRefreshButton(loading) {
    const btn = document.getElementById('actorRefreshBtn');
    if (!btn) return;
    const label = loading ? '刷新中' : actorRefreshLabel();
    btn.disabled = !!loading;
    btn.title = label;
    btn.setAttribute('aria-label', label);
    btn.innerHTML = actorRefreshIcon(loading);
}

function scheduleFitActorsLayout() {
    if (actorsLayoutFrame) return;
    actorsLayoutFrame = requestAnimationFrame(() => {
        actorsLayoutFrame = null;
        fitActorsLayout();
    });
}

function fitActorsLayout() {
    const view = document.getElementById('view-actors');
    const card = view ? view.querySelector('.card') : null;
    if (!view || !card || view.classList.contains('hidden')) return;

    const rect = view.getBoundingClientRect();
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    const documentTop = rect.top + (window.scrollY || window.pageYOffset || 0);
    const bodyStyle = window.getComputedStyle(document.body);
    const bodyBottomPadding = parseFloat(bodyStyle.paddingBottom) || 0;
    const guard = 8;
    const available = Math.floor(viewportHeight - documentTop - bodyBottomPadding - guard);
    const height = Math.max(240, available);

    view.style.height = `${height}px`;
    view.style.overflow = 'hidden';
    card.style.height = '100%';
}

/* ===== 视图入口与数据加载 ===== */

function renderActorsView() {
    if (!actorsLoaded) {
        loadActors();
        return;
    }
    renderActorTabs();
    renderActorList();
    renderActorRefreshInfo();
    scheduleFitActorsLayout();
}

async function loadActors() {
    try {
        const res = await apiFetch('/api/actors').then(r => r.json());
        if (res.code === 200 && res.data) {
            actorsData = normalizeActorsData(res.data);
            actorsLoaded = true;
        }
    } catch (err) {
        console.error('加载收藏演员失败:', err);
    }
    renderActorRefreshBanner(null);
    renderActorTabs();
    renderActorList();
    renderActorRefreshInfo();
    scheduleFitActorsLayout();
}

function normalizeActorsData(data) {
    return {
        categories: (data.categories && data.categories.length) ? data.categories : DEFAULT_ACTOR_CATEGORIES,
        actors: data.actors || [],
    };
}

async function refreshActors() {
    setActorRefreshButton(true);
    try {
        const resp = await apiFetch('/api/actors/refresh', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ category: actorCategory }),
        });
        const res = await resp.json().catch(() => ({}));
        if (!resp.ok || res.code !== 200) {
            // 远端失败时保留本地已有清单，仅提示（PRD §12.1）。
            showToast(res.msg || `刷新失败 (${resp.status})`);
            return;
        }
        actorsData = normalizeActorsData(res.data || {});
        actorsLoaded = true;
        const failed = (res.data && res.data.failed) || [];
        renderActorRefreshBanner(failed);
        renderActorTabs();
        renderActorList();
        renderActorRefreshInfo();
        if (failed.length) showToast(`部分分类刷新失败：${failed.map(f => f.label || f.category).join('、')}`);
        else showToast('刷新完成');
    } catch (err) {
        showToast(err.message || '刷新失败');
    } finally {
        setActorRefreshButton(false);
    }
}

/* ===== 分类标签栏 ===== */

function renderActorTabs() {
    const box = document.getElementById('actorCategoryTabs');
    if (!box) return;
    const cats = (actorsData.categories && actorsData.categories.length) ? actorsData.categories : DEFAULT_ACTOR_CATEGORIES;
    box.innerHTML = `
        <label for="actorCategorySelect" class="sr-only">收藏演员分类</label>
        <select id="actorCategorySelect" onchange="selectActorCategory(this.value)" class="h-9 w-full rounded border border-[color:var(--c-border)] bg-surface px-3 text-sm font-semibold text-[color:var(--c-text)] focus:border-primary focus:outline-none focus:ring-2 focus:ring-[color:var(--c-primary-ring)]">
            ${cats.map(cat => {
        const count = cat.key === 'all'
            ? actorsData.actors.length
            : actorsData.actors.filter(a => a.category === cat.key).length;
        const selected = cat.key === actorCategory ? ' selected' : '';
        return `<option value="${escapeHtml(cat.key)}"${selected}>${escapeHtml(cat.label)} ${count}</option>`;
    }).join('')}
        </select>`;
    setActorRefreshButton(false);
}

function selectActorCategory(key) {
    actorCategory = key;
    expandedActorId = null;
    renderActorTabs();
    renderActorList();
    renderActorRefreshInfo();
}

function actorCategoryLabel(key) {
    const cat = (actorsData.categories || []).find(c => c.key === key);
    return cat ? cat.label : key;
}

function filteredActors() {
    const actors = actorsData.actors || [];
    return actorCategory === 'all' ? actors : actors.filter(a => a.category === actorCategory);
}

function formatActorTime(ts) {
    if (!ts) return '—';
    try {
        return new Date(ts * 1000).toLocaleString();
    } catch {
        return '—';
    }
}

function renderActorRefreshInfo() {
    const el = document.getElementById('actorRefreshInfo');
    if (!el) return;
    const actors = filteredActors();
    if (!actorsLoaded) { el.innerText = '尚未刷新'; return; }
    if (!actors.length) { el.innerText = '该分类暂无收藏演员'; return; }
    const latest = actors.reduce((m, a) => Math.max(m, a.refreshed_at || 0), 0);
    el.innerText = formatActorTime(latest);
}

function renderActorRefreshBanner(failed) {
    const box = document.getElementById('actorRefreshBanner');
    if (!box) return;
    if (!failed || !failed.length) {
        box.classList.add('hidden');
        box.innerHTML = '';
        scheduleFitActorsLayout();
        return;
    }
    box.classList.remove('hidden');
    box.innerHTML = `刷新失败的分类：${failed.map(f => `${escapeHtml(f.label || f.category)}（${escapeHtml(f.msg || '')}）`).join('；')}`;
    scheduleFitActorsLayout();
}

/* ===== 列表与行渲染 ===== */

function renderActorList() {
    const box = document.getElementById('actor-list');
    if (!box) return;
    const actors = filteredActors();
    if (!actors.length) {
        box.innerHTML = `<div class="empty-state flex-1">${actorsLoaded ? '该分类暂无收藏演员，点击刷新获取' : '点击刷新按钮从 JavDB 获取收藏演员'}</div>`;
        scheduleFitActorsLayout();
        return;
    }
    box.innerHTML = actors.map(actorRow).join('');
    scheduleFitActorsLayout();
}

function actorRow(actor) {
    const id = escapeJs(actor.actor_id);
    const expanded = actor.actor_id === expandedActorId;
    const dbMarker = actor.has_collection
        ? `<button type="button" onclick="location.hash = databaseActorHash('${escapeJs(actor.collection_filename)}')" title="已存在数据集合，点击查看" aria-label="已入库，点击查看数据集合" class="mr-1 inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full bg-success-soft text-[11px] font-bold leading-none text-success-text">✓</button>`
        : '';
    const lastTags = (actor.last_task_tags || [])
        .map(t => `<span class="badge badge-info">${escapeHtml(t.name || t.value)}</span>`)
        .join(' ');
    return `
    <div class="px-4 py-2 text-sm">
        <div class="flex items-start gap-2">
            <div class="min-w-0 flex-1">
                <div class="flex flex-wrap items-center gap-2">
                    <span class="inline-flex min-w-0 items-center font-bold" title="${escapeHtml(actor.actor_name)}">${dbMarker}<span class="truncate">${escapeHtml(actor.actor_name)}</span></span>
                    <span class="badge badge-neutral shrink-0">${escapeHtml(actorCategoryLabel(actor.category))}</span>
                </div>
                ${lastTags ? `<div class="mt-1 flex flex-wrap gap-1">${lastTags}</div>` : ''}
            </div>
            <div class="flex shrink-0 items-center gap-1">
                <button type="button" onclick="toggleActorTags('${id}')" class="btn btn-sm btn-soft">${expanded ? '收起' : '标签'}</button>
                <button type="button" onclick="addActorTask('${id}')" class="btn btn-icon-sm btn-primary" title="加入队列" aria-label="加入队列" ${(actorTagState[actor.actor_id] || {}).loading ? 'disabled' : ''}>
                    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                        <path d="M8 6h8M8 12h5M8 18h8" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                        <path d="M18 9v6M15 12h6" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
                    </svg>
                    <span class="sr-only">加入队列</span>
                </button>
            </div>
        </div>
        ${expanded ? renderActorTagPanel(actor) : ''}
    </div>`;
}

function renderActorTagPanel(actor) {
    const id = escapeJs(actor.actor_id);
    const st = actorTagState[actor.actor_id] || {};
    let body;
    if (st.loading) {
        body = '<div class="text-xs text-[color:var(--c-text-muted)]">加载标签中...</div>';
    } else if (st.error) {
        body = `<div class="text-xs text-danger-text">${escapeHtml(st.error)}</div>`;
    } else {
        const tags = st.tags || [];
        const selected = st.selected || new Set();
        const tagButtons = tags.length
            ? tags.map(t => {
                const on = selected.has(t.value);
                return `<button type="button" onclick="toggleActorTag('${id}','${escapeJs(t.value)}')" class="px-2.5 py-1 rounded text-xs border transition-colors ${on ? 'bg-primary text-white border-primary' : 'bg-surface text-[color:var(--c-text-muted)] border-[color:var(--c-border)] hover:bg-[color:var(--c-surface-sunken)]'}">${escapeHtml(t.name)}</button>`;
            }).join('')
            : '<span class="text-xs text-[color:var(--c-text-subtle)]">该演员页无可选标签，可直接加入任务</span>';
        body = `<div class="max-h-[130px] overflow-y-auto overscroll-contain flex flex-wrap gap-2">${tagButtons}</div>`;
    }
    return `
    <div class="mt-2 rounded-lg border border-[color:var(--c-primary-soft)] p-3">
        ${body}
    </div>`;
}

/* ===== 标签展开与加入任务 ===== */

async function toggleActorTags(actorId) {
    if (expandedActorId === actorId) {
        expandedActorId = null;
        renderActorList();
        return;
    }
    expandedActorId = actorId;
    renderActorList();
    const st = actorTagState[actorId];
    if (!st || (!st.tags && !st.loading && !st.error)) {
        await loadActorTags(actorId);
    }
}

async function loadActorTags(actorId) {
    const actor = (actorsData.actors || []).find(a => a.actor_id === actorId);
    if (!actor) return;
    const prevSelected = actorTagState[actorId] && actorTagState[actorId].selected;
    actorTagState[actorId] = { loading: true, selected: prevSelected || new Set() };
    if (expandedActorId === actorId) renderActorList();
    try {
        const resp = await apiFetch('/api/get_tags', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url: actor.actor_url }),
        });
        if (resp.status === 404) {
            // 演员页无标签区域：按“无可选标签”处理，仍允许直接加入任务。
            actorTagState[actorId] = { tags: [], selected: new Set() };
        } else {
            const res = await resp.json().catch(() => ({}));
            if (!resp.ok || res.code !== 200) {
                actorTagState[actorId] = { tags: [], selected: new Set(), error: res.msg || '获取标签失败' };
            } else {
                // 预选上次使用的标签（PRD §8.6 复用）。
                const lastValues = new Set((actor.last_task_tags || []).map(t => t.value));
                actorTagState[actorId] = { tags: res.data || [], selected: lastValues };
            }
        }
    } catch (err) {
        actorTagState[actorId] = { tags: [], selected: new Set(), error: err.message || '获取标签失败' };
    }
    if (expandedActorId === actorId) renderActorList();
}

function toggleActorTag(actorId, value) {
    const st = actorTagState[actorId];
    if (!st) return;
    if (!st.selected) st.selected = new Set();
    if (st.selected.has(value)) st.selected.delete(value);
    else st.selected.add(value);
    renderActorList();
}

async function addActorTask(actorId, crawlMode = '') {
    const actor = (actorsData.actors || []).find(a => a.actor_id === actorId);
    if (!actor) return;
    const st = actorTagState[actorId] || {};
    const selected = st.selected || new Set();
    const tags = Array.isArray(st.tags) && !st.error
        ? st.tags.filter(t => selected.has(t.value))
        : (actor.last_task_tags || []);
    try {
        const resp = await apiFetch('/api/actors/add_task', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ actor_id: actorId, tags, crawl_mode: crawlMode }),
        });
        const res = await resp.json().catch(() => ({}));
        if (resp.status === 409 && res.needs_mode) {
            const useIncremental = confirm(`检测到已有数据库集合：${displayName(res.filename)}\n点击“确定”使用增量，点击“取消”使用覆盖。`);
            return addActorTask(actorId, useIncremental ? 'incremental' : 'overwrite');
        }
        if (!resp.ok || res.code !== 200) {
            // 失败不更新最后一次标签记录（PRD §8.8）。
            showToast(res.msg || '添加任务失败');
            return;
        }
        const data = res.data || {};
        actor.last_task_tags = data.tags || tags;
        showToast(res.msg || '任务已加入队列');
        renderActorList();
    } catch (err) {
        showToast(err.message || '添加任务失败');
    }
}
