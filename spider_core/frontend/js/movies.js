/*
 * movies.js — 数据库集合与影片浏览
 * 集合列表渲染、展开、标签过滤、影片候选磁力表格、复制/下载/删除/自动选择等。
 */

/* ===== 标签过滤辅助 ===== */

function selectedCollectionTags(name) {
    return collectionTagFilters[filterKey(name)] || [];
}

function selectedExcludeTags(name) {
    return collectionExcludeFilters[filterKey(name)] || [];
}

function tagsQuery(name) {
    const tags = selectedCollectionTags(name);
    const excludes = selectedExcludeTags(name);
    let query = '';
    if (tags.length) query += `&tags=${encodeURIComponent(tags.join(','))}`;
    if (excludes.length) query += `&exclude_tags=${encodeURIComponent(excludes.join(','))}`;
    return query;
}

function movieMatchesTags(movie, selectedTags, excludeTags) {
    const movieTags = new Set(movie.tags || []);
    if (excludeTags && excludeTags.length && excludeTags.some(tag => movieTags.has(tag))) return false;
    if (!selectedTags.length) return true;
    return selectedTags.every(tag => movieTags.has(tag));
}

function filterMovies(movies, selectedTags, excludeTags) {
    return (movies || []).filter(movie => movieMatchesTags(movie, selectedTags, excludeTags));
}

/* ===== 健康度统计 ===== */

function collectionHealthCounts(movies) {
    return (movies || []).reduce((counts, movie) => {
        if (movie.magnet_health && counts[movie.magnet_health] !== undefined) {
            counts[movie.magnet_health] += 1;
        }
        return counts;
    }, { active: 0, weak: 0, dead: 0, failed: 0 });
}

function renderHealthCount(value) {
    return value ? String(value) : '-';
}

function renderCollectionHealthTags(movies) {
    const counts = collectionHealthCounts(movies);
    return `<div class="grid h-9 grid-cols-2 grid-rows-2 gap-0.5" aria-label="磁力检测影片统计">
        ${HEALTH_ITEMS.map(item => `<span title="${item.title}" class="badge ${item.badge} min-w-[4ch] px-1 text-[10px]">${renderHealthCount(counts[item.key])}</span>`).join('')}
    </div>`;
}

/* ===== 影片标签渲染与自适应折叠 ===== */

function renderMovieTags(tags) {
    const list = tags || [];
    if (!list.length) return '';
    return `<div class="movie-tags mt-2 flex max-w-full flex-nowrap gap-0.5 overflow-hidden" title="${escapeHtml(list.join(', '))}">
        ${list.map(tag => `<span data-role="tag" class="shrink-0 max-w-[104px] truncate px-1.5 py-0.5 rounded bg-[color:var(--c-neutral-soft)] text-[color:var(--c-neutral-text)] text-[11px]">${escapeHtml(tag)}</span>`).join('')}
        <span data-role="more" class="badge badge-info hidden shrink-0 text-[11px]">+0</span>
    </div>`;
}

function fitMovieTags(root = document) {
    root.querySelectorAll('.movie-tags').forEach(container => {
        const tags = Array.from(container.querySelectorAll('[data-role="tag"]'));
        const more = container.querySelector('[data-role="more"]');
        if (!more) return;
        tags.forEach(tag => tag.classList.remove('hidden'));
        more.classList.add('hidden');
        more.innerText = '+0';
        const available = container.clientWidth;
        if (!available) return;
        let used = 0;
        let visibleCount = 0;
        const gap = 2;
        for (const tag of tags) {
            const nextUsed = used + tag.offsetWidth + (visibleCount ? gap : 0);
            const remaining = tags.length - visibleCount - 1;
            more.innerText = `+${remaining}`;
            let moreWidth = 0;
            if (remaining > 0) {
                more.classList.remove('hidden');
                more.classList.add('invisible');
                moreWidth = more.offsetWidth + gap;
                more.classList.add('hidden');
                more.classList.remove('invisible');
            }
            if (nextUsed + moreWidth > available) break;
            used = nextUsed;
            visibleCount += 1;
        }
        tags.forEach((tag, index) => tag.classList.toggle('hidden', index >= visibleCount));
        const hiddenCount = tags.length - visibleCount;
        if (hiddenCount > 0) {
            more.innerText = `+${hiddenCount}`;
            more.classList.remove('hidden');
        }
    });
}

/* ===== 磁力状态图标 ===== */

function renderMagnetStatus(magnet) {
    const meta = magnetStatusMeta(magnet);
    return `<span title="${escapeHtml(meta.title)}" class="${meta.text}">${meta.icon}</span>`;
}

/* ===== 数据库面包屑路由与集合列表 ===== */

function databaseContent() {
    return document.getElementById('database-content');
}

function databaseBreadcrumb() {
    return document.getElementById('database-breadcrumb');
}

const DATABASE_TYPE_ACTOR = 'actor';
const DATABASE_TYPE_RANKING = 'ranking';
const RANKING_CATEGORIES = [
    { key: 'censored', label: '有码' },
    { key: 'uncensored', label: '无码' },
    { key: 'western', label: '欧美' },
    { key: 'fc2', label: 'FC2' },
];
const RANKING_PERIODS = [
    { key: 'daily', label: '日榜' },
    { key: 'weekly', label: '周榜' },
    { key: 'monthly', label: '月榜' },
];

function databaseTypeLabel(type) {
    if (type === DATABASE_TYPE_ACTOR) return '演员';
    if (type === DATABASE_TYPE_RANKING) return '排行榜';
    return '';
}

function isDatabaseType(type) {
    return [DATABASE_TYPE_ACTOR, DATABASE_TYPE_RANKING].includes(type);
}

function rankingCategoryMeta(key) {
    return RANKING_CATEGORIES.find(item => item.key === key) || null;
}

function rankingPeriodMeta(key) {
    return RANKING_PERIODS.find(item => item.key === key) || null;
}

function databaseRouteParts() {
    const hash = window.location.hash || '#/database';
    const path = hash.replace(/^#\/?/, '');
    if (!path.startsWith('database')) return [];
    return path.split('/').slice(1).map(part => decodeURIComponent(part));
}

function databaseRouteInfo() {
    const parts = databaseRouteParts();
    if (!parts.length) {
        return { type: null, collectionName: null, movieId: null, legacy: false };
    }
    const [first, second, third] = parts;
    if (isDatabaseType(first)) {
        if (first === DATABASE_TYPE_ACTOR) {
            return { type: first, collectionName: second || null, movieId: third || null, legacy: false };
        }
        return { type: first, category: second || null, period: third || null, collectionName: null, movieId: null, legacy: false };
    }
    return { type: DATABASE_TYPE_ACTOR, collectionName: first, movieId: second || null, legacy: true };
}

function currentDatabaseMovieId() {
    return databaseRouteInfo().movieId;
}

function databaseHash() {
    return '#/database';
}

function databaseActorHash(collectionName = null, movieId = null) {
    let hash = `#/database/${DATABASE_TYPE_ACTOR}`;
    if (collectionName) hash += `/${encodeURIComponent(collectionName)}`;
    if (movieId) hash += `/${encodeURIComponent(String(movieId))}`;
    return hash;
}

function databaseTypeHash(type) {
    return `#/database/${encodeURIComponent(type)}`;
}

function databaseRankingHash(category = null, period = null) {
    let hash = `#/database/${DATABASE_TYPE_RANKING}`;
    if (category) hash += `/${encodeURIComponent(category)}`;
    if (period) hash += `/${encodeURIComponent(period)}`;
    return hash;
}

function setDatabaseHash(collectionName = null, movieId = null) {
    const hash = collectionName ? databaseActorHash(collectionName, movieId) : databaseHash();
    if (window.location.hash === hash) {
        renderDatabaseRoute();
    } else {
        window.location.hash = hash;
    }
}

function setDatabaseTypeHash(type) {
    const hash = databaseTypeHash(type);
    if (window.location.hash === hash) {
        renderDatabaseRoute();
    } else {
        window.location.hash = hash;
    }
}

function setRankingHash(category = null, period = null) {
    const hash = databaseRankingHash(category, period);
    if (window.location.hash === hash) {
        renderDatabaseRoute();
    } else {
        window.location.hash = hash;
    }
}

function renderDatabaseBreadcrumb(collectionName = null, movie = null, options = {}) {
    const box = databaseBreadcrumb();
    if (!box) return;
    const type = options.type || (collectionName ? DATABASE_TYPE_ACTOR : null);
    const items = [
        `<button type="button" onclick="setDatabaseHash()" class="font-bold text-[color:var(--c-primary-text)] hover:underline">数据库</button>`
    ];
    if (type) {
        items.push(`<button type="button" onclick="setDatabaseTypeHash('${escapeJs(type)}')" class="font-bold text-[color:var(--c-primary-text)] hover:underline">${databaseTypeLabel(type)}</button>`);
    }
    if (collectionName) {
        items.push(`<button type="button" onclick="setDatabaseHash('${escapeJs(collectionName)}')" class="max-w-[42vw] truncate font-bold text-[color:var(--c-primary-text)] hover:underline">${escapeHtml(displayName(collectionName))}</button>`);
    }
    if (options.rankingCategory) {
        const category = options.rankingCategory;
        items.push(`<button type="button" onclick="setRankingHash('${escapeJs(category.key)}')" class="font-bold text-[color:var(--c-primary-text)] hover:underline">${escapeHtml(category.label)}</button>`);
    }
    if (options.rankingPeriod) {
        items.push(`<span class="font-bold text-slate-700">${escapeHtml(options.rankingPeriod.label)}</span>`);
    }
    if (movie) {
        items.push(`<span class="max-w-[42vw] truncate font-bold text-slate-700">${escapeHtml(movie.code || String(movie.id))}</span>`);
    }
    box.innerHTML = `<div class="flex min-w-0 flex-wrap items-center gap-2">${items.join('<span class="text-slate-300">/</span>')}</div>`;
}

function showDatabaseLoading(label = '加载中...') {
    const content = databaseContent();
    if (!content) return;
    content.innerHTML = `<div class="empty-state flex-1 flex-col gap-3">
        <span class="spinner-ring" aria-hidden="true"></span>
        <span>${escapeHtml(label)}</span>
    </div>`;
}

function setCollectionToolbarVisible(show) {
    const toolbar = document.getElementById('databaseCollectionToolbar');
    if (toolbar) toolbar.classList.toggle('hidden', !show);
}

function renderDatabaseTypePage() {
    expandedCollectionName = null;
    expandedMovieId = null;
    openTagDropdown = null;
    openExcludeDropdown = null;
    openMagnetCheckMenu = null;
    setCollectionToolbarVisible(false);
    hideBatchDeleteControls();
    renderDatabaseBreadcrumb();
    const content = databaseContent();
    if (!content) return;
    const totalCollections = collectionsCache.length;
    const totalMovies = collectionsCache.reduce((sum, item) => sum + Number(item.count || 0), 0);
    content.innerHTML = `
        <div class="shrink-0 border-b border-slate-100 px-5 py-3">
            <div class="text-sm font-bold text-[color:var(--c-text)]">类型</div>
        </div>
        <div class="min-h-0 flex-1 overflow-y-auto p-4">
            <div class="grid gap-3 md:grid-cols-2">
                <button type="button" onclick="setDatabaseTypeHash('${DATABASE_TYPE_RANKING}')" class="group flex min-h-[92px] flex-col items-start justify-between rounded-[var(--radius)] border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-[color:var(--c-primary-soft)]">
                    <span class="text-base font-bold text-slate-800">排行榜</span>
                    <span class="text-xs font-bold text-slate-400 group-hover:text-[color:var(--c-primary-text)]">4 个分类</span>
                </button>
                <button type="button" onclick="setDatabaseTypeHash('${DATABASE_TYPE_ACTOR}')" class="group flex min-h-[92px] flex-col items-start justify-between rounded-[var(--radius)] border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-[color:var(--c-primary-soft)]">
                    <span class="text-base font-bold text-slate-800">演员</span>
                    <span class="text-xs font-bold text-slate-400 group-hover:text-[color:var(--c-primary-text)]">${totalCollections} 个集合 · ${totalMovies} 部影片</span>
                </button>
            </div>
        </div>`;
}

function resetDatabasePageState(options = {}) {
    expandedCollectionName = null;
    expandedMovieId = null;
    openTagDropdown = null;
    openExcludeDropdown = null;
    if (!options.preserveMagnetCheckMenu) openMagnetCheckMenu = null;
    setCollectionToolbarVisible(false);
    hideBatchDeleteControls();
}

function renderRankingCategoryPage() {
    resetDatabasePageState();
    renderDatabaseBreadcrumb(null, null, { type: DATABASE_TYPE_RANKING });
    const content = databaseContent();
    if (!content) return;
    content.innerHTML = `
        <div class="shrink-0 border-b border-slate-100 px-5 py-3">
            <div class="text-sm font-bold text-[color:var(--c-text)]">排行榜分类</div>
        </div>
        <div class="min-h-0 flex-1 overflow-y-auto p-4">
            <div class="grid gap-3 md:grid-cols-2">
                ${RANKING_CATEGORIES.map(category => `
                <button type="button" onclick="setRankingHash('${escapeJs(category.key)}')" class="group flex min-h-[92px] flex-col items-start justify-between rounded-[var(--radius)] border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-[color:var(--c-primary-soft)]">
                    <span class="text-base font-bold text-slate-800">${escapeHtml(category.label)}</span>
                    <span class="text-xs font-bold text-slate-400 group-hover:text-[color:var(--c-primary-text)]">日榜 · 周榜 · 月榜</span>
                </button>`).join('')}
            </div>
        </div>`;
}

function renderRankingPeriodPage(category) {
    resetDatabasePageState();
    renderDatabaseBreadcrumb(null, null, { type: DATABASE_TYPE_RANKING, rankingCategory: category });
    const content = databaseContent();
    if (!content) return;
    content.innerHTML = `
        <div class="shrink-0 border-b border-slate-100 px-5 py-3">
            <div class="text-sm font-bold text-[color:var(--c-text)]">${escapeHtml(category.label)}</div>
        </div>
        <div class="min-h-0 flex-1 overflow-y-auto p-4">
            <div class="grid gap-3 md:grid-cols-3">
                ${RANKING_PERIODS.map(period => `
                <button type="button" onclick="setRankingHash('${escapeJs(category.key)}', '${escapeJs(period.key)}')" class="group flex min-h-[92px] flex-col items-start justify-between rounded-[var(--radius)] border border-[color:var(--c-border)] bg-surface p-4 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-[color:var(--c-primary-soft)]">
                    <span class="text-base font-bold text-slate-800">${escapeHtml(period.label)}</span>
                    <span class="text-xs font-bold text-slate-400 group-hover:text-[color:var(--c-primary-text)]">影片列表</span>
                </button>`).join('')}
            </div>
        </div>`;
}

function notifyRankingFeaturePending(feature) {
    showToast(`${feature}功能后续添加`);
}

function rankingMagnetCheckMenuKey(category, period) {
    return `ranking:${category.key}:${period.key}`;
}

function startRankingMagnetCheck(categoryKey, periodKey, failedOnly = false) {
    openMagnetCheckMenu = null;
    renderDatabaseRoute();
    notifyRankingFeaturePending(failedOnly ? '检测失败磁力' : '磁力检测');
}

function toggleRankingMagnetCheckMenu(categoryKey, periodKey, event = null) {
    if (event) event.stopPropagation();
    const category = rankingCategoryMeta(categoryKey);
    const period = rankingPeriodMeta(periodKey);
    if (!category || !period) return;
    openExclusiveMenu('check', rankingMagnetCheckMenuKey(category, period), () => renderDatabaseRoute());
}

function renderRankingMagnetCheckButton(category, period) {
    const key = rankingMagnetCheckMenuKey(category, period);
    const isOpen = openMagnetCheckMenu === key;
    return `
        <div class="relative shrink-0" data-menu-root="magnet-check">
            <div class="inline-flex">
                <button type="button" onclick="startRankingMagnetCheck('${escapeJs(category.key)}', '${escapeJs(period.key)}')" title="检测磁力" aria-label="检测磁力" class="btn-split-primary h-9 w-9 text-xs shadow-sm">
                    <span class="inline-flex items-center justify-center gap-1">${MAGNET_RADAR_ICON}</span>
                </button>
                <button type="button" onclick="toggleRankingMagnetCheckMenu('${escapeJs(category.key)}', '${escapeJs(period.key)}', event)" title="更多检测选项" aria-label="更多检测选项" class="btn-split-toggle h-9 w-7 text-xs shadow-sm">${isOpen ? '▲' : '▼'}</button>
            </div>
            <div onclick="event.stopPropagation()" class="menu ${isOpen ? '' : 'hidden'} right-0 w-28 text-xs">
                <button type="button" onclick="startRankingMagnetCheck('${escapeJs(category.key)}', '${escapeJs(period.key)}', true)" class="menu-item font-bold text-[color:var(--c-neutral-text)]">check failed</button>
            </div>
        </div>`;
}

function renderRankingToolbarActions(category, period) {
    return `
        <div class="ml-auto flex shrink-0 items-center gap-2">
            <button type="button" onclick="notifyRankingFeaturePending('复制榜单磁力')" title="复制榜单磁力" aria-label="复制榜单磁力" class="btn btn-icon-md btn-info text-sm">⧉</button>
            <button type="button" onclick="notifyRankingFeaturePending('下载榜单 CSV')" title="下载榜单 CSV" aria-label="下载榜单 CSV" class="btn btn-icon-md btn-success text-sm">⇩</button>
            <button type="button" onclick="notifyRankingFeaturePending('更新榜单')" title="更新榜单" aria-label="更新榜单" class="btn btn-icon-md btn-info text-sm">⟳</button>
            ${renderRankingMagnetCheckButton(category, period)}
            <button type="button" onclick="notifyRankingFeaturePending('清空榜单')" title="清空榜单" aria-label="清空榜单" class="btn btn-icon-md btn-danger">
                <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M3 6h18"></path>
                    <path d="M8 6V4h8v2"></path>
                    <path d="M6 6l1 15h10l1-15"></path>
                    <path d="M10 11v6"></path>
                    <path d="M14 11v6"></path>
                </svg>
            </button>
        </div>`;
}

function renderRankingMovieListPage(category, period) {
    resetDatabasePageState({ preserveMagnetCheckMenu: true });
    renderDatabaseBreadcrumb(null, null, { type: DATABASE_TYPE_RANKING, rankingCategory: category, rankingPeriod: period });
    const content = databaseContent();
    if (!content) return;
    content.innerHTML = `
        <div class="shrink-0 border-b border-slate-100 px-5 pb-2 pt-2">
            <div class="text-xs text-slate-500">${escapeHtml(category.label)} · ${escapeHtml(period.label)} · 0 部影片</div>
        </div>
        <div class="flex min-h-0 flex-1 flex-col overflow-hidden px-4 pb-4 pt-3 text-sm text-slate-500">
            <div class="mb-3 flex shrink-0 flex-wrap items-center gap-2">
                <span class="badge badge-info text-[11px]">榜单影片 0/0</span>
                ${renderRankingToolbarActions(category, period)}
            </div>
            <div class="min-h-0 flex-1 max-w-full divide-y divide-slate-100 overflow-y-auto rounded-lg border border-slate-200 bg-white">
                <div class="empty-state px-6 py-10">暂无榜单影片</div>
            </div>
        </div>`;
}

async function loadCollections() {
    const res = await apiFetch('/api/history').then(r => r.json());
    collectionsCache = res.data || [];
    updateDatabaseSummary();
    renderGlobalMagnetCheckButton();
    await renderDatabaseRoute();
}

function updateDatabaseSummary() {
    const summary = document.getElementById('databaseSummary');
    if (!summary) return;
    const totalCollections = collectionsCache.length;
    const totalMovies = collectionsCache.reduce((sum, item) => sum + Number(item.count || 0), 0);
    summary.innerText = `${totalCollections} 个集合 · ${totalMovies} 部影片`;
}

function filteredCollections() {
    const query = collectionSearchQuery.trim().toLowerCase();
    if (!query) return collectionsCache;
    return collectionsCache.filter(item => displayName(item.name).toLowerCase().includes(query) || String(item.name).toLowerCase().includes(query));
}

function updateCollectionSearch() {
    const input = document.getElementById('collectionSearch');
    collectionSearchQuery = input ? input.value : '';
    renderCollections();
}

function renderCollectionListPage() {
    expandedCollectionName = null;
    expandedMovieId = null;
    openTagDropdown = null;
    openExcludeDropdown = null;
    openMagnetCheckMenu = null;
    setCollectionToolbarVisible(true);
    renderDatabaseBreadcrumb(null, null, { type: DATABASE_TYPE_ACTOR });
    const content = databaseContent();
    if (!content) return;
    content.innerHTML = `
        <div class="shrink-0 border-b border-slate-100 px-4 pb-4 pt-3">
            <div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                <input id="collectionSearch" type="search" oninput="updateCollectionSearch()" value="${escapeHtml(collectionSearchQuery)}" class="input md:max-w-sm" placeholder="搜索数据集合">
                <label class="flex items-center gap-2 text-xs font-bold text-slate-600">
                    <input id="selectAllCheckbox" type="checkbox" class="accent-[color:var(--c-primary)]" onclick="toggleSelectAll()">
                    <span id="selectAllLabel">全选当前列表</span>
                </label>
            </div>
        </div>
        <div id="collection-list" class="min-h-0 flex-1 divide-y divide-slate-100 overflow-y-auto"></div>`;
    renderGlobalMagnetCheckButton();
    renderCollections();
}

function hideBatchDeleteControls() {
    const batchBtn = document.getElementById('batchDeleteBtn');
    if (batchBtn) batchBtn.classList.add('hidden');
}

function renderCollections() {
    const list = document.getElementById('collection-list');
    const selectAll = document.getElementById('selectAllCheckbox');
    const selectAllLabel = document.getElementById('selectAllLabel');
    const batchBtn = document.getElementById('batchDeleteBtn');
    updateDatabaseSummary();
    if (!list || !selectAll || !batchBtn) {
        if (batchBtn) batchBtn.classList.add('hidden');
        return;
    }
    if (!collectionsCache.length) {
        list.innerHTML = '<div class="empty-state px-6 py-10">暂无数据库集合</div>';
        selectAll.classList.add('hidden');
        if (selectAllLabel) selectAllLabel.classList.add('hidden');
        batchBtn.classList.add('hidden');
        return;
    }
    const visibleCollections = filteredCollections();
    selectAll.classList.remove('hidden');
    if (selectAllLabel) selectAllLabel.classList.remove('hidden');
    batchBtn.classList.remove('hidden');
    if (!visibleCollections.length) {
        list.innerHTML = '<div class="empty-state px-6 py-10">没有匹配的数据集合</div>';
        updateBatchDeleteBtn();
        return;
    }
    list.innerHTML = visibleCollections.map(item => {
        const name = escapeHtml(item.name);
        const shownName = escapeHtml(displayName(item.name));
        const jsName = escapeJs(item.name);
        return `
        <div class="group flex items-start gap-3 px-4 py-3 hover:bg-slate-50">
            <input type="checkbox" class="collection-checkbox mt-1" value="${name}" onclick="event.stopPropagation(); updateBatchDeleteBtn()">
            <button type="button" onclick="selectCollection('${jsName}')" class="min-w-0 flex-1 text-left">
                <div class="flex min-w-0 items-center justify-between gap-2">
                    <div class="truncate font-bold text-slate-800" title="${shownName}">${shownName}</div>
                    <span class="badge badge-info shrink-0 text-[11px]">${item.count}</span>
                </div>
                <div class="mt-1 truncate text-xs text-slate-400">${escapeHtml(item.time)} · ${((item.tags || []).length)} 个标签</div>
            </button>
        </div>`;
    }).join('');
    updateBatchDeleteBtn();
}

async function ensureCollectionMovies(collectionName, forceReload = false) {
    if (collectionMovieCache[filterKey(collectionName)] && !forceReload) return true;
    const res = await apiFetch(`/api/collections/${encodeURIComponent(collectionName)}/movies`).then(r => r.json());
    if (res.code !== 200) {
        showDatabaseLoading(res.msg || '加载失败');
        return false;
    }
    collectionMovieCache[filterKey(collectionName)] = res.data || { movies: [], available_tags: [], total_count: 0 };
    return true;
}

function collectionItem(collectionName) {
    return collectionsCache.find(item => item.name === collectionName);
}

function collectionData(collectionName) {
    return collectionMovieCache[filterKey(collectionName)] || { movies: [], available_tags: [], total_count: 0 };
}

function movieById(collectionName, movieId) {
    const data = collectionData(collectionName);
    return (data.movies || []).find(movie => String(movie.id) === String(movieId));
}

function renderCollectionToolbar(collectionName) {
    const item = collectionItem(collectionName) || { name: collectionName, count: 0, tags: [], time: '' };
    const jsName = escapeJs(collectionName);
    const incrementalButton = item.has_source_url
        ? `<button onclick="enqueueCollectionIncremental('${jsName}', event)" title="增量爬取此集合" aria-label="增量爬取此集合" class="btn btn-icon-md btn-info text-sm">⟳</button>`
        : `<button type="button" disabled title="缺少原始 URL，无法快捷增量" aria-label="缺少原始 URL，无法快捷增量" class="btn btn-icon-md btn-soft text-sm">⟳</button>`;
    return `
        <div class="shrink-0 border-b border-slate-100 px-5 pb-2 pt-2">
            <div class="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                <div class="min-w-0">
                    <div class="text-xs text-slate-500">${escapeHtml(item.time || '-')} · ${Number(item.count || 0)} 部影片 · ${((item.tags || []).length)} 个标签</div>
                </div>
                <div class="flex flex-wrap gap-2">
                    <button onclick="copyMagnets('${jsName}')" title="复制集合磁力" aria-label="复制集合磁力" class="btn btn-icon-md btn-info text-sm">⧉</button>
                    <button onclick="downloadCsv('${jsName}')" title="下载 CSV" aria-label="下载 CSV" class="btn btn-icon-md btn-success text-sm">⇩</button>
                    ${incrementalButton}
                    ${renderMagnetCheckButton('collection', collectionName)}
                    <button onclick="deleteFiles(['${jsName}'])" title="删除集合" aria-label="删除集合" class="btn btn-icon-md btn-danger">
                        <svg aria-hidden="true" viewBox="0 0 24 24" class="h-4 w-4" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M3 6h18"></path>
                            <path d="M8 6V4h8v2"></path>
                            <path d="M6 6l1 15h10l1-15"></path>
                            <path d="M10 11v6"></path>
                            <path d="M14 11v6"></path>
                        </svg>
                    </button>
                </div>
            </div>
        </div>`;
}

function renderMovieListPage(collectionName) {
    expandedCollectionName = collectionName;
    expandedMovieId = null;
    setCollectionToolbarVisible(false);
    hideBatchDeleteControls();
    renderDatabaseBreadcrumb(collectionName);
    const content = databaseContent();
    if (!content) return;
    content.innerHTML = `
        ${renderCollectionToolbar(collectionName)}
        <div id="collection-body-${escapeHtml(collectionName)}" data-loaded="1" class="flex min-h-0 flex-1 flex-col overflow-hidden px-4 pb-4 pt-3 text-sm text-slate-500"></div>`;
    renderCollectionBody(collectionName);
}

async function renderMagnetListPage(collectionName, movieId) {
    expandedCollectionName = collectionName;
    expandedMovieId = Number(movieId);
    setCollectionToolbarVisible(false);
    hideBatchDeleteControls();
    const movie = movieById(collectionName, movieId);
    if (!movie) {
        setDatabaseHash(collectionName);
        showToast('影片不存在或已被过滤');
        return;
    }
    renderDatabaseBreadcrumb(collectionName, movie);
    const content = databaseContent();
    if (!content) return;
    const movieTitle = movie.title || movie.code || String(movie.id);
    content.innerHTML = `
        <div class="shrink-0 border-b border-slate-100 px-5 pb-4 pt-2">
            <div class="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                <div class="min-w-0">
                    <div class="truncate text-xs text-slate-500" title="${escapeHtml(movieTitle)}">${escapeHtml(movieTitle)}</div>
                    <div class="mt-1 flex min-w-0 items-center gap-2 text-xs text-slate-500">
                        ${renderMagnetCheckButton('movie', movie.id)}
                        <div id="movie-selected-name-${movie.id}" class="min-w-0 truncate" title="${escapeHtml(movie.best_magnet_name || '未选中磁力')}">${escapeHtml(movie.best_magnet_name || '未选中磁力')}</div>
                    </div>
                    ${renderMovieTags(movie.tags || [])}
                </div>
            </div>
        </div>
        <div id="magnets-${movie.id}" class="flex min-h-0 flex-1 flex-col overflow-hidden p-4"></div>`;
    await loadMagnets(movie.id, true);
    fitMovieTags(content);
}

async function renderDatabaseRoute() {
    renderGlobalMagnetCheckButton();
    updateDatabaseSummary();
    const route = databaseRouteInfo();
    if (!route.type) {
        renderDatabaseTypePage();
        return;
    }
    if (route.type === DATABASE_TYPE_RANKING) {
        if (!route.category) {
            renderRankingCategoryPage();
            return;
        }
        const category = rankingCategoryMeta(route.category);
        if (!category) {
            setDatabaseTypeHash(DATABASE_TYPE_RANKING);
            showToast('排行榜分类不存在');
            return;
        }
        if (!route.period) {
            renderRankingPeriodPage(category);
            return;
        }
        const period = rankingPeriodMeta(route.period);
        if (!period) {
            setRankingHash(category.key);
            showToast('榜单周期不存在');
            return;
        }
        renderRankingMovieListPage(category, period);
        return;
    }
    if (!route.collectionName) {
        renderCollectionListPage();
        return;
    }
    setCollectionToolbarVisible(false);
    if (!collectionsCache.length) {
        showDatabaseLoading();
        return;
    }
    if (!collectionItem(route.collectionName)) {
        setDatabaseHash();
        showToast('数据集合不存在或已被删除');
        return;
    }
    if (route.legacy) {
        setDatabaseHash(route.collectionName, route.movieId);
        return;
    }
    showDatabaseLoading();
    if (!(await ensureCollectionMovies(route.collectionName))) return;
    if (route.movieId) {
        await renderMagnetListPage(route.collectionName, route.movieId);
    } else {
        renderMovieListPage(route.collectionName);
    }
}

async function selectCollection(name, _options = {}) {
    setDatabaseHash(name);
}

async function toggleCollection(name) {
    await selectCollection(name);
}

function closeCollectionDetail() {
    setDatabaseHash();
}

function selectMovie(collectionName, movieId) {
    setDatabaseHash(collectionName, movieId);
}

function renderCollectionBody(collectionName) {
    const body = document.getElementById(`collection-body-${collectionName}`);
    if (!body) return;
    const data = collectionMovieCache[filterKey(collectionName)] || { movies: [], available_tags: [], total_count: 0 };
    const selected = selectedCollectionTags(collectionName);
    const excluded = selectedExcludeTags(collectionName);
    const filtered = filterMovies(data.movies, selected, excluded);
    body.innerHTML = renderCollectionFilter(collectionName, data.available_tags || [], filtered.length, data.movies.length, filtered)
        + renderMovies(collectionName, filtered);
    fitMovieTags(body);
}

function renderCollectionFilter(collectionName, availableTags, filteredCount, totalCount, filteredMovies) {
    const selected = selectedCollectionTags(collectionName);
    const excluded = selectedExcludeTags(collectionName);
    const isOpen = openTagDropdown === collectionName;
    const isExcludeOpen = openExcludeDropdown === collectionName;
    return `
        <div class="mb-3 flex shrink-0 flex-wrap items-center gap-2">
            <div class="relative min-w-0" data-menu-root="tag-filter">
                <button type="button" onclick="toggleCollectionTagDropdown('${escapeJs(collectionName)}', event)" class="flex h-9 min-w-[104px] items-center justify-between gap-2 rounded border border-[color:var(--c-border)] bg-surface px-2 text-left text-xs font-bold text-[color:var(--c-neutral-text)] transition-colors hover:bg-[color:var(--c-surface-sunken)]">
                    <span class="min-w-0 truncate">筛选: ${filteredCount}/${totalCount}</span>
                    <span class="shrink-0">${isOpen ? '▲' : '▼'}</span>
                </button>
                <div id="tag-dropdown-${escapeHtml(collectionName)}" onclick="event.stopPropagation()" class="menu ${isOpen ? '' : 'hidden'} w-64 max-h-72 overflow-y-auto">
                    ${renderTagOption(collectionName, 'all', '全部', selected.length === 0)}
                    ${(availableTags || []).map(tag => renderTagOption(collectionName, tag, tag, selected.includes(tag))).join('')}
                </div>
            </div>
            <div class="relative shrink-0" data-menu-root="exclude-filter">
                <button type="button" onclick="toggleExcludeDropdown('${escapeJs(collectionName)}', event)" class="flex h-9 min-w-[72px] items-center justify-between gap-1 rounded border px-2 text-left text-xs font-bold transition-colors ${excluded.length ? 'border-[color:var(--c-danger)] bg-danger-soft text-danger-text' : 'border-[color:var(--c-border)] bg-surface text-[color:var(--c-text-muted)] hover:bg-[color:var(--c-surface-sunken)]'}">
                    <span class="min-w-0 truncate">${excluded.length ? `排除: ${excluded.length}个` : '排除'}</span>
                    <span class="shrink-0">${isExcludeOpen ? '▲' : '▼'}</span>
                </button>
                <div onclick="event.stopPropagation()" class="menu ${isExcludeOpen ? '' : 'hidden'} w-64 max-h-72 overflow-y-auto">
                    <label class="menu-item text-danger-text font-bold">
                        <input type="checkbox" onchange="clearExcludeTags('${escapeJs(collectionName)}')">
                        <span>清除排除</span>
                    </label>
                    ${(availableTags || []).map(tag => renderExcludeOption(collectionName, tag, excluded.includes(tag))).join('')}
                </div>
            </div>
            <div class="flex shrink-0 items-center gap-2">
                ${renderCollectionHealthTags(filteredMovies)}
            </div>
        </div>`;
}

function renderTagOption(collectionName, value, label, checked) {
    return `<label class="menu-item">
        <input type="checkbox" class="accent-[color:var(--c-primary)]" ${checked ? 'checked' : ''} onchange="toggleCollectionTag('${escapeJs(collectionName)}', '${escapeJs(value)}')">
        <span class="truncate" title="${escapeHtml(label)}">${escapeHtml(label)}</span>
    </label>`;
}

function renderExcludeOption(collectionName, tag, checked) {
    return `<label class="menu-item hover:bg-danger-soft">
        <input type="checkbox" ${checked ? 'checked' : ''} onchange="toggleExcludeTag('${escapeJs(collectionName)}', '${escapeJs(tag)}')"
            class="accent-[color:var(--c-danger)]">
        <span class="truncate ${checked ? 'text-danger-text font-bold' : ''}" title="${escapeHtml(tag)}">${escapeHtml(tag)}</span>
    </label>`;
}

function renderMovies(collectionName, movies) {
    if (!movies.length) return '<div class="empty-state">暂无匹配影片记录</div>';
    return `<div class="min-h-0 flex-1 max-w-full divide-y divide-slate-100 overflow-y-auto rounded-lg border border-slate-200 bg-white">${movies.map(movie => `
        <div class="p-3">
            <button type="button" onclick="selectMovie('${escapeJs(collectionName)}', ${movie.id})" class="block w-full min-w-0 text-left">
                <div class="truncate font-bold" title="${escapeHtml(`${movie.code} ${movie.title || ''}`)}"><span>${escapeHtml(movie.code)}</span> <span class="font-normal text-slate-500">${escapeHtml(movie.title || '')}</span></div>
                <div class="mt-1 flex min-w-0 items-center gap-2 text-xs text-slate-500">
                    <span class="badge badge-info shrink-0 whitespace-nowrap">候选 ${movie.candidate_count || 0}</span>
                    <span id="movie-selected-name-${movie.id}" class="min-w-0 truncate" title="${escapeHtml(movie.best_magnet_name || '未选中磁力')}">${escapeHtml(movie.best_magnet_name || '未选中磁力')}</span>
                </div>
                ${renderMovieTags(movie.tags || [])}
            </button>
        </div>
    `).join('')}</div>`;
}

/* ===== 标签下拉交互 ===== */

function toggleCollectionTagDropdown(collectionName, event = null) {
    if (event) event.stopPropagation();
    openExclusiveMenu('tag', collectionName, () => renderCollectionBody(collectionName));
}

function toggleExcludeDropdown(collectionName, event = null) {
    if (event) event.stopPropagation();
    openExclusiveMenu('exclude', collectionName, () => renderCollectionBody(collectionName));
}

function toggleExcludeTag(collectionName, value) {
    const key = filterKey(collectionName);
    const current = new Set(selectedExcludeTags(collectionName));
    if (current.has(value)) current.delete(value);
    else current.add(value);
    collectionExcludeFilters[key] = Array.from(current);
    expandedMovieId = null;
    renderCollectionBody(collectionName);
}

function clearExcludeTags(collectionName) {
    collectionExcludeFilters[filterKey(collectionName)] = [];
    expandedMovieId = null;
    renderCollectionBody(collectionName);
}

function toggleCollectionTag(collectionName, value) {
    const key = filterKey(collectionName);
    if (value === 'all') {
        collectionTagFilters[key] = [];
    } else {
        const current = new Set(selectedCollectionTags(collectionName));
        if (current.has(value)) current.delete(value);
        else current.add(value);
        collectionTagFilters[key] = Array.from(current);
    }
    expandedMovieId = null;
    renderCollectionBody(collectionName);
}

/* ===== 候选磁力表格 ===== */

async function loadMagnets(movieId, keepOpen = false) {
    const box = document.getElementById(`magnets-${movieId}`);
    if (!box) return [];
    if (keepOpen) box.classList.remove('hidden');
    const res = await apiFetch(`/api/movies/${movieId}/magnets`).then(r => r.json());
    const magnets = res.data || [];
    box.innerHTML = magnets.length ? renderMagnetTable(movieId, magnets) : '<div class="empty-state flex-1">暂无候选磁力</div>';
    return magnets;
}

function renderMagnetTable(movieId, magnets) {
    return `
        <div class="min-h-0 flex-1 overflow-auto rounded-lg border border-slate-200">
            <table class="w-full table-fixed text-xs">
                <colgroup>
                    <col class="w-14"><col><col class="w-10"><col class="w-16"><col class="w-16">
                </colgroup>
                <thead class="sticky top-0 border-b border-slate-200 bg-slate-50 text-slate-500"><tr><th class="p-2 text-center whitespace-nowrap font-bold">状态</th><th class="p-2 text-left font-bold">文件名</th><th class="p-2 text-center font-bold">分数</th><th class="p-2 text-center font-bold">大小</th><th class="p-2 text-center font-bold">操作</th></tr></thead>
                <tbody>${magnets.map(magnet => renderMagnetRow(movieId, magnet)).join('')}</tbody>
            </table>
        </div>`;
}

function magnetRowSignature(magnet) {
    return [
        magnet.id,
        magnet.is_selected ? 1 : 0,
        magnet.priority_score,
        magnet.check_status || '',
        magnet.seeders ?? 0,
        magnet.leechers ?? 0,
        magnet.checked_at || '',
        magnet.check_error || ''
    ].join('|');
}

function renderMagnetRow(movieId, magnet) {
    const signature = escapeHtml(magnetRowSignature(magnet));
    return `
        <tr id="magnet-row-${magnet.id}" data-signature="${signature}" class="border-t border-slate-100 transition-colors ${magnet.is_selected ? 'bg-success-soft' : 'hover:bg-slate-50'}">
            <td class="p-2 text-center align-middle ${magnet.is_selected ? 'border-l-2 border-[color:var(--c-success)]' : ''}">
                <div>${renderMagnetStatus(magnet)}</div>
                <div class="mt-1 text-[10px] leading-none text-slate-400">${magnet.checked_at ? `${magnet.seeders ?? 0}/${magnet.leechers ?? 0}` : '-/-'}</div>
            </td>
            <td class="min-w-0 cursor-pointer p-2" title="${escapeHtml(magnet.link)}" onclick="${magnet.is_selected ? '' : `selectMagnet(${movieId}, ${magnet.id})`}">
                <div class="truncate">${magnet.is_selected ? '<span class="mr-1 text-success-text">✓</span>' : ''}${escapeHtml(magnet.name)}</div>
                <div class="mt-1 inline-flex max-w-full rounded bg-[color:var(--c-neutral-soft)] px-1.5 py-0.5 text-[10px] leading-none text-[color:var(--c-text-muted)]">${escapeHtml(magnet.magnet_date || '-')}</div>
            </td>
            <td class="p-2 text-center align-middle">${magnet.priority_score}</td>
            <td class="p-2 text-center align-middle whitespace-nowrap">${formatGb(magnet.size_mb)}</td>
            <td class="p-2 text-center align-middle">
                <div class="flex justify-center gap-1">
                    <button onclick="copyTextWithToast('${escapeJs(magnet.link)}', '已复制磁力链接')" title="复制磁力链接" aria-label="复制磁力链接" class="btn btn-icon-sm btn-info">
                        <span class="text-sm leading-none">⧉</span>
                    </button>
                </div>
            </td>
        </tr>`;
}

async function refreshMagnetRows(movieId) {
    const box = document.getElementById(`magnets-${movieId}`);
    if (!box || box.classList.contains('hidden')) return [];
    const res = await apiFetch(`/api/movies/${movieId}/magnets`).then(r => r.json());
    const magnets = res.data || [];
    if (!box.querySelector('tbody')) {
        box.innerHTML = magnets.length ? renderMagnetTable(movieId, magnets) : '<div class="empty-state flex-1">暂无候选磁力</div>';
        return magnets;
    }
    for (const magnet of magnets) {
        const row = document.getElementById(`magnet-row-${magnet.id}`);
        const signature = magnetRowSignature(magnet);
        if (!row) {
            await loadMagnets(movieId, true);
            return magnets;
        }
        if (row.dataset.signature !== signature) {
            row.outerHTML = renderMagnetRow(movieId, magnet);
        }
    }
    return magnets;
}

function syncSelectedMagnetToMovie(movieId, magnets) {
    if (!expandedCollectionName) return false;
    const data = collectionMovieCache[filterKey(expandedCollectionName)];
    const selected = (magnets || []).find(magnet => magnet.is_selected);
    if (!data || !selected) return false;
    const movie = (data.movies || []).find(item => Number(item.id) === Number(movieId));
    if (!movie) return false;
    movie.best_magnet_name = selected.name || '';
    movie.best_magnet_link = selected.link || '';
    movie.priority_score = selected.priority_score || 0;
    movie.magnet_date = selected.magnet_date || '';
    movie.size_mb = selected.size_mb || 0;
    return true;
}

function updateMovieSelectedName(movieId, magnets) {
    const selected = (magnets || []).find(magnet => magnet.is_selected);
    const target = document.getElementById(`movie-selected-name-${movieId}`);
    if (!selected || !target) return;
    const name = selected.name || '未选中磁力';
    target.innerText = name;
    target.title = name;
}

async function toggleMagnets(movieId) {
    if (!expandedCollectionName) return;
    selectMovie(expandedCollectionName, movieId);
}

async function selectMagnet(movieId, magnetId) {
    const res = await apiFetch(`/api/movies/${movieId}/select_magnet`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ magnet_id: magnetId })
    }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '更新失败');
    const magnets = await loadMagnets(movieId, true);
    if (syncSelectedMagnetToMovie(movieId, magnets)) {
        updateMovieSelectedName(movieId, magnets);
    }
}

async function reloadCollectionMovies(collectionName) {
    const res = await apiFetch(`/api/collections/${encodeURIComponent(collectionName)}/movies`).then(r => r.json());
    if (res.code !== 200) return;
    const previousMovieId = expandedMovieId;
    collectionMovieCache[filterKey(collectionName)] = res.data || { movies: [], available_tags: [], total_count: 0 };
    if (previousMovieId && movieById(collectionName, previousMovieId)) {
        await renderMagnetListPage(collectionName, previousMovieId);
    } else if (expandedCollectionName === collectionName) {
        renderMovieListPage(collectionName);
    }
}

/* ===== 复制 / 下载 / 删除 / 自动选择 ===== */

async function downloadCsv(name) {
    const response = await apiFetch(`/api/download?name=${encodeURIComponent(name)}${tagsQuery(name)}`);
    if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        return showToast(data.msg || '下载失败');
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = name;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}

async function copyMagnets(name) {
    try {
        const res = await apiFetch(`/api/magnets?name=${encodeURIComponent(name)}${tagsQuery(name)}`).then(r => r.json());
        if (res.code !== 200) return showToast(res.msg || '读取失败');
        const links = res.data || [];
        if (!links.length) return showToast('暂无磁力链接可复制');
        const copied = await copyText(links.join('\n'));
        showToast(copied ? `已复制 ${links.length} 条磁力链接` : '自动复制失败，请在弹窗中手动复制磁力链接');
    } catch (err) {
        console.error(err);
        showToast(err.message || '复制失败');
    }
}

function toggleSelectAll() {
    const checked = document.getElementById('selectAllCheckbox').checked;
    document.querySelectorAll('.collection-checkbox').forEach(cb => cb.checked = checked);
    updateBatchDeleteBtn();
}

function updateBatchDeleteBtn() {
    const checked = document.querySelectorAll('.collection-checkbox:checked').length;
    const btn = document.getElementById('batchDeleteBtn');
    const count = document.getElementById('batchDeleteCount');
    btn.disabled = checked === 0;
    btn.setAttribute('aria-label', `批量删除 ${checked} 个数据集合`);
    if (count) count.textContent = checked;
}

function batchDelete() {
    const names = Array.from(document.querySelectorAll('.collection-checkbox:checked')).map(cb => cb.value);
    if (names.length) deleteFiles(names);
}

async function autoSelectMagnets() {
    const checkedNames = Array.from(document.querySelectorAll('.collection-checkbox:checked')).map(cb => cb.value);
    const names = checkedNames.length ? checkedNames : collectionsCache.map(item => item.name);
    if (!names.length) return showToast('暂无可自动选择的集合');
    const scopeText = checkedNames.length ? `${checkedNames.length} 个已选集合` : '全部集合';
    if (!confirm(`按评分自动选择 ${scopeText} 的推荐磁力？`)) return;
    const res = await apiFetch('/api/magnets/auto_select', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filenames: names })
    }).then(r => r.json());
    showToast(res.msg || '自动选择完成');
    await loadCollections();
}

async function enqueueCollectionIncremental(name, event) {
    if (event) event.stopPropagation();
    if (!confirm(`确定对「${displayName(name)}」执行增量爬取吗？`)) return;
    const res = await apiFetch(`/api/collections/${encodeURIComponent(name)}/incremental_task`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '添加增量任务失败');
    showToast(res.msg || '任务已加入队列');
    await refreshMonitor();
}

async function deleteFiles(names) {
    if (!confirm(`确定删除 ${names.length} 个数据库集合吗？`)) return;
    const res = await apiFetch('/api/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filenames: names })
    }).then(r => r.json());
    showToast(res.msg);
    await loadCollections();
}
