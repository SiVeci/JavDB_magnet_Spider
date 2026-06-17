/*
 * movies.js — 数据库集合与影片浏览
 * 集合列表渲染、展开、标签过滤、影片候选磁力表格、复制/下载/删除/自动选择等。
 */

/* ===== 标签过滤辅助 ===== */

function selectedTagsForFilterKey(key) {
    return collectionTagFilters[filterKey(key)] || [];
}

function selectedExcludeTagsForFilterKey(key) {
    return collectionExcludeFilters[filterKey(key)] || [];
}

function selectedCollectionTags(name) {
    return selectedTagsForFilterKey(name);
}

function selectedExcludeTags(name) {
    return selectedExcludeTagsForFilterKey(name);
}

function tagsQueryFromFilters(tags, excludes, prefix = '&') {
    const parts = [];
    if (tags.length) parts.push(`tags=${encodeURIComponent(tags.join(','))}`);
    if (excludes.length) parts.push(`exclude_tags=${encodeURIComponent(excludes.join(','))}`);
    return parts.length ? `${prefix}${parts.join('&')}` : '';
}

function tagsQuery(name) {
    return tagsQueryFromFilters(selectedCollectionTags(name), selectedExcludeTags(name));
}

function rankingFilterKey(category, period) {
    const categoryKey = typeof category === 'string' ? category : (category && category.key) || '';
    const periodKey = typeof period === 'string' ? period : (period && period.key) || '';
    return `ranking:${categoryKey}:${periodKey}`;
}

function selectedRankingTags(category, period) {
    return rankingTagFilters[filterKey(rankingFilterKey(category, period))] || [];
}

function selectedRankingExcludeTags(category, period) {
    return rankingExcludeFilters[filterKey(rankingFilterKey(category, period))] || [];
}

function rankingTagsQuery(categoryKey, periodKey) {
    return tagsQueryFromFilters(selectedRankingTags(categoryKey, periodKey), selectedRankingExcludeTags(categoryKey, periodKey), '?');
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

function renderRankingHealthTags(movies) {
    const counts = collectionHealthCounts(movies);
    return `<div class="flex shrink-0 items-center gap-1" aria-label="磁力检测影片统计">
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
    { key: 'playback', label: '热播' },
    { key: 'top250', label: 'TOP250', dynamicOptions: true, subLabel: '动态分类' },
];
const RANKING_PERIODS = [
    { key: 'daily', label: '日榜' },
    { key: 'weekly', label: '周榜' },
    { key: 'monthly', label: '月榜' },
];
let top250OptionCache = null;

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

function top250OptionFallbackLabel(key) {
    if (key === 'all') return '全部';
    const labels = { '0': '有码', '1': '无码', '2': '欧美', '3': 'FC2' };
    if (labels[key]) return labels[key];
    if (/^y\d{4}$/.test(key || '')) return key.slice(1);
    return key || '全部';
}

function isValidTop250OptionKey(key) {
    return /^[A-Za-z0-9_-]{1,32}$/.test(key || '');
}

function rankingPeriodMetaForCategory(category, key) {
    if (!category) return null;
    if (category.dynamicOptions) {
        const option = (top250OptionCache || []).find(item => item.key === key);
        if (option) return option;
        return isValidTop250OptionKey(key) ? { key, label: top250OptionFallbackLabel(key) } : null;
    }
    return rankingPeriodMeta(key);
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
    const [first, second, third, fourth] = parts;
    if (isDatabaseType(first)) {
        if (first === DATABASE_TYPE_ACTOR) {
            return { type: first, collectionName: second || null, movieId: third || null, legacy: false };
        }
        return { type: first, category: second || null, period: third || null, collectionName: null, movieId: fourth || null, legacy: false };
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

function databaseRankingHash(category = null, period = null, movieId = null) {
    let hash = `#/database/${DATABASE_TYPE_RANKING}`;
    if (category) hash += `/${encodeURIComponent(category)}`;
    if (period) hash += `/${encodeURIComponent(period)}`;
    if (movieId) hash += `/${encodeURIComponent(String(movieId))}`;
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

function setRankingHash(category = null, period = null, movieId = null) {
    const hash = databaseRankingHash(category, period, movieId);
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
        const period = options.rankingPeriod;
        if (options.rankingCategory) {
            const category = options.rankingCategory;
            items.push(`<button type="button" onclick="setRankingHash('${escapeJs(category.key)}', '${escapeJs(period.key)}')" class="font-bold text-[color:var(--c-primary-text)] hover:underline">${escapeHtml(period.label)}</button>`);
        } else {
            items.push(`<span class="font-bold text-slate-700">${escapeHtml(period.label)}</span>`);
        }
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
                    <span class="text-xs font-bold text-slate-400 group-hover:text-[color:var(--c-primary-text)]">${RANKING_CATEGORIES.length} 个分类</span>
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
    if (!options.preserveFilterMenus) {
        openTagDropdown = null;
        openExcludeDropdown = null;
    }
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
                <button type="button" onclick="setRankingHash('${escapeJs(category.key)}')" class="group flex min-h-[56px] items-center gap-3 rounded-[var(--radius)] border border-[color:var(--c-border)] bg-surface px-4 py-3 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-[color:var(--c-primary-soft)]">
                    <span class="shrink-0 text-base font-bold leading-none text-slate-800">${escapeHtml(category.label)}</span>
                    <span class="text-xs font-bold leading-none text-slate-400 group-hover:text-[color:var(--c-primary-text)]">${escapeHtml(category.subLabel || '日榜 · 周榜 · 月榜')}</span>
                </button>`).join('')}
            </div>
        </div>`;
}

async function loadTop250Options(refresh = false) {
    const suffix = refresh ? '?refresh=1' : '';
    const res = await apiFetch(`/api/rankings/top250/options${suffix}`).then(r => r.json());
    if (res.code !== 200) {
        const error = new Error(top250OptionErrorMessage(res.error_type, res.msg || 'TOP250 分类加载失败'));
        error.errorType = res.error_type || '';
        throw error;
    }
    const data = res.data || {};
    top250OptionCache = data.options || [];
    if (data.msg) showToast(top250OptionErrorMessage(data.error_type, data.msg));
    return data;
}

function top250OptionErrorMessage(errorType, fallback = '') {
    if (errorType === 'auth') return '刷新失败：Cookie 可能失效，请更新 Cookie 或重新登录 JavDB';
    if (errorType === 'network') return '刷新失败：网络或代理异常，请检查网络和代理设置';
    if (errorType === 'parse') return '刷新失败：TOP250 页面结构可能已变化，未找到分类选项';
    return fallback || '刷新分类失败';
}

async function refreshTop250Options() {
    try {
        await loadTop250Options(true);
        showToast('TOP250 分类已刷新');
        await renderDatabaseRoute();
    } catch (err) {
        showToast(err.message || '刷新分类失败');
    }
}

async function renderTop250OptionPage(category) {
    resetDatabasePageState();
    renderDatabaseBreadcrumb(null, null, { type: DATABASE_TYPE_RANKING, rankingCategory: category });
    const content = databaseContent();
    if (!content) return;
    showDatabaseLoading('加载 TOP250 分类...');
    let data = {};
    try {
        data = await loadTop250Options(false);
    } catch (err) {
        showDatabaseLoading(err.message || 'TOP250 分类加载失败');
        return;
    }
    const options = top250OptionCache || [];
    const optionListHtml = options.length ? `
            <div class="grid gap-3 md:grid-cols-3">
                ${options.map(option => `
                <button type="button" onclick="setRankingHash('${escapeJs(category.key)}', '${escapeJs(option.key)}')" class="group flex min-h-[56px] items-center gap-3 rounded-[var(--radius)] border border-[color:var(--c-border)] bg-surface px-4 py-3 text-left transition-colors hover:border-[color:var(--c-primary-ring)] hover:bg-[color:var(--c-primary-soft)]">
                    <span class="shrink-0 text-base font-bold leading-none text-slate-800">${escapeHtml(option.label)}</span>
                    <span class="text-xs font-bold leading-none text-slate-400 group-hover:text-[color:var(--c-primary-text)]">影片列表</span>
                </button>`).join('')}
            </div>` : `
            <div class="empty-state flex-1 flex-col gap-3 px-6 py-10">
                <div>暂无 TOP250 分类，请点击刷新分类</div>
                <button type="button" onclick="refreshTop250Options()" class="btn btn-sm btn-info text-xs">刷新分类</button>
            </div>`;
    content.innerHTML = `
        <div class="shrink-0 border-b border-slate-100 px-5 py-3">
            <div class="flex items-center justify-between gap-3">
                <div class="min-w-0">
                    <div class="text-sm font-bold text-[color:var(--c-text)]">${escapeHtml(category.label)}</div>
                    <div class="mt-1 truncate text-xs text-slate-400">${data.stale ? '使用本地缓存' : '动态分类'} · ${options.length} 个选项</div>
                </div>
                <button type="button" onclick="refreshTop250Options()" class="btn btn-sm btn-info shrink-0 text-xs">刷新分类</button>
            </div>
        </div>
        <div class="min-h-0 flex-1 overflow-y-auto p-4">
            ${optionListHtml}
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

function rankingCacheKey(category, period) {
    return `${category.key}:${period.key}`;
}

function rankingApiPath(categoryKey, periodKey, tail = '') {
    return `/api/rankings/${encodeURIComponent(categoryKey)}/${encodeURIComponent(periodKey)}${tail}`;
}

function rankingData(category, period) {
    return rankingMovieCache[rankingCacheKey(category, period)] || { movies: [], available_tags: [], total_count: 0, collection_filename: '' };
}

function currentRankingRouteMeta() {
    const route = databaseRouteInfo();
    if (route.type !== DATABASE_TYPE_RANKING || !route.category || !route.period) return null;
    const category = rankingCategoryMeta(route.category);
    const period = rankingPeriodMetaForCategory(category, route.period);
    if (!category || !period) return null;
    return { route, category, period, movieId: route.movieId || null };
}

function rankingMovieById(category, period, movieId) {
    const data = rankingData(category, period);
    return (data.movies || []).find(movie => Number(movie.id) === Number(movieId)) || null;
}

async function ensureRankingMovies(category, period, forceReload = false) {
    const key = rankingCacheKey(category, period);
    if (rankingMovieCache[key] && !forceReload) return true;
    const res = await apiFetch(rankingApiPath(category.key, period.key, '/movies')).then(r => r.json());
    if (res.code !== 200) {
        showDatabaseLoading(res.msg || '加载失败');
        return false;
    }
    rankingMovieCache[key] = res.data || { movies: [], available_tags: [], total_count: 0, collection_filename: '' };
    return true;
}

function rankingMagnetCheckTarget(category, period) {
    return `${category.key}:${period.key}`;
}

function rankingMagnetCheckMenuKey(category, period) {
    return `ranking:${rankingMagnetCheckTarget(category, period)}`;
}

async function startRankingMagnetCheck(categoryKey, periodKey, failedOnly = false) {
    openMagnetCheckMenu = null;
    await renderDatabaseRoute();
    const suffix = failedOnly ? '?failed_only=1' : '';
    const res = await apiFetch(rankingApiPath(categoryKey, periodKey, `/check_magnets${suffix}`), { method: 'POST' }).then(r => r.json());
    if (res.code !== 200 && res.code !== 409) return showToast(res.msg || '检测启动失败');
    if (res.code === 409) showToast(res.msg || '磁力检测任务正在运行');
    watchMagnetCheckJob(res.data);
    await renderDatabaseRoute();
}

function toggleRankingMagnetCheckMenu(categoryKey, periodKey, event = null) {
    if (event) event.stopPropagation();
    const category = rankingCategoryMeta(categoryKey);
    const period = rankingPeriodMetaForCategory(category, periodKey);
    if (!category || !period) return;
    openExclusiveMenu('check', rankingMagnetCheckMenuKey(category, period), () => renderDatabaseRoute());
}

function renderRankingMagnetCheckButton(category, period) {
    const target = rankingMagnetCheckTarget(category, period);
    const key = rankingMagnetCheckMenuKey(category, period);
    const job = activeMagnetCheckJob;
    const hasRunningJob = !!(job && job.running);
    const isRunningTarget = !!(hasRunningJob && job.scope === 'ranking' && String(job.target) === target);
    const isCancelling = !!(isRunningTarget && job.cancelled);
    const isOpen = openMagnetCheckMenu === key;
    const disabledAttr = (hasRunningJob && !isRunningTarget) || isCancelling ? ' disabled' : '';
    const primaryContent = isRunningTarget ? magnetSpinner('h-3 w-3') : MAGNET_RADAR_ICON;
    const primaryTitle = isRunningTarget ? '检测中' : '检测磁力';
    const toggleContent = isRunningTarget ? (isCancelling ? magnetSpinner('h-3 w-3') : MAGNET_STOP_ICON) : (isOpen ? '▲' : '▼');
    const toggleClass = isRunningTarget ? 'btn-split-stop' : 'btn-split-toggle';
    const toggleAction = isRunningTarget
        ? `cancelMagnetCheck('${escapeJs(job.job_id)}')`
        : `toggleRankingMagnetCheckMenu('${escapeJs(category.key)}', '${escapeJs(period.key)}', event)`;
    return `
        <div class="relative shrink-0" data-menu-root="magnet-check">
            <div class="inline-flex">
                <button type="button" onclick="startRankingMagnetCheck('${escapeJs(category.key)}', '${escapeJs(period.key)}')" title="${primaryTitle}" aria-label="${primaryTitle}"${disabledAttr} class="btn-split-primary h-7 w-7 text-[11px] leading-none">
                    <span class="inline-flex items-center justify-center gap-1">${primaryContent}</span>
                </button>
                <button type="button" onclick="${toggleAction}" title="更多检测选项" aria-label="更多检测选项"${isCancelling ? ' disabled' : ''} class="${toggleClass} h-7 w-6 text-[10px] leading-none">${toggleContent}</button>
            </div>
            <div onclick="event.stopPropagation()" class="menu ${isOpen && !hasRunningJob ? '' : 'hidden'} right-0 w-28 text-xs">
                <button type="button" onclick="startRankingMagnetCheck('${escapeJs(category.key)}', '${escapeJs(period.key)}', true)" class="menu-item font-bold text-[color:var(--c-neutral-text)]">check failed</button>
            </div>
        </div>`;
}

function renderRankingToolbarActions(category, period) {
    return `
        <div class="ml-auto flex shrink-0 items-center gap-1">
            <button type="button" onclick="copyRankingMagnets('${escapeJs(category.key)}', '${escapeJs(period.key)}')" title="复制榜单磁力" aria-label="复制榜单磁力" class="btn btn-icon-sm btn-info text-xs">⧉</button>
            <button type="button" onclick="downloadRankingCsv('${escapeJs(category.key)}', '${escapeJs(period.key)}')" title="下载榜单 CSV" aria-label="下载榜单 CSV" class="btn btn-icon-sm btn-success text-xs">⇩</button>
            <button type="button" onclick="updateRankingList('${escapeJs(category.key)}', '${escapeJs(period.key)}')" title="更新榜单" aria-label="更新榜单" class="btn btn-icon-sm btn-info text-xs">⟳</button>
            <button type="button" onclick="clearRankingList('${escapeJs(category.key)}', '${escapeJs(period.key)}')" title="清空榜单" aria-label="清空榜单" class="btn btn-icon-sm btn-danger">
                <svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M3 6h18"></path>
                    <path d="M8 6V4h8v2"></path>
                    <path d="M6 6l1 15h10l1-15"></path>
                    <path d="M10 11v6"></path>
                    <path d="M14 11v6"></path>
                </svg>
            </button>
        </div>`;
}

function renderRankingFilter(category, period, availableTags, filteredCount, totalCount) {
    const key = rankingFilterKey(category, period);
    const selected = selectedRankingTags(category, period);
    const excluded = selectedRankingExcludeTags(category, period);
    const isOpen = openTagDropdown === key;
    const isExcludeOpen = openExcludeDropdown === key;
    return `
        <div class="relative min-w-0" data-menu-root="tag-filter">
            <button type="button" onclick="toggleRankingTagDropdown('${escapeJs(category.key)}', '${escapeJs(period.key)}', event)" class="flex h-7 min-w-[104px] items-center justify-between gap-2 rounded border border-[color:var(--c-border)] bg-surface px-2 text-left text-xs font-bold text-[color:var(--c-neutral-text)] transition-colors hover:bg-[color:var(--c-surface-sunken)]">
                <span class="min-w-0 truncate">筛选: ${filteredCount}/${totalCount}</span>
                <span class="shrink-0">${isOpen ? '▲' : '▼'}</span>
            </button>
            <div onclick="event.stopPropagation()" class="menu ${isOpen ? '' : 'hidden'} w-64 max-h-72 overflow-y-auto">
                ${renderRankingTagOption(category, period, 'all', '全部', selected.length === 0)}
                ${(availableTags || []).map(tag => renderRankingTagOption(category, period, tag, tag, selected.includes(tag))).join('')}
            </div>
        </div>
        <div class="relative shrink-0" data-menu-root="exclude-filter">
            <button type="button" onclick="toggleRankingExcludeDropdown('${escapeJs(category.key)}', '${escapeJs(period.key)}', event)" class="flex h-7 min-w-[68px] items-center justify-between gap-1 rounded border px-2 text-left text-xs font-bold transition-colors ${excluded.length ? 'border-[color:var(--c-danger)] bg-danger-soft text-danger-text' : 'border-[color:var(--c-border)] bg-surface text-[color:var(--c-text-muted)] hover:bg-[color:var(--c-surface-sunken)]'}">
                <span class="min-w-0 truncate">${excluded.length ? `排除: ${excluded.length}个` : '排除'}</span>
                <span class="shrink-0">${isExcludeOpen ? '▲' : '▼'}</span>
            </button>
            <div onclick="event.stopPropagation()" class="menu ${isExcludeOpen ? '' : 'hidden'} w-64 max-h-72 overflow-y-auto">
                <label class="menu-item text-danger-text font-bold">
                    <input type="checkbox" onchange="clearRankingExcludeTags('${escapeJs(category.key)}', '${escapeJs(period.key)}')">
                    <span>清除排除</span>
                </label>
                ${(availableTags || []).map(tag => renderRankingExcludeOption(category, period, tag, excluded.includes(tag))).join('')}
            </div>
        </div>`;
}

function renderRankingTagOption(category, period, value, label, checked) {
    return `<label class="menu-item">
        <input type="checkbox" class="accent-[color:var(--c-primary)]" ${checked ? 'checked' : ''} onchange="toggleRankingTag('${escapeJs(category.key)}', '${escapeJs(period.key)}', '${escapeJs(value)}')">
        <span class="truncate" title="${escapeHtml(label)}">${escapeHtml(label)}</span>
    </label>`;
}

function renderRankingExcludeOption(category, period, tag, checked) {
    return `<label class="menu-item hover:bg-danger-soft">
        <input type="checkbox" ${checked ? 'checked' : ''} onchange="toggleRankingExcludeTag('${escapeJs(category.key)}', '${escapeJs(period.key)}', '${escapeJs(tag)}')"
            class="accent-[color:var(--c-danger)]">
        <span class="truncate ${checked ? 'text-danger-text font-bold' : ''}" title="${escapeHtml(tag)}">${escapeHtml(tag)}</span>
    </label>`;
}

async function copyRankingMagnets(categoryKey, periodKey) {
    try {
        const res = await apiFetch(rankingApiPath(categoryKey, periodKey, `/magnets${rankingTagsQuery(categoryKey, periodKey)}`)).then(r => r.json());
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

async function downloadRankingCsv(categoryKey, periodKey) {
    const response = await apiFetch(rankingApiPath(categoryKey, periodKey, `/download${rankingTagsQuery(categoryKey, periodKey)}`));
    if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        return showToast(data.msg || '下载失败');
    }
    const blob = await response.blob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `ranking_${categoryKey}_${periodKey}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}

async function updateRankingList(categoryKey, periodKey) {
    const res = await apiFetch(rankingApiPath(categoryKey, periodKey, '/update'), { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '添加更新任务失败');
    showToast(res.msg || '榜单更新任务已加入队列');
    await refreshMonitor();
}

async function clearRankingList(categoryKey, periodKey) {
    if (!confirm('确定清空当前榜单吗？')) return;
    const res = await apiFetch(rankingApiPath(categoryKey, periodKey, '/clear'), { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '清空失败');
    showToast(res.msg || '清空成功');
    const category = rankingCategoryMeta(categoryKey);
    const period = rankingPeriodMetaForCategory(category, periodKey);
    if (category && period) {
        delete rankingMovieCache[rankingCacheKey(category, period)];
    }
    await renderDatabaseRoute();
}

function renderRankingMovies(category, period, movies) {
    if (!movies.length) {
        return '<div class="min-h-0 flex-1 max-w-full overflow-y-auto rounded-lg border border-slate-200 bg-white"><div class="empty-state px-6 py-10">暂无榜单影片</div></div>';
    }
    return `<div class="min-h-0 flex-1 max-w-full divide-y divide-slate-100 overflow-y-auto rounded-lg border border-slate-200 bg-white">${movies.map(movie => `
        <div class="p-3">
            <button type="button" onclick="selectRankingMovie('${escapeJs(category.key)}', '${escapeJs(period.key)}', ${Number(movie.id)})" class="block w-full min-w-0 text-left transition-colors hover:text-[color:var(--c-primary-text)]">
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

async function renderRankingMovieListPage(category, period, options = {}) {
    resetDatabasePageState({ preserveMagnetCheckMenu: true, preserveFilterMenus: !!options.preserveFilterMenus });
    renderDatabaseBreadcrumb(null, null, { type: DATABASE_TYPE_RANKING, rankingCategory: category, rankingPeriod: period });
    const content = databaseContent();
    if (!content) return;
    if (!rankingMovieCache[rankingCacheKey(category, period)]) showDatabaseLoading();
    if (!(await ensureRankingMovies(category, period))) return;
    const data = rankingData(category, period);
    const movies = data.movies || [];
    const selected = selectedRankingTags(category, period);
    const excluded = selectedRankingExcludeTags(category, period);
    const filtered = filterMovies(movies, selected, excluded);
    const totalCount = Number(data.total_count || movies.length);
    content.innerHTML = `
        <div class="shrink-0 border-b border-slate-100 px-5 pb-2 pt-2">
            <div class="flex min-w-0 flex-wrap items-center justify-between gap-2 text-xs text-slate-500">
                <div class="flex min-w-0 flex-wrap items-center gap-2">
                    <span class="min-w-0 truncate">${escapeHtml(category.label)} · ${escapeHtml(period.label)} · ${totalCount} 部影片</span>
                    ${renderRankingHealthTags(filtered)}
                </div>
                ${renderRankingMagnetCheckButton(category, period)}
            </div>
        </div>
        <div class="flex min-h-0 flex-1 flex-col overflow-hidden px-4 pb-4 pt-3 text-sm text-slate-500">
            <div class="mb-3 flex shrink-0 flex-wrap items-center gap-2">
                ${renderRankingFilter(category, period, data.available_tags || [], filtered.length, totalCount)}
                ${renderRankingToolbarActions(category, period)}
            </div>
            ${renderRankingMovies(category, period, filtered)}
        </div>`;
    fitMovieTags(content);
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

async function renderRankingMagnetListPage(category, period, movieId) {
    expandedCollectionName = null;
    expandedMovieId = Number(movieId);
    setCollectionToolbarVisible(false);
    hideBatchDeleteControls();
    if (!(await ensureRankingMovies(category, period))) return;
    const movie = rankingMovieById(category, period, movieId);
    if (!movie) {
        setRankingHash(category.key, period.key);
        showToast('影片不存在或已被过滤');
        return;
    }
    renderDatabaseBreadcrumb(null, movie, { type: DATABASE_TYPE_RANKING, rankingCategory: category, rankingPeriod: period });
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
            if (category.dynamicOptions) {
                await renderTop250OptionPage(category);
                return;
            }
            renderRankingPeriodPage(category);
            return;
        }
        if (category.dynamicOptions && !top250OptionCache) {
            try {
                await loadTop250Options(false);
            } catch (err) {
                console.warn(err);
            }
        }
        const period = rankingPeriodMetaForCategory(category, route.period);
        if (!period) {
            setRankingHash(category.key);
            showToast('榜单周期不存在');
            return;
        }
        if (route.movieId) {
            await renderRankingMagnetListPage(category, period, route.movieId);
        } else {
            await renderRankingMovieListPage(category, period);
        }
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

function rankingMetaFromKeys(categoryKey, periodKey) {
    const category = rankingCategoryMeta(categoryKey);
    const period = rankingPeriodMetaForCategory(category, periodKey);
    return category && period ? { category, period } : null;
}

function renderRankingFilterState(categoryKey, periodKey) {
    const meta = rankingMetaFromKeys(categoryKey, periodKey);
    if (!meta) {
        renderDatabaseRoute();
        return;
    }
    renderRankingMovieListPage(meta.category, meta.period, { preserveFilterMenus: true });
}

function toggleRankingTagDropdown(categoryKey, periodKey, event = null) {
    if (event) event.stopPropagation();
    openExclusiveMenu('tag', rankingFilterKey(categoryKey, periodKey), () => renderRankingFilterState(categoryKey, periodKey));
}

function toggleRankingExcludeDropdown(categoryKey, periodKey, event = null) {
    if (event) event.stopPropagation();
    openExclusiveMenu('exclude', rankingFilterKey(categoryKey, periodKey), () => renderRankingFilterState(categoryKey, periodKey));
}

function toggleRankingExcludeTag(categoryKey, periodKey, value) {
    const key = filterKey(rankingFilterKey(categoryKey, periodKey));
    const current = new Set(selectedRankingExcludeTags(categoryKey, periodKey));
    if (current.has(value)) current.delete(value);
    else current.add(value);
    rankingExcludeFilters[key] = Array.from(current);
    expandedMovieId = null;
    renderRankingFilterState(categoryKey, periodKey);
}

function clearRankingExcludeTags(categoryKey, periodKey) {
    rankingExcludeFilters[filterKey(rankingFilterKey(categoryKey, periodKey))] = [];
    expandedMovieId = null;
    renderRankingFilterState(categoryKey, periodKey);
}

function toggleRankingTag(categoryKey, periodKey, value) {
    const key = filterKey(rankingFilterKey(categoryKey, periodKey));
    if (value === 'all') {
        rankingTagFilters[key] = [];
    } else {
        const current = new Set(selectedRankingTags(categoryKey, periodKey));
        if (current.has(value)) current.delete(value);
        else current.add(value);
        rankingTagFilters[key] = Array.from(current);
    }
    expandedMovieId = null;
    renderRankingFilterState(categoryKey, periodKey);
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

function syncSelectedMagnetToRankingMovie(movieId, magnets) {
    const meta = currentRankingRouteMeta();
    if (!meta) return false;
    const data = rankingMovieCache[rankingCacheKey(meta.category, meta.period)];
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

function selectRankingMovie(categoryKey, periodKey, movieId) {
    setRankingHash(categoryKey, periodKey, movieId);
}

async function selectMagnet(movieId, magnetId) {
    const res = await apiFetch(`/api/movies/${movieId}/select_magnet`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ magnet_id: magnetId })
    }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '更新失败');
    const magnets = await loadMagnets(movieId, true);
    const actorSynced = syncSelectedMagnetToMovie(movieId, magnets);
    const rankingSynced = syncSelectedMagnetToRankingMovie(movieId, magnets);
    if (actorSynced || rankingSynced) {
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

async function reloadRankingMovies(category, period) {
    await ensureRankingMovies(category, period, true);
    const route = databaseRouteInfo();
    if (route.type !== DATABASE_TYPE_RANKING || route.category !== category.key || route.period !== period.key) return;
    if (route.movieId && rankingMovieById(category, period, route.movieId)) {
        await renderRankingMagnetListPage(category, period, route.movieId);
    } else {
        await renderRankingMovieListPage(category, period);
    }
}

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
