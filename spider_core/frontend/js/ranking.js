// Ranking database views and actions.

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
    const res = await apiFetchJson(`/api/rankings/top250/options${suffix}`);
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
    const res = await apiFetchJson(rankingApiPath(category.key, period.key, '/movies'));
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

function rankingTargetParts(target) {
    const parts = String(target || '').split(':');
    return { categoryKey: parts[0] || '', periodKey: parts[1] || '' };
}

async function startRankingMagnetCheckByTarget(target, failedOnly = false) {
    const parts = rankingTargetParts(target);
    await startRankingMagnetCheck(parts.categoryKey, parts.periodKey, failedOnly);
}

function toggleRankingMagnetCheckMenuByTarget(target, event = null) {
    const parts = rankingTargetParts(target);
    toggleRankingMagnetCheckMenu(parts.categoryKey, parts.periodKey, event);
}

async function startRankingMagnetCheck(categoryKey, periodKey, failedOnly = false) {
    openMagnetCheckMenu = null;
    await renderDatabaseRoute();
    const suffix = failedOnly ? '?failed_only=1' : '';
    const res = await apiFetchJson(rankingApiPath(categoryKey, periodKey, `/check_magnets${suffix}`), { method: 'POST' });
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
    await copyMagnetsFromUrl(rankingApiPath(categoryKey, periodKey, `/magnets${rankingTagsQuery(categoryKey, periodKey)}`));
}

async function downloadRankingCsv(categoryKey, periodKey) {
    try {
        await apiDownloadBlob(
            rankingApiPath(categoryKey, periodKey, `/download${rankingTagsQuery(categoryKey, periodKey)}`),
            `ranking_${categoryKey}_${periodKey}.csv`
        );
    } catch (err) {
        showToast(err.message || '下载失败');
    }
}

async function updateRankingList(categoryKey, periodKey) {
    const res = await apiFetchJson(rankingApiPath(categoryKey, periodKey, '/update'), { method: 'POST' });
    if (res.code !== 200) return showToast(res.msg || '添加更新任务失败');
    showToast(res.msg || '榜单更新任务已加入队列');
    await refreshMonitor();
}

async function clearRankingList(categoryKey, periodKey) {
    if (!confirm('确定清空当前榜单吗？')) return;
    const res = await apiFetchJson(rankingApiPath(categoryKey, periodKey, '/clear'), { method: 'POST' });
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
                ${renderMagnetCheckButton('ranking', rankingMagnetCheckTarget(category, period))}
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
