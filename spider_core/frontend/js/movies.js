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
    const items = [
        { key: 'active', title: '有效影片', className: 'border-emerald-200 bg-emerald-50 text-emerald-700' },
        { key: 'weak', title: '弱影片', className: 'border-amber-200 bg-amber-50 text-amber-700' },
        { key: 'dead', title: '无效影片', className: 'border-red-200 bg-red-100 text-red-700' },
        { key: 'failed', title: '检测失败影片', className: 'border-red-100 bg-red-50 text-red-400' },
    ];
    return `<div class="grid h-9 grid-cols-2 grid-rows-2 gap-0.5" aria-label="磁力检测影片统计">
        ${items.map(item => `<span title="${item.title}" class="flex min-w-[4ch] items-center justify-center rounded border px-1 text-[10px] font-bold leading-none ${item.className}">${renderHealthCount(counts[item.key])}</span>`).join('')}
    </div>`;
}

/* ===== 影片标签渲染与自适应折叠 ===== */

function renderMovieTags(tags) {
    const list = tags || [];
    if (!list.length) return '';
    return `<div class="movie-tags mt-2 flex max-w-full flex-nowrap gap-0.5 overflow-hidden" title="${escapeHtml(list.join(', '))}">
        ${list.map(tag => `<span data-role="tag" class="shrink-0 max-w-[104px] truncate px-1.5 py-0.5 rounded bg-slate-100 text-slate-600 text-[11px]">${escapeHtml(tag)}</span>`).join('')}
        <span data-role="more" class="hidden shrink-0 px-1.5 py-0.5 rounded bg-indigo-50 text-indigo-700 text-[11px]">+0</span>
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
    if (magnet.check_error && !magnet.check_status) {
        return `<span title="${escapeHtml(magnet.check_error)}" class="text-slate-500">❌</span>`;
    }
    if (!magnet.checked_at) return '<span title="未检测" class="text-slate-400">⚪</span>';
    if (magnet.check_status === 'active') return '<span title="有效" class="text-green-600">🟢</span>';
    if (magnet.check_status === 'weak') return '<span title="弱" class="text-yellow-500">🟡</span>';
    if (magnet.check_status === 'dead') return `<span title="${escapeHtml(magnet.check_error || '无效')}" class="text-red-600">🔴</span>`;
    return `<span title="${escapeHtml(magnet.check_error || '检测失败')}" class="text-slate-500">❌</span>`;
}

/* ===== 集合列表 ===== */

async function loadCollections() {
    const res = await apiFetch('/api/history').then(r => r.json());
    collectionsCache = res.data || [];
    expandedCollectionName = null;
    expandedMovieId = null;
    renderCollections();
    renderGlobalMagnetCheckButton();
}

function renderCollections() {
    const tbody = document.getElementById('collection-list');
    const selectAll = document.getElementById('selectAllCheckbox');
    const batchBtn = document.getElementById('batchDeleteBtn');
    if (!collectionsCache.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="px-6 py-5 text-center text-slate-400">暂无数据库集合</td></tr>';
        selectAll.classList.add('hidden');
        batchBtn.classList.add('hidden');
        return;
    }
    selectAll.classList.remove('hidden');
    batchBtn.classList.remove('hidden');
    tbody.innerHTML = collectionsCache.map(item => {
        const name = escapeHtml(item.name);
        const shownName = escapeHtml(displayName(item.name));
        const jsName = escapeJs(item.name);
        const incrementalButton = item.has_source_url
            ? `<button onclick="enqueueCollectionIncremental('${jsName}', event)" title="增量爬取此集合" aria-label="增量爬取此集合" class="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded bg-indigo-50 text-sm font-bold text-indigo-700 hover:bg-indigo-100">⟳</button>`
            : `<button type="button" disabled title="缺少原始 URL，无法快捷增量" aria-label="缺少原始 URL，无法快捷增量" class="inline-flex h-6 w-6 shrink-0 cursor-not-allowed items-center justify-center rounded bg-slate-50 text-sm font-bold text-slate-300">⟳</button>`;
        return `
        <tr class="border-b border-slate-100 bg-white">
            <td class="px-3 py-3 text-center"><input type="checkbox" class="collection-checkbox" value="${name}" onclick="updateBatchDeleteBtn()"></td>
            <td class="min-w-0 px-2 py-3">
                <div class="flex min-w-0 items-start gap-2">
                    <button onclick="toggleCollection('${jsName}')" title="展开/收起" aria-label="展开或收起数据集合" class="mt-0.5 inline-flex h-6 w-6 shrink-0 items-center justify-center rounded bg-slate-50 text-xs font-bold text-slate-700 hover:bg-slate-100"><span id="collection-toggle-${escapeHtml(item.name)}" class="inline-block transition-transform duration-200 ease-out">▾</span></button>
                    ${incrementalButton}
                    <div class="min-w-0 flex-1">
                        <div class="truncate font-bold" title="${shownName}">${shownName}</div>
                        <div class="truncate text-xs text-slate-400">${escapeHtml(item.time)} · ${((item.tags || []).length)} 个标签</div>
                    </div>
                </div>
            </td>
            <td class="px-1 py-3 text-center"><span class="inline-flex justify-center whitespace-nowrap rounded bg-blue-50 px-1.5 py-0.5 text-[11px] font-bold text-blue-700">${item.count}</span></td>
            <td class="px-1 py-3">
                <div class="flex justify-center gap-1 whitespace-nowrap">
                    <button onclick="copyMagnets('${jsName}')" title="复制磁力" aria-label="复制磁力" class="inline-flex h-7 w-7 items-center justify-center rounded bg-blue-50 text-sm font-bold text-blue-700 hover:bg-blue-100">⧉</button>
                    <button onclick="downloadCsv('${jsName}')" title="下载 CSV" aria-label="下载 CSV" class="inline-flex h-7 w-7 items-center justify-center rounded bg-green-50 text-sm font-bold text-green-700 hover:bg-green-100">⇩</button>
                    <button onclick="deleteFiles(['${jsName}'])" title="删除集合" aria-label="删除集合" class="inline-flex h-7 w-7 items-center justify-center rounded bg-red-50 text-base font-bold text-red-700 hover:bg-red-100">×</button>
                </div>
            </td>
        </tr>
        <tr id="collection-${escapeHtml(item.name)}" class="hidden bg-slate-50">
            <td colspan="4" class="p-0"><div id="collection-body-${escapeHtml(item.name)}" class="w-full min-w-0 max-w-full overflow-x-hidden p-4 text-sm text-slate-500">加载中...</div></td>
        </tr>`;
    }).join('');
    updateBatchDeleteBtn();
}

async function toggleCollection(name) {
    const row = document.getElementById(`collection-${name}`);
    const body = document.getElementById(`collection-body-${name}`);
    if (!row || !body) return;

    const shouldOpen = row.classList.contains('hidden');
    if (expandedCollectionName && expandedCollectionName !== name) {
        const previousRow = document.getElementById(`collection-${expandedCollectionName}`);
        if (previousRow) previousRow.classList.add('hidden');
        const previousIcon = document.getElementById(`collection-toggle-${expandedCollectionName}`);
        if (previousIcon) previousIcon.classList.remove('rotate-180');
    }
    if (!shouldOpen) {
        row.classList.add('hidden');
        const icon = document.getElementById(`collection-toggle-${name}`);
        if (icon) icon.classList.remove('rotate-180');
        expandedCollectionName = null;
        expandedMovieId = null;
        openTagDropdown = null;
        openMagnetCheckMenu = null;
        return;
    }

    row.classList.remove('hidden');
    const icon = document.getElementById(`collection-toggle-${name}`);
    if (icon) icon.classList.add('rotate-180');
    expandedCollectionName = name;
    expandedMovieId = null;
    openTagDropdown = null;
    openMagnetCheckMenu = null;
    row.querySelectorAll('[id^="magnets-"]').forEach(item => item.classList.add('hidden'));
    if (body.dataset.loaded) return;
    const res = await apiFetch(`/api/collections/${encodeURIComponent(name)}/movies`).then(r => r.json());
    if (res.code !== 200) {
        body.innerText = res.msg || '加载失败';
        return;
    }
    body.dataset.loaded = '1';
    collectionMovieCache[filterKey(name)] = res.data || { movies: [], available_tags: [], total_count: 0 };
    renderCollectionBody(name);
}

function renderCollectionBody(collectionName) {
    const body = document.getElementById(`collection-body-${collectionName}`);
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
        <div class="mb-3 flex items-center gap-2">
            <div class="relative min-w-0" data-menu-root="tag-filter">
                <button type="button" onclick="toggleCollectionTagDropdown('${escapeJs(collectionName)}', event)" class="flex h-9 min-w-[104px] items-center justify-between gap-2 rounded border border-slate-200 bg-white px-2 text-left text-xs font-bold text-slate-700">
                    <span class="min-w-0 truncate">筛选: ${filteredCount}/${totalCount}</span>
                    <span class="shrink-0">${isOpen ? '▲' : '▼'}</span>
                </button>
                <div id="tag-dropdown-${escapeHtml(collectionName)}" onclick="event.stopPropagation()" class="${isOpen ? '' : 'hidden'} absolute z-20 mt-1 w-64 max-h-72 overflow-y-auto rounded border border-slate-200 bg-white shadow-lg p-2">
                    ${renderTagOption(collectionName, 'all', '全部', selected.length === 0)}
                    ${(availableTags || []).map(tag => renderTagOption(collectionName, tag, tag, selected.includes(tag))).join('')}
                </div>
            </div>
            <div class="relative shrink-0" data-menu-root="exclude-filter">
                <button type="button" onclick="toggleExcludeDropdown('${escapeJs(collectionName)}', event)" class="flex h-9 min-w-[72px] items-center justify-between gap-1 rounded border px-2 text-left text-xs font-bold ${excluded.length ? 'border-red-300 bg-red-50 text-red-700' : 'border-slate-200 bg-white text-slate-500'}">
                    <span class="min-w-0 truncate">${excluded.length ? `排除: ${excluded.length}个` : '排除'}</span>
                    <span class="shrink-0">${isExcludeOpen ? '▲' : '▼'}</span>
                </button>
                <div onclick="event.stopPropagation()" class="${isExcludeOpen ? '' : 'hidden'} absolute z-20 mt-1 w-64 max-h-72 overflow-y-auto rounded border border-slate-200 bg-white shadow-lg p-2">
                    <label class="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-slate-50 text-xs cursor-pointer text-red-600 font-bold">
                        <input type="checkbox" ${excluded.length === 0 ? '' : ''} onchange="clearExcludeTags('${escapeJs(collectionName)}')">
                        <span>清除排除</span>
                    </label>
                    ${(availableTags || []).map(tag => renderExcludeOption(collectionName, tag, excluded.includes(tag))).join('')}
                </div>
            </div>
            <div class="flex shrink-0 items-center gap-2">
                ${renderMagnetCheckButton('collection', collectionName)}
                ${renderCollectionHealthTags(filteredMovies)}
            </div>
        </div>`;
}

function renderTagOption(collectionName, value, label, checked) {
    return `<label class="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-slate-50 text-xs cursor-pointer">
        <input type="checkbox" ${checked ? 'checked' : ''} onchange="toggleCollectionTag('${escapeJs(collectionName)}', '${escapeJs(value)}')">
        <span class="truncate" title="${escapeHtml(label)}">${escapeHtml(label)}</span>
    </label>`;
}

function renderExcludeOption(collectionName, tag, checked) {
    return `<label class="flex items-center gap-2 px-2 py-1.5 rounded hover:bg-red-50 text-xs cursor-pointer">
        <input type="checkbox" ${checked ? 'checked' : ''} onchange="toggleExcludeTag('${escapeJs(collectionName)}', '${escapeJs(tag)}')"
            class="accent-red-500">
        <span class="truncate ${checked ? 'text-red-700 font-bold' : ''}" title="${escapeHtml(tag)}">${escapeHtml(tag)}</span>
    </label>`;
}

function renderMovies(collectionName, movies) {
    if (!movies.length) return '<div class="text-center text-slate-400">暂无匹配影片记录</div>';
    return `<div class="max-h-[60vh] max-w-full space-y-2 overflow-y-auto overflow-x-hidden pr-2">${movies.map(movie => `
        <div class="max-w-full overflow-hidden bg-white border border-slate-200 rounded p-3">
            <div class="min-w-0 overflow-hidden">
                <div class="truncate font-bold" title="${escapeHtml(`${movie.code} ${movie.title || ''}`)}"><span>${escapeHtml(movie.code)}</span> <span class="font-normal text-slate-500">${escapeHtml(movie.title)}</span></div>
                <div class="mt-1 grid grid-cols-[max-content_minmax(0,1fr)] items-center gap-2">
                    <div class="flex min-w-max items-center gap-1">
                        <button onclick="toggleMagnets(${movie.id})" title="展开/收起候选磁力" aria-label="展开或收起候选磁力" class="inline-flex h-6 w-6 items-center justify-center rounded bg-slate-50 text-xs font-bold text-slate-600 hover:bg-slate-100">
                            <span id="movie-toggle-${movie.id}" class="inline-block transition-transform duration-200 ease-out">▾</span>
                        </button>
                        <div class="whitespace-nowrap rounded bg-indigo-50 px-2 py-1 text-xs font-bold leading-none text-indigo-700">候选 ${movie.candidate_count || 0}</div>
                        ${renderMagnetCheckButton('movie', movie.id)}
                    </div>
                    <div id="movie-selected-name-${movie.id}" class="min-w-0 truncate text-xs text-slate-500" title="${escapeHtml(movie.best_magnet_name || '未选中磁力')}">${escapeHtml(movie.best_magnet_name || '未选中磁力')}</div>
                </div>
                ${renderMovieTags(movie.tags || [])}
            </div>
            <div id="magnets-${movie.id}" class="hidden mt-3"></div>
        </div>
    `).join('')}</div>`;
}

/* ===== 标签下拉交互 ===== */

function toggleCollectionTagDropdown(collectionName, event = null) {
    if (event) event.stopPropagation();
    openTagDropdown = openTagDropdown === collectionName ? null : collectionName;
    openExcludeDropdown = null;
    openMagnetCheckMenu = null;
    renderCollectionBody(collectionName);
}

function toggleExcludeDropdown(collectionName, event = null) {
    if (event) event.stopPropagation();
    openExcludeDropdown = openExcludeDropdown === collectionName ? null : collectionName;
    openTagDropdown = null;
    openMagnetCheckMenu = null;
    renderCollectionBody(collectionName);
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
    box.innerHTML = magnets.length ? renderMagnetTable(movieId, magnets) : '<div class="text-slate-400">暂无候选磁力</div>';
    return magnets;
}

function renderMagnetTable(movieId, magnets) {
    return `
        <div class="mb-2 flex items-center justify-between gap-2">
        </div>
        <div class="max-h-[280px] overflow-auto">
            <table class="w-full table-fixed text-xs">
                <colgroup>
                    <col class="w-14"><col><col class="w-10"><col class="w-16"><col class="w-16">
                </colgroup>
                <thead class="sticky top-0 bg-slate-50"><tr><th class="p-2 text-center whitespace-nowrap">状态</th><th class="p-2 text-left">文件名</th><th class="p-2 text-center">分数</th><th class="p-2 text-center">大小</th><th class="p-2 text-center">操作</th></tr></thead>
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
        <tr id="magnet-row-${magnet.id}" data-signature="${signature}" class="border-t border-slate-100">
            <td class="p-2 text-center align-middle">
                <div>${renderMagnetStatus(magnet)}</div>
                <div class="mt-1 text-[10px] leading-none text-slate-400">${magnet.checked_at ? `${magnet.seeders ?? 0}/${magnet.leechers ?? 0}` : '-/-'}</div>
            </td>
            <td class="min-w-0 cursor-pointer p-2 hover:bg-slate-50" title="${escapeHtml(magnet.link)}" onclick="${magnet.is_selected ? '' : `selectMagnet(${movieId}, ${magnet.id})`}">
                <div class="truncate">${magnet.is_selected ? '<span class="mr-1 text-green-600">✓</span>' : ''}${escapeHtml(magnet.name)}</div>
                <div class="mt-1 inline-flex max-w-full rounded bg-slate-100 px-1.5 py-0.5 text-[10px] leading-none text-slate-500">${escapeHtml(magnet.magnet_date || '-')}</div>
            </td>
            <td class="p-2 text-center align-middle">${magnet.priority_score}</td>
            <td class="p-2 text-center align-middle whitespace-nowrap">${formatGb(magnet.size_mb)}</td>
            <td class="p-2 text-center align-middle">
                <div class="flex justify-center gap-1">
                    <button onclick="copyText('${escapeJs(magnet.link)}')" title="复制磁力链接" aria-label="复制磁力链接" class="inline-flex h-7 w-7 items-center justify-center rounded bg-blue-50 text-blue-700 hover:bg-blue-100">
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
        box.innerHTML = magnets.length ? renderMagnetTable(movieId, magnets) : '<div class="text-slate-400">暂无候选磁力</div>';
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
    const box = document.getElementById(`magnets-${movieId}`);
    if (!box) return;
    const shouldOpen = box.classList.contains('hidden');
    if (expandedMovieId && expandedMovieId !== movieId) {
        const previousBox = document.getElementById(`magnets-${expandedMovieId}`);
        if (previousBox) previousBox.classList.add('hidden');
        const previousIcon = document.getElementById(`movie-toggle-${expandedMovieId}`);
        if (previousIcon) previousIcon.classList.remove('rotate-180');
    }
    if (!shouldOpen) {
        box.classList.add('hidden');
        const icon = document.getElementById(`movie-toggle-${movieId}`);
        if (icon) icon.classList.remove('rotate-180');
        expandedMovieId = null;
        openMagnetCheckMenu = null;
        return;
    }
    expandedMovieId = movieId;
    const icon = document.getElementById(`movie-toggle-${movieId}`);
    if (icon) icon.classList.add('rotate-180');
    await loadMagnets(movieId, true);
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
    renderCollectionBody(collectionName);
    if (previousMovieId) {
        expandedMovieId = previousMovieId;
        const box = document.getElementById(`magnets-${previousMovieId}`);
        if (box) await loadMagnets(previousMovieId, true);
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
