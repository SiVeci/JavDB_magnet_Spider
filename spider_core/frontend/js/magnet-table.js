// Movie magnet table rendering and selection.

async function loadMagnets(movieId, keepOpen = false) {
    const box = document.getElementById(`magnets-${movieId}`);
    if (!box) return [];
    if (keepOpen) box.classList.remove('hidden');
    const res = await apiFetchJson(`/api/movies/${movieId}/magnets`);
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
    const res = await apiFetchJson(`/api/movies/${movieId}/magnets`);
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
