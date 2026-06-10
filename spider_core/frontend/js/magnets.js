/*
 * magnets.js — 磁力验活（检测任务）
 * 检测按钮渲染、检测任务启动/轮询/取消，以及检测完成后的结果刷新。
 */

/* ===== 磁力检测轮询：setTimeout 链 + try/catch ===== */

function startMagnetCheckPolling() {
    stopMagnetCheckPolling();
    const tick = async () => {
        try {
            await pollMagnetCheckJob();
        } catch (err) {
            console.error('磁力检测轮询出错:', err);
        } finally {
            if (magnetCheckPollInterval) magnetCheckPollInterval = setTimeout(tick, 1000);
        }
    };
    magnetCheckPollInterval = setTimeout(tick, 1000);
}

function stopMagnetCheckPolling() {
    if (magnetCheckPollInterval) {
        clearTimeout(magnetCheckPollInterval);
        magnetCheckPollInterval = null;
    }
}

/* ===== 检测按钮渲染 ===== */

function magnetCheckMenuKey(scope, target) {
    return `${scope}:${target}`;
}

function magnetCheckButtonId(scope, target) {
    return `magnet-check-button-${scope}-${encodeURIComponent(String(target))}`;
}

function renderMagnetCheckButton(scope, target) {
    const key = magnetCheckMenuKey(scope, target);
    const isOpen = openMagnetCheckMenu === key;
    const job = activeMagnetCheckJob;
    const isRunning = !!(job && job.running && job.scope === scope && String(job.target) === String(target));
    const progress = isRunning ? `${Number(job.completed || 0)}/${Number(job.total || 0)}` : 'check';
    const radarIcon = `<svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <circle cx="12" cy="12" r="9"></circle>
                        <path d="M12 12l6-4"></path>
                        <path d="M12 3v2"></path>
                        <path d="M12 19v2"></path>
                        <path d="M3 12h2"></path>
                        <path d="M19 12h2"></path>
                        <path d="M8.5 8.5a5 5 0 0 1 7 0"></path>
                    </svg>`;
    const targetArg = scope === 'collection' || scope === 'all' ? `'${escapeJs(target)}'` : target;
    const startFn = scope === 'collection'
        ? 'startCollectionMagnetCheck'
        : (scope === 'all' ? 'startAllMagnetCheck' : 'startMovieMagnetCheck');
    const toggleFn = scope === 'collection'
        ? 'toggleCollectionMagnetCheckMenu'
        : (scope === 'all' ? 'toggleAllMagnetCheckMenu' : 'toggleMovieMagnetCheckMenu');
    const idAttr = scope === 'movie' ? ` id="check-movie-${target}"` : '';
    const disabledAttr = isRunning ? ' disabled' : '';
    const primaryIdleClass = scope === 'movie'
        ? 'h-5 w-6 rounded-l bg-emerald-50 text-[11px] font-bold leading-none text-emerald-700 hover:bg-emerald-100'
        : 'h-9 w-14 rounded-l border border-r-0 border-emerald-200 bg-emerald-50 text-xs font-bold text-emerald-700 shadow-sm hover:bg-emerald-100';
    const primaryRunningClass = scope === 'movie'
        ? 'h-5 w-16 cursor-not-allowed rounded-l bg-slate-100 text-[10px] font-bold leading-none text-slate-500'
        : 'h-9 w-16 cursor-not-allowed rounded-l border border-r-0 border-slate-200 bg-slate-100 text-xs font-bold text-slate-500 shadow-sm';
    const toggleIdleClass = scope === 'movie'
        ? 'h-5 w-5 rounded-r border-l border-emerald-100 bg-emerald-50 text-[10px] font-bold leading-none text-emerald-700 hover:bg-emerald-100'
        : 'h-9 w-7 rounded-r border border-emerald-200 bg-emerald-50 text-xs font-bold text-emerald-700 shadow-sm hover:bg-emerald-100';
    const toggleRunningClass = scope === 'movie'
        ? 'h-5 w-5 cursor-not-allowed rounded-r border-l border-slate-200 bg-slate-100 text-[10px] font-bold leading-none text-slate-400'
        : 'h-9 w-7 cursor-not-allowed rounded-r border border-slate-200 bg-slate-100 text-xs font-bold text-slate-400 shadow-sm';
    const primaryClass = isRunning
        ? primaryRunningClass
        : primaryIdleClass;
    const toggleClass = isRunning ? toggleRunningClass : toggleIdleClass;
    const spinnerClass = scope === 'movie' ? 'h-2.5 w-2.5' : 'h-3 w-3';
    const primaryContent = scope === 'movie' && !isRunning ? radarIcon : `<span>${progress}</span>`;
    return `
        <div id="${magnetCheckButtonId(scope, target)}" class="relative shrink-0" data-menu-root="magnet-check">
            <div class="inline-flex">
                <button${idAttr} type="button" onclick="${startFn}(${targetArg})" title="检测磁力" aria-label="检测磁力"${disabledAttr} class="${primaryClass}">
                    <span class="inline-flex items-center justify-center gap-1">
                        ${isRunning ? `<span class="inline-block ${spinnerClass} animate-spin rounded-full border-2 border-slate-300 border-t-slate-600"></span>` : ''}
                        ${primaryContent}
                    </span>
                </button>
                <button type="button" onclick="${toggleFn}(${targetArg}, event)" title="更多检测选项" aria-label="更多检测选项"${disabledAttr} class="${toggleClass}">${isOpen ? '▲' : '▼'}</button>
            </div>
            <div onclick="event.stopPropagation()" class="${isOpen && !isRunning ? '' : 'hidden'} absolute right-0 z-30 mt-1 w-28 rounded border border-slate-200 bg-white p-1 text-xs shadow-lg">
                <button type="button" onclick="${startFn}(${targetArg}, true)" class="w-full rounded px-2 py-1.5 text-left font-bold text-slate-700 hover:bg-slate-50">check failed</button>
            </div>
        </div>`;
}

function updateRenderedMagnetCheckButtons() {
    renderGlobalMagnetCheckButton();
    if (!expandedCollectionName) return;
    const collectionButton = document.getElementById(magnetCheckButtonId('collection', expandedCollectionName));
    if (collectionButton) collectionButton.outerHTML = renderMagnetCheckButton('collection', expandedCollectionName);
    const movieButtonTarget = activeMagnetCheckJob && activeMagnetCheckJob.scope === 'movie'
        ? activeMagnetCheckJob.target
        : expandedMovieId;
    if (movieButtonTarget) {
        const movieButton = document.getElementById(magnetCheckButtonId('movie', movieButtonTarget));
        if (movieButton) movieButton.outerHTML = renderMagnetCheckButton('movie', movieButtonTarget);
    }
}

function renderGlobalMagnetCheckButton() {
    const slot = document.getElementById('globalMagnetCheckSlot');
    if (!slot) return;
    slot.innerHTML = renderMagnetCheckButton('all', 'all');
}

// 注：批量检测进度条 + “取消检测”按钮 UI（原 renderMagnetCheckProgress）此前从未被渲染调用，
// 已作为死代码移除以免误导。cancelMagnetCheck() 取消能力保留，待后续把入口正式接入检测按钮区。

/* ===== 检测选项菜单 ===== */

function toggleCollectionMagnetCheckMenu(collectionName, event = null) {
    if (event) event.stopPropagation();
    const key = magnetCheckMenuKey('collection', collectionName);
    openMagnetCheckMenu = openMagnetCheckMenu === key ? null : key;
    openTagDropdown = null;
    renderCollectionBody(collectionName);
}

function toggleAllMagnetCheckMenu(_target = 'all', event = null) {
    if (event) event.stopPropagation();
    const key = magnetCheckMenuKey('all', 'all');
    openMagnetCheckMenu = openMagnetCheckMenu === key ? null : key;
    openTagDropdown = null;
    renderGlobalMagnetCheckButton();
}

async function toggleMovieMagnetCheckMenu(movieId, event = null) {
    if (event) event.stopPropagation();
    const key = magnetCheckMenuKey('movie', movieId);
    openMagnetCheckMenu = openMagnetCheckMenu === key ? null : key;
    if (!expandedCollectionName) return;
    const openedMovieId = expandedMovieId;
    renderCollectionBody(expandedCollectionName);
    if (openedMovieId) {
        expandedMovieId = openedMovieId;
        await loadMagnets(openedMovieId, true);
    }
}

/* ===== 检测任务启动 ===== */

async function startMovieMagnetCheck(movieId, failedOnly = false) {
    openMagnetCheckMenu = null;
    const suffix = failedOnly ? '?failed_only=1' : '';
    const res = await apiFetch(`/api/movies/${movieId}/check_magnets${suffix}`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200 && res.code !== 409) {
        return showToast(res.msg || '检测启动失败');
    }
    if (res.code === 409) showToast(res.msg || '磁力检测任务正在运行');
    watchMagnetCheckJob(res.data);
    if (expandedCollectionName) {
        updateRenderedMagnetCheckButtons();
        if (expandedMovieId === movieId) await refreshMagnetRows(movieId);
    }
}

async function startCollectionMagnetCheck(collectionName, failedOnly = false) {
    openMagnetCheckMenu = null;
    renderCollectionBody(collectionName);
    const suffix = failedOnly ? '?failed_only=1' : '';
    const res = await apiFetch(`/api/collections/${encodeURIComponent(collectionName)}/check_magnets${suffix}`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200 && res.code !== 409) return showToast(res.msg || '检测启动失败');
    if (res.code === 409) showToast(res.msg || '磁力检测任务正在运行');
    watchMagnetCheckJob(res.data);
    renderCollectionBody(collectionName);
}

async function startAllMagnetCheck(_target = 'all', failedOnly = false) {
    openMagnetCheckMenu = null;
    renderGlobalMagnetCheckButton();
    const suffix = failedOnly ? '?failed_only=1' : '';
    const res = await apiFetch(`/api/magnets/check_all${suffix}`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200 && res.code !== 409) return showToast(res.msg || '检测启动失败');
    if (res.code === 409) showToast(res.msg || '磁力检测任务正在运行');
    watchMagnetCheckJob(res.data);
    updateRenderedMagnetCheckButtons();
}

/* ===== 检测任务轮询与恢复 ===== */

function watchMagnetCheckJob(job) {
    activeMagnetCheckJob = job;
    startMagnetCheckPolling();
    pollMagnetCheckJob();
}

async function restoreMagnetCheckJob() {
    const res = await apiFetch('/api/magnet_check_jobs/current').then(r => r.json());
    if (res.code !== 200 || !res.data) return;
    activeMagnetCheckJob = res.data;
    if (activeMagnetCheckJob.scope === 'collection') {
        await ensureCollectionVisible(activeMagnetCheckJob.target);
    }
    watchMagnetCheckJob(activeMagnetCheckJob);
}

async function ensureCollectionVisible(collectionName) {
    if (!collectionName) return;
    if (expandedCollectionName !== collectionName) {
        await toggleCollection(collectionName);
        return;
    }
    const body = document.getElementById(`collection-body-${collectionName}`);
    if (body && !body.dataset.loaded) {
        const res = await apiFetch(`/api/collections/${encodeURIComponent(collectionName)}/movies`).then(r => r.json());
        if (res.code === 200) {
            body.dataset.loaded = '1';
            collectionMovieCache[filterKey(collectionName)] = res.data || { movies: [], available_tags: [], total_count: 0 };
        }
    }
    renderCollectionBody(collectionName);
}

async function pollMagnetCheckJob() {
    if (!activeMagnetCheckJob) return;
    const res = await apiFetch(`/api/magnet_check_jobs/${encodeURIComponent(activeMagnetCheckJob.job_id)}`).then(r => r.json());
    if (res.code !== 200) return;
    activeMagnetCheckJob = res.data;
    const runningJob = activeMagnetCheckJob.running;
    if (runningJob && expandedCollectionName && activeMagnetCheckJob.scope === 'collection' && activeMagnetCheckJob.target === expandedCollectionName) {
        updateRenderedMagnetCheckButtons();
        if (expandedMovieId) await refreshMagnetRows(expandedMovieId);
    } else if (runningJob && activeMagnetCheckJob.scope === 'all') {
        updateRenderedMagnetCheckButtons();
        if (expandedMovieId) await refreshMagnetRows(expandedMovieId);
    } else if (runningJob && expandedCollectionName && activeMagnetCheckJob.scope === 'movie') {
        updateRenderedMagnetCheckButtons();
        if (expandedMovieId && String(expandedMovieId) === String(activeMagnetCheckJob.target)) {
            await refreshMagnetRows(expandedMovieId);
        }
    }
    if (!activeMagnetCheckJob.running) {
        stopMagnetCheckPolling();
        await refreshMagnetCheckTarget(activeMagnetCheckJob);
    }
}

async function cancelMagnetCheck(jobId) {
    const res = await apiFetch(`/api/magnet_check_jobs/${encodeURIComponent(jobId)}/cancel`, { method: 'POST' }).then(r => r.json());
    if (res.code === 200) {
        activeMagnetCheckJob = res.data;
        if (expandedCollectionName) renderCollectionBody(expandedCollectionName);
    }
}

async function refreshMagnetCheckTarget(job) {
    if (job.scope === 'collection' && expandedCollectionName === job.target) {
        const body = document.getElementById(`collection-body-${job.target}`);
        if (body) body.dataset.loaded = '';
        await reloadCollectionMovies(job.target);
    }
    if (job.scope === 'movie') {
        const movieId = Number(job.target);
        const box = document.getElementById(`magnets-${movieId}`);
        let magnets = [];
        if (box && !box.classList.contains('hidden')) {
            magnets = await refreshMagnetRows(movieId);
        } else {
            const res = await apiFetch(`/api/movies/${movieId}/magnets`).then(r => r.json());
            magnets = res.data || [];
        }
        if (syncSelectedMagnetToMovie(movieId, magnets)) updateMovieSelectedName(movieId, magnets);
        updateRenderedMagnetCheckButtons();
    }
    if (job.scope === 'all') {
        await loadCollections();
        updateRenderedMagnetCheckButtons();
    }
}
