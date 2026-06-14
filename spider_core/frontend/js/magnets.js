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
    const hasRunningJob = !!(job && job.running);
    const isRunningTarget = !!(hasRunningJob && job.scope === scope && String(job.target) === String(target));
    const isCancelling = !!(isRunningTarget && job.cancelled);
    const progress = isRunningTarget ? `${Number(job.completed || 0)}/${Number(job.total || 0)}` : 'check';
    const radarIcon = `<svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <circle cx="12" cy="12" r="9"></circle>
                        <path d="M12 12l6-4"></path>
                        <path d="M12 3v2"></path>
                        <path d="M12 19v2"></path>
                        <path d="M3 12h2"></path>
                        <path d="M19 12h2"></path>
                        <path d="M8.5 8.5a5 5 0 0 1 7 0"></path>
                    </svg>`;
    const stopIcon = `<svg aria-hidden="true" viewBox="0 0 24 24" class="h-3 w-3" fill="currentColor">
                        <rect x="6" y="6" width="12" height="12" rx="1.5"></rect>
                    </svg>`;
    const targetArg = scope === 'collection' || scope === 'all' ? `'${escapeJs(target)}'` : target;
    const startFn = scope === 'collection'
        ? 'startCollectionMagnetCheck'
        : (scope === 'all' ? 'startAllMagnetCheck' : 'startMovieMagnetCheck');
    const toggleFn = scope === 'collection'
        ? 'toggleCollectionMagnetCheckMenu'
        : (scope === 'all' ? 'toggleAllMagnetCheckMenu' : 'toggleMovieMagnetCheckMenu');
    const idAttr = scope === 'movie' ? ` id="check-movie-${target}"` : '';
    const primaryDisabledAttr = (hasRunningJob && !isRunningTarget) || isCancelling ? ' disabled' : '';
    const showProgressWithStop = isRunningTarget && scope !== 'movie';
    const showStopOnToggle = isRunningTarget;
    const toggleDisabledAttr = hasRunningJob && !showStopOnToggle ? ' disabled' : '';
    const cancelAction = isRunningTarget ? `cancelMagnetCheck('${escapeJs(job.job_id)}')` : '';
    const primaryAction = `${startFn}(${targetArg})`;
    const primaryTitle = isRunningTarget
        ? (scope === 'movie' ? (isCancelling ? '正在终止检测' : '检测中') : '检测进度')
        : '检测磁力';
    const primaryIdleClass = scope === 'movie'
        ? 'h-5 w-6 rounded-l bg-emerald-50 text-[11px] font-bold leading-none text-emerald-700 hover:bg-emerald-100'
        : 'h-9 w-14 rounded-l border border-r-0 border-emerald-200 bg-emerald-50 text-xs font-bold text-emerald-700 shadow-sm hover:bg-emerald-100';
    const primaryDisabledClass = scope === 'movie'
        ? 'h-5 w-6 cursor-not-allowed rounded-l bg-slate-100 text-[10px] font-bold leading-none text-slate-500'
        : 'h-9 w-16 cursor-not-allowed rounded-l border border-r-0 border-slate-200 bg-slate-100 text-xs font-bold text-slate-500 shadow-sm';
    const primaryRunningClass = scope === 'movie'
        ? 'h-5 w-6 cursor-not-allowed rounded-l bg-slate-100 text-[10px] font-bold leading-none text-slate-500'
        : primaryDisabledClass;
    const toggleIdleClass = scope === 'movie'
        ? 'h-5 w-5 rounded-r border-l border-emerald-100 bg-emerald-50 text-[10px] font-bold leading-none text-emerald-700 hover:bg-emerald-100'
        : 'h-9 w-7 rounded-r border border-emerald-200 bg-emerald-50 text-xs font-bold text-emerald-700 shadow-sm hover:bg-emerald-100';
    const toggleDisabledClass = scope === 'movie'
        ? 'h-5 w-5 cursor-not-allowed rounded-r border-l border-slate-200 bg-slate-100 text-[10px] font-bold leading-none text-slate-400'
        : 'h-9 w-7 cursor-not-allowed rounded-r border border-slate-200 bg-slate-100 text-xs font-bold text-slate-400 shadow-sm';
    const primaryClass = isRunningTarget
        ? primaryRunningClass
        : (hasRunningJob ? primaryDisabledClass : primaryIdleClass);
    const stopToggleClass = scope === 'movie'
        ? 'inline-flex h-5 w-5 items-center justify-center rounded-r border-l border-red-100 bg-red-50 text-red-700 hover:bg-red-100 disabled:cursor-not-allowed disabled:border-slate-200 disabled:bg-slate-100 disabled:text-slate-400'
        : 'inline-flex h-9 w-7 items-center justify-center rounded-r border border-red-200 bg-red-50 text-red-700 shadow-sm hover:bg-red-100 disabled:cursor-not-allowed disabled:border-slate-200 disabled:bg-slate-100 disabled:text-slate-400';
    const toggleClass = showStopOnToggle ? stopToggleClass : (hasRunningJob ? toggleDisabledClass : toggleIdleClass);
    const spinnerClass = scope === 'movie' ? 'h-2.5 w-2.5' : 'h-3 w-3';
    const primaryContent = isRunningTarget
        ? (scope === 'movie'
            ? `<span class="inline-block ${spinnerClass} animate-spin rounded-full border-2 border-slate-300 border-t-slate-600"></span>`
            : `<span>${progress}</span>`)
        : (scope === 'movie' ? radarIcon : `<span>${progress}</span>`);
    const toggleAction = showStopOnToggle ? cancelAction : `${toggleFn}(${targetArg}, event)`;
    const toggleTitle = showStopOnToggle ? (isCancelling ? '正在终止检测' : '终止检测') : '更多检测选项';
    const toggleContent = showStopOnToggle
        ? (isCancelling ? `<span class="inline-block ${spinnerClass} animate-spin rounded-full border-2 border-slate-300 border-t-slate-600"></span>` : stopIcon)
        : (isOpen ? '▲' : '▼');
    return `
        <div id="${magnetCheckButtonId(scope, target)}" class="relative shrink-0" data-menu-root="magnet-check">
            <div class="inline-flex">
                <button${idAttr} type="button" onclick="${primaryAction}" title="${primaryTitle}" aria-label="${primaryTitle}"${primaryDisabledAttr} class="${primaryClass}">
                    <span class="inline-flex items-center justify-center gap-1">
                        ${primaryContent}
                    </span>
                </button>
                <button type="button" onclick="${toggleAction}" title="${toggleTitle}" aria-label="${toggleTitle}"${isCancelling ? ' disabled' : toggleDisabledAttr} class="${toggleClass}">${toggleContent}</button>
            </div>
            <div onclick="event.stopPropagation()" class="${isOpen && !hasRunningJob ? '' : 'hidden'} absolute right-0 z-30 mt-1 w-28 rounded border border-slate-200 bg-white p-1 text-xs shadow-lg">
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

async function renderExpandedCollectionPreservingMovie(movieId = expandedMovieId) {
    renderGlobalMagnetCheckButton();
    if (!expandedCollectionName) {
        return;
    }
    await ensureCollectionMovies(expandedCollectionName);
    const routeMovieId = databaseRouteParts()[1];
    if (movieId && routeMovieId) {
        await renderMagnetListPage(expandedCollectionName, movieId);
    } else {
        renderMovieListPage(expandedCollectionName);
    }
}

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
    await renderDatabaseRoute();
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
        await renderExpandedCollectionPreservingMovie(expandedMovieId);
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
    await renderExpandedCollectionPreservingMovie(expandedMovieId);
}

async function startAllMagnetCheck(_target = 'all', failedOnly = false) {
    openMagnetCheckMenu = null;
    renderGlobalMagnetCheckButton();
    const suffix = failedOnly ? '?failed_only=1' : '';
    const res = await apiFetch(`/api/magnets/check_all${suffix}`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200 && res.code !== 409) return showToast(res.msg || '检测启动失败');
    if (res.code === 409) showToast(res.msg || '磁力检测任务正在运行');
    watchMagnetCheckJob(res.data);
    await renderExpandedCollectionPreservingMovie(expandedMovieId);
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
        setDatabaseHash(collectionName);
        await ensureCollectionMovies(collectionName);
        return;
    }
    await ensureCollectionMovies(collectionName);
    await renderDatabaseRoute();
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
        const finishedJob = activeMagnetCheckJob;
        activeMagnetCheckJob = null;
        await refreshMagnetCheckTarget(finishedJob);
    }
}

async function cancelMagnetCheck(jobId) {
    if (!confirm('确定终止当前磁力检测吗？已完成的检测结果会保留。')) return;
    const res = await apiFetch(`/api/magnet_check_jobs/${encodeURIComponent(jobId)}/cancel`, { method: 'POST' }).then(r => r.json());
    if (res.code === 200) {
        activeMagnetCheckJob = res.data;
        await renderExpandedCollectionPreservingMovie(expandedMovieId);
    } else {
        showToast(res.msg || '终止检测失败');
    }
}

async function refreshMagnetCheckTarget(job) {
    if (job.scope === 'collection') {
        if (expandedCollectionName === job.target) {
            await reloadCollectionMovies(job.target);
        }
        renderGlobalMagnetCheckButton();
    }
    if (job.scope === 'movie') {
        const movieId = Number(job.target);
        const box = document.getElementById(`magnets-${movieId}`);
        const previouslyExpandedMovieId = expandedMovieId;
        let magnets = [];
        if (box && !box.classList.contains('hidden')) {
            magnets = await refreshMagnetRows(movieId);
        } else {
            const res = await apiFetch(`/api/movies/${movieId}/magnets`).then(r => r.json());
            magnets = res.data || [];
        }
        if (syncSelectedMagnetToMovie(movieId, magnets)) updateMovieSelectedName(movieId, magnets);
        await renderExpandedCollectionPreservingMovie(previouslyExpandedMovieId);
    }
    if (job.scope === 'all') {
        await loadCollections();
        updateRenderedMagnetCheckButtons();
    }
}
