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

// 各 scope 的差异：启动 / 切换函数名、目标参数是否加引号、按钮尺寸档。
const MAGNET_CHECK_SCOPE = {
    movie:      { startFn: 'startMovieMagnetCheck',      toggleFn: 'toggleMovieMagnetCheckMenu',      size: 'mini' },
    collection: { startFn: 'startCollectionMagnetCheck', toggleFn: 'toggleCollectionMagnetCheckMenu', size: 'toolbar' },
    all:        { startFn: 'startAllMagnetCheck',        toggleFn: 'toggleAllMagnetCheckMenu',        size: 'std'  },
    ranking:    { startFn: 'startRankingMagnetCheckByTarget', toggleFn: 'toggleRankingMagnetCheckMenuByTarget', size: 'toolbar' },
};

// 尺寸档：主按钮 / 副按钮的宽高与字号（配色由 .btn-split-* 组件类负责）。
const MAGNET_CHECK_SIZE = {
    mini: { primary: 'h-5 w-6 text-[11px] leading-none', toggle: 'h-5 w-5 text-[10px] leading-none', spinner: 'h-2.5 w-2.5' },
    std:  { primary: 'h-9 w-14 text-xs shadow-sm',        toggle: 'h-9 w-7 text-xs shadow-sm',         spinner: 'h-3 w-3'    },
    toolbar: { primary: 'h-7 w-7 text-[11px] leading-none', toggle: 'h-7 w-6 text-[10px] leading-none', spinner: 'h-3 w-3' },
};

const MAGNET_RADAR_ICON = `<svg aria-hidden="true" viewBox="0 0 24 24" class="h-3.5 w-3.5" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <circle cx="12" cy="12" r="9"></circle>
                        <path d="M12 12l6-4"></path>
                        <path d="M12 3v2"></path>
                        <path d="M12 19v2"></path>
                        <path d="M3 12h2"></path>
                        <path d="M19 12h2"></path>
                        <path d="M8.5 8.5a5 5 0 0 1 7 0"></path>
                    </svg>`;
const MAGNET_STOP_ICON = `<svg aria-hidden="true" viewBox="0 0 24 24" class="h-3 w-3" fill="currentColor">
                        <rect x="6" y="6" width="12" height="12" rx="1.5"></rect>
                    </svg>`;

function magnetSpinner(sizeClass) {
    return `<span class="inline-block ${sizeClass} animate-spin rounded-full border-2 border-slate-300 border-t-slate-600"></span>`;
}

// 计算按钮的派生状态（纯函数，无 DOM）。
function magnetCheckButtonState(scope, target) {
    const job = activeMagnetCheckJob;
    const hasRunningJob = !!(job && job.running);
    const isRunningTarget = !!(hasRunningJob && job.scope === scope && String(job.target) === String(target));
    const isCancelling = !!(isRunningTarget && job.cancelled);
    return {
        key: magnetCheckMenuKey(scope, target),
        isOpen: openMagnetCheckMenu === magnetCheckMenuKey(scope, target),
        job,
        hasRunningJob,
        isRunningTarget,
        isCancelling,
        progress: isRunningTarget ? `${Number(job.completed || 0)}/${Number(job.total || 0)}` : 'check',
    };
}

// 主按钮：空闲时启动检测；运行中显示进度（collection/all）或 spinner（movie）。
function renderMagnetCheckPrimary(scope, target, st) {
    const cfg = MAGNET_CHECK_SCOPE[scope];
    const size = MAGNET_CHECK_SIZE[cfg.size];
    const targetArg = scope === 'movie' ? target : `'${escapeJs(target)}'`;
    const idAttr = scope === 'movie' ? ` id="check-movie-${target}"` : '';
    const disabledAttr = (st.hasRunningJob && !st.isRunningTarget) || st.isCancelling ? ' disabled' : '';
    const title = st.isRunningTarget
        ? (scope === 'movie' ? (st.isCancelling ? '正在终止检测' : '检测中') : '检测进度')
        : '检测磁力';
    const content = st.isRunningTarget
        ? (scope === 'movie' ? magnetSpinner(size.spinner) : `<span>${st.progress}</span>`)
        : MAGNET_RADAR_ICON;
    return `<button${idAttr} type="button" onclick="${cfg.startFn}(${targetArg})" title="${title}" aria-label="${title}"${disabledAttr} class="btn-split-primary ${size.primary}">
                    <span class="inline-flex items-center justify-center gap-1">${content}</span>
                </button>`;
}

// 副按钮：运行中变为「终止检测」（红色/spinner）；空闲时为「更多选项」下拉箭头。
function renderMagnetCheckToggle(scope, target, st) {
    const cfg = MAGNET_CHECK_SCOPE[scope];
    const size = MAGNET_CHECK_SIZE[cfg.size];
    const targetArg = scope === 'movie' ? target : `'${escapeJs(target)}'`;
    if (st.isRunningTarget) {
        const action = `cancelMagnetCheck('${escapeJs(st.job.job_id)}')`;
        const title = st.isCancelling ? '正在终止检测' : '终止检测';
        const content = st.isCancelling ? magnetSpinner(size.spinner) : MAGNET_STOP_ICON;
        return `<button type="button" onclick="${action}" title="${title}" aria-label="${title}"${st.isCancelling ? ' disabled' : ''} class="btn-split-stop ${size.toggle}">${content}</button>`;
    }
    const disabledAttr = st.hasRunningJob ? ' disabled' : '';
    const content = st.isOpen ? '▲' : '▼';
    return `<button type="button" onclick="${cfg.toggleFn}(${targetArg}, event)" title="更多检测选项" aria-label="更多检测选项"${disabledAttr} class="btn-split-toggle ${size.toggle}">${content}</button>`;
}

function renderMagnetCheckButton(scope, target) {
    const cfg = MAGNET_CHECK_SCOPE[scope];
    const st = magnetCheckButtonState(scope, target);
    const targetArg = scope === 'movie' ? target : `'${escapeJs(target)}'`;
    return `
        <div id="${magnetCheckButtonId(scope, target)}" class="relative shrink-0" data-menu-root="magnet-check">
            <div class="inline-flex">
                ${renderMagnetCheckPrimary(scope, target, st)}
                ${renderMagnetCheckToggle(scope, target, st)}
            </div>
            <div onclick="event.stopPropagation()" class="menu ${st.isOpen && !st.hasRunningJob ? '' : 'hidden'} right-0 w-28 text-xs">
                <button type="button" onclick="${cfg.startFn}(${targetArg}, true)" class="menu-item font-bold text-[color:var(--c-neutral-text)]">check failed</button>
            </div>
        </div>`;
}

function updateRenderedMagnetCheckButtons() {
    renderGlobalMagnetCheckButton();
    if (expandedCollectionName) {
        const collectionButton = document.getElementById(magnetCheckButtonId('collection', expandedCollectionName));
        if (collectionButton) collectionButton.outerHTML = renderMagnetCheckButton('collection', expandedCollectionName);
    }
    const movieButtonTarget = activeMagnetCheckJob && activeMagnetCheckJob.scope === 'movie'
        ? activeMagnetCheckJob.target
        : expandedMovieId;
    if (movieButtonTarget) {
        const movieButton = document.getElementById(magnetCheckButtonId('movie', movieButtonTarget));
        if (movieButton) movieButton.outerHTML = renderMagnetCheckButton('movie', movieButtonTarget);
    }
    const rankingButtonTarget = activeMagnetCheckJob && activeMagnetCheckJob.scope === 'ranking'
        ? activeMagnetCheckJob.target
        : currentRankingMagnetCheckTarget();
    if (rankingButtonTarget) {
        const rankingButton = document.getElementById(magnetCheckButtonId('ranking', rankingButtonTarget));
        if (rankingButton) rankingButton.outerHTML = renderMagnetCheckButton('ranking', rankingButtonTarget);
    }
}

function renderGlobalMagnetCheckButton() {
    const slot = document.getElementById('globalMagnetCheckSlot');
    if (!slot) return;
    slot.innerHTML = renderMagnetCheckButton('all', 'all');
}

function currentRankingMagnetCheckTarget() {
    const route = databaseRouteInfo();
    if (route.type !== DATABASE_TYPE_RANKING || !route.category || !route.period) return '';
    return `${route.category}:${route.period}`;
}

function currentRankingMovieCheckTarget() {
    const route = databaseRouteInfo();
    if (route.type !== DATABASE_TYPE_RANKING || !route.movieId) return '';
    return String(route.movieId);
}

async function renderExpandedCollectionPreservingMovie(movieId = expandedMovieId) {
    renderGlobalMagnetCheckButton();
    if (!expandedCollectionName) {
        return;
    }
    await ensureCollectionMovies(expandedCollectionName);
    const routeMovieId = currentDatabaseMovieId();
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
    openExclusiveMenu('check', key, () => renderCollectionBody(collectionName));
}

function toggleAllMagnetCheckMenu(_target = 'all', event = null) {
    if (event) event.stopPropagation();
    const key = magnetCheckMenuKey('all', 'all');
    openExclusiveMenu('check', key, () => renderGlobalMagnetCheckButton());
}

async function toggleMovieMagnetCheckMenu(movieId, event = null) {
    if (event) event.stopPropagation();
    const key = magnetCheckMenuKey('movie', movieId);
    openExclusiveMenu('check', key, null);
    if (activeView === 'database') await renderDatabaseRoute();
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
    } else if (activeView === 'database' && currentRankingMovieCheckTarget() === String(movieId)) {
        await renderDatabaseRoute();
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
    } else if (runningJob && activeView === 'database' && activeMagnetCheckJob.scope === 'movie' && currentRankingMovieCheckTarget() === String(activeMagnetCheckJob.target)) {
        updateRenderedMagnetCheckButtons();
        await refreshMagnetRows(activeMagnetCheckJob.target);
    } else if (runningJob && activeView === 'database' && activeMagnetCheckJob.scope === 'ranking') {
        if (currentRankingMagnetCheckTarget() === activeMagnetCheckJob.target) {
            await renderDatabaseRoute();
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
        if (activeMagnetCheckJob.scope === 'ranking' || (activeMagnetCheckJob.scope === 'movie' && currentRankingMovieCheckTarget() === String(activeMagnetCheckJob.target))) {
            await renderDatabaseRoute();
        } else {
            await renderExpandedCollectionPreservingMovie(expandedMovieId);
        }
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
        const actorSynced = syncSelectedMagnetToMovie(movieId, magnets);
        const rankingSynced = syncSelectedMagnetToRankingMovie(movieId, magnets);
        if (actorSynced || rankingSynced) updateMovieSelectedName(movieId, magnets);
        const rankingMeta = currentRankingRouteMeta();
        if (activeView === 'database' && rankingMeta && rankingMeta.movieId && String(rankingMeta.movieId) === String(movieId)) {
            await reloadRankingMovies(rankingMeta.category, rankingMeta.period);
        } else {
            await renderExpandedCollectionPreservingMovie(previouslyExpandedMovieId);
        }
    }
    if (job.scope === 'all') {
        await loadCollections();
        updateRenderedMagnetCheckButtons();
    }
    if (job.scope === 'ranking') {
        const parts = String(job.target || '').split(':');
        const category = rankingCategoryMeta(parts[0]);
        const period = rankingPeriodMeta(parts[1]);
        if (category && period) {
            delete rankingMovieCache[rankingCacheKey(category, period)];
        }
        if (activeView === 'database' && currentRankingMagnetCheckTarget() === job.target) {
            await renderDatabaseRoute();
        }
    }
}
