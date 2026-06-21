/*
 * tasks.js — 任务配置、任务队列、任务监控
 * 包含起始 URL/标签处理、任务增删、队列控制、状态轮询与运行日志面板。
 */

/* ===== 监控轮询：setTimeout 链 + try/catch，避免请求堆积与静默失败 ===== */

function startMonitorPolling(intervalMs = 2000) {
    if (pollInterval) return;            // 已在轮询则保持原有节奏（等价于旧的 if(!pollInterval)）
    const tick = async () => {
        try {
            await refreshMonitor();
        } catch (err) {
            console.error('监控轮询出错:', err);
        } finally {
            // 仅当未被停止时才安排下一次，避免请求堆积
            if (pollInterval) pollInterval = setTimeout(tick, intervalMs);
        }
    };
    pollInterval = setTimeout(tick, intervalMs);
}

function stopMonitorPolling() {
    if (pollInterval) {
        clearTimeout(pollInterval);
        pollInterval = null;
    }
}

/* ===== 任务配置：起始 URL 与标签 ===== */

function prepareActorUrl(rawUrl) {
    const parsed = new URL(rawUrl);
    actorBaseUrl = `${parsed.origin}${parsed.pathname}`;
    actorBaseParams = new URLSearchParams(parsed.search);
    actorBaseParams.set('locale', 'zh');
    if (!actorBaseParams.has('sort_type')) actorBaseParams.set('sort_type', '0');
    actorBaseParams.delete('page');
    actorBaseParams.delete('t');
    return buildActorUrl();
}

function buildActorUrl(tagValues = null) {
    const params = new URLSearchParams(actorBaseParams);
    params.set('locale', 'zh');
    if (tagValues) params.set('t', tagValues);
    else params.delete('t');
    return `${actorBaseUrl}?${params.toString()}`;
}

async function fetchTags() {
    const urlInput = document.getElementById('start_url');
    if (!urlInput.value.trim()) return showToast('请先输入起始页面 URL');
    await saveRuntimeConfig(false);
    const normalizedUrl = prepareActorUrl(urlInput.value.trim());
    urlInput.value = normalizedUrl;
    selectedTags.clear();
    const res = await apiFetch('/api/get_tags', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: normalizedUrl })
    }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '获取标签失败');
    availableTags = res.data || [];
    tagsCollapsed = false;
    renderTags();
    document.getElementById('tags-wrapper').classList.toggle('hidden', availableTags.length === 0);
    renderTagsPanelState();
}

function renderTags() {
    const box = document.getElementById('tags-list');
    box.innerHTML = availableTags.map(tag => {
        const selected = selectedTags.has(tag.value);
        return `<button type="button" onclick="toggleTag('${escapeJs(tag.value)}')" class="px-3 py-1.5 rounded text-xs border transition-colors ${selected ? 'bg-primary text-white border-primary' : 'bg-surface text-[color:var(--c-text-muted)] border-[color:var(--c-border)] hover:bg-[color:var(--c-surface-sunken)]'}">${escapeHtml(tag.name)}</button>`;
    }).join('');
}

function renderTagsPanelState() {
    document.getElementById('tags-list').classList.toggle('hidden', tagsCollapsed);
    document.getElementById('tags-toggle-icon').innerText = tagsCollapsed ? '▼' : '▲';
}

function toggleTagsPanel() {
    tagsCollapsed = !tagsCollapsed;
    renderTagsPanelState();
}

function toggleTag(value) {
    if (selectedTags.has(value)) selectedTags.delete(value);
    else selectedTags.add(value);
    document.getElementById('start_url').value = selectedTags.size ? buildActorUrl(Array.from(selectedTags).join(',')) : buildActorUrl();
    renderTags();
}

/* ===== 运行日志面板 ===== */

let tasksLayoutFrame = null;
const TASK_LIST_MIN_HEIGHT = 34;
const EMPTY_TASK_LIST_MIN_HEIGHT = 44;

function scheduleFitTasksLayout() {
    if (tasksLayoutFrame) return;
    tasksLayoutFrame = requestAnimationFrame(() => {
        tasksLayoutFrame = null;
        fitTasksLayout();
    });
}

function renderLogPanelState() {
    const logContainer = document.getElementById('logContainer');
    logContainer.classList.toggle('hidden', logCollapsed);
    document.getElementById('log-toggle-icon').innerText = logCollapsed ? '▼' : '▲';
    if (!logCollapsed) {
        logContainer.style.height = '';
    }
    scheduleFitTasksLayout();
}

function pxValue(value) {
    const parsed = parseFloat(value);
    return Number.isFinite(parsed) ? parsed : 0;
}

function elementHeight(element) {
    if (!element) return 0;
    return Math.ceil(element.getBoundingClientRect().height || element.scrollHeight || 0);
}

function borderBlockSize(element) {
    if (!element) return 0;
    const style = window.getComputedStyle(element);
    return pxValue(style.borderTopWidth) + pxValue(style.borderBottomWidth);
}

function clamp(value, min, max) {
    return Math.max(min, Math.min(value, max));
}

function taskListMinimumHeight(taskList) {
    const count = Number(taskList?.dataset.visibleTaskCount || 0);
    const target = taskListTargetHeight(taskList);
    if (count <= 0) return Math.min(target || EMPTY_TASK_LIST_MIN_HEIGHT, EMPTY_TASK_LIST_MIN_HEIGHT);
    return Math.min(target || TASK_LIST_MIN_HEIGHT, TASK_LIST_MIN_HEIGHT);
}

function fitTasksViewHeight() {
    const view = document.getElementById('view-tasks');
    if (!view || view.classList.contains('hidden')) return;
    const rect = view.getBoundingClientRect();
    const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 0;
    const documentTop = rect.top + (window.scrollY || window.pageYOffset || 0);
    const bodyStyle = window.getComputedStyle(document.body);
    const bodyBottomPadding = parseFloat(bodyStyle.paddingBottom) || 0;
    const guard = 16;
    const available = Math.floor(viewportHeight - documentTop - bodyBottomPadding - guard);
    if (available <= 0) return available;
    view.style.height = `${available}px`;
    view.style.overflowY = 'hidden';
    view.style.overflowX = 'hidden';
    return available;
}

function fitTaskMonitorHeight() {
    const view = document.getElementById('view-tasks');
    if (!view || view.classList.contains('hidden')) return;
    const viewRect = view.getBoundingClientRect();
    const configPanel = document.getElementById('taskConfigPanel');
    const monitor = document.getElementById('taskMonitorCard');
    if (!configPanel || !monitor) return;
    const viewSplit = window.matchMedia('(min-width: 1280px)').matches;
    if (viewSplit) {
        view.style.gridTemplateRows = '';
        const monitorRect = monitor.getBoundingClientRect();
        const available = Math.floor(viewRect.bottom - monitorRect.top);
        monitor.style.height = `${Math.max(0, available)}px`;
        return;
    }

    const viewStyle = window.getComputedStyle(view);
    const gap = pxValue(viewStyle.rowGap);
    const configHeight = Math.ceil(Math.max(elementHeight(configPanel), configPanel.scrollHeight || 0));
    const monitorHeight = Math.max(0, Math.floor(viewRect.height - configHeight - gap));
    view.style.gridTemplateRows = `${configHeight}px minmax(0, ${monitorHeight}px)`;
    monitor.style.height = `${monitorHeight}px`;
}

function taskListTargetHeight(taskList) {
    const count = Number(taskList.dataset.visibleTaskCount || 0);
    if (count <= 0) return Math.max(44, Math.min(56, taskList.scrollHeight || 44));
    const rows = Array.from(taskList.children).slice(0, Math.min(count, 5));
    const measured = rows.reduce((sum, row) => sum + row.getBoundingClientRect().height, 0);
    if (measured > 0) return Math.ceil(measured);
    return Math.min(taskList.scrollHeight || 0, 5 * 44) || 44;
}

function fitLogPanelHeight() {
    // —— 1. 取元素引用 ——
    const logContainer = document.getElementById('logContainer');
    const monitorBody = document.getElementById('taskMonitorBody');
    const queuePanel = document.getElementById('taskQueuePanel');
    const queueToolbar = document.getElementById('taskQueueToolbar');
    const taskList = document.getElementById('task-list');
    const logPanel = document.getElementById('taskLogPanel');
    const logShell = document.getElementById('taskLogShell');
    const logHeader = logShell?.querySelector('button');
    const currentActions = document.getElementById('currentActions');
    if (!logContainer || !monitorBody || !queuePanel || !queueToolbar || !taskList || !logPanel || !logShell || !logHeader) return;

    // —— 2. 测量：可用高度、布局模式、列表目标/最小高度、日志面板固定开销 ——
    const bodyHeight = Math.max(0, Math.floor(monitorBody.clientHeight || monitorBody.getBoundingClientRect().height));
    const isSplitLayout = window.matchMedia('(min-width: 1024px)').matches;
    const targetListHeight = taskListTargetHeight(taskList);
    const minListHeight = taskListMinimumHeight(taskList);
    const queueToolbarHeight = elementHeight(queueToolbar);
    const logPanelStyle = window.getComputedStyle(logPanel);
    const logPanelPaddingY = pxValue(logPanelStyle.paddingTop) + pxValue(logPanelStyle.paddingBottom);
    const logPanelGap = pxValue(logPanelStyle.rowGap || logPanelStyle.gap);
    const actionVisible = currentActions && !currentActions.classList.contains('hidden');
    const actionHeight = actionVisible ? elementHeight(currentActions) : 0;
    const actionGap = actionVisible ? logPanelGap : 0;
    const logHeaderHeight = elementHeight(logHeader);
    const logFixedHeight = logPanelPaddingY + actionHeight + actionGap + logHeaderHeight + borderBlockSize(logShell);
    const logExpanded = !logContainer.classList.contains('hidden');

    taskList.style.maxHeight = 'none';
    queuePanel.style.overflow = 'hidden';
    logPanel.style.overflow = 'hidden';

    // —— 3a. 宽屏（lg+）：左右分栏，两栏各占满 body 高度 ——
    if (isSplitLayout) {
        monitorBody.style.gridTemplateRows = '';
        queuePanel.style.height = `${bodyHeight}px`;
        logPanel.style.height = `${bodyHeight}px`;
        taskList.style.height = `${Math.max(0, bodyHeight - queueToolbarHeight)}px`;
        logContainer.style.height = logExpanded ? `${Math.max(0, bodyHeight - logFixedHeight)}px` : '';
        return;
    }

    // —— 3b. 窄屏：上下堆叠，在队列列表与日志面板间分配高度预算 ——
    const desiredListHeight = Math.max(minListHeight, targetListHeight);
    let listHeight = desiredListHeight;
    let logContentHeight = 0;

    if (logExpanded) {
        const flexibleBudget = bodyHeight - queueToolbarHeight - logFixedHeight;
        if (flexibleBudget >= desiredListHeight) {
            logContentHeight = flexibleBudget - desiredListHeight;
        } else if (flexibleBudget >= minListHeight) {
            listHeight = Math.min(desiredListHeight, flexibleBudget);
        } else {
            listHeight = Math.min(minListHeight, Math.max(0, bodyHeight - queueToolbarHeight));
        }
    } else {
        const listBudget = bodyHeight - queueToolbarHeight - logFixedHeight;
        if (listBudget >= desiredListHeight) {
            listHeight = desiredListHeight;
        } else if (listBudget >= minListHeight) {
            listHeight = Math.min(desiredListHeight, listBudget);
        } else {
            listHeight = Math.min(minListHeight, Math.max(0, bodyHeight - queueToolbarHeight));
        }
    }

    const queueHeight = clamp(queueToolbarHeight + listHeight, 0, bodyHeight);
    const logPanelHeight = Math.max(0, bodyHeight - queueHeight);
    if (logExpanded) {
        logContentHeight = Math.max(0, Math.min(logContentHeight, logPanelHeight - logFixedHeight));
    }

    monitorBody.style.gridTemplateRows = `${queueHeight}px minmax(0, ${logPanelHeight}px)`;
    queuePanel.style.height = `${queueHeight}px`;
    logPanel.style.height = `${logPanelHeight}px`;
    taskList.style.maxHeight = 'none';
    taskList.style.height = `${Math.max(0, queueHeight - queueToolbarHeight)}px`;

    if (!logExpanded) {
        logContainer.style.height = '';
        return;
    }
    logContainer.style.height = `${logContentHeight}px`;
}

function fitTasksLayout() {
    fitTasksViewHeight();
    fitTaskMonitorHeight();
    fitLogPanelHeight();
}

function toggleLogPanel() {
    logCollapsed = !logCollapsed;
    renderLogPanelState();
}

/* ===== 任务增删与队列 ===== */

async function addTask(crawlMode = '') {
    const url = document.getElementById('start_url').value.trim();
    if (!url) return showToast('URL 不能为空');
    await saveRuntimeConfig(false);
    let proxy = '';
    try {
        proxy = getProxyValue();
    } catch (err) {
        return showToast(err.message);
    }
    const payload = {
        start_url: url,
        filename: document.getElementById('filename').value.trim(),
        crawl_mode: crawlMode,
        cookie: document.getElementById('cookie').value.trim(),
        remember_cookie: document.getElementById('remember_cookie').checked,
        user_agent: document.getElementById('user_agent').value.trim(),
        proxies: proxy
    };
    const response = await apiFetch('/api/tasks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });
    const res = await response.json();
    if (response.status === 409 && res.needs_mode) {
        const useIncremental = confirm(`检测到已有数据库集合：${displayName(res.filename)}\n点击“确定”使用增量，点击“取消”使用覆盖。`);
        return addTask(useIncremental ? 'incremental' : 'overwrite');
    }
    if (res.code !== 200) return showToast(res.msg || '添加任务失败');
    document.getElementById('filename').value = '';
    await refreshMonitor();
}

async function startQueue() {
    const res = await apiFetch('/api/tasks/start_queue', { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '无法启动队列');
    startMonitorPolling(2000);
    await refreshMonitor();
}

function progressPercent(progress) {
    const parts = String(progress || '0/0').split('/');
    return Number(parts[1]) ? Math.min(100, Math.round(Number(parts[0]) / Number(parts[1]) * 100)) : 0;
}

function isFinishedTask(task) {
    return isTerminalTaskState(task.state);
}

async function loadTasks() {
    const [tasksRes, queueRes] = await Promise.all([
        apiFetch('/api/tasks').then(r => r.json()),
        apiFetch('/api/tasks/queue_status').then(r => r.json())
    ]);
    tasksCache = tasksRes.data || [];
    queueStatus = queueRes.data || {};
    renderTaskList();
    renderQueueControls();
}

function renderQueueControls() {
    const btn = document.getElementById('startQueueBtn');
    btn.disabled = !queueStatus.can_start;
    const startTitle = queueStatus.queue_state === 'running' ? '队列运行中' : '开始任务队列';
    btn.title = startTitle;
    btn.setAttribute('aria-label', startTitle);
    const activeCount = queueStatus.active_count || 0;
    const finishedCount = queueStatus.finished_count || 0;
    const queueSummary = document.getElementById('queueSummary');
    queueSummary.title = `待处理 ${activeCount} 个 · 已结束 ${finishedCount} 个 · ${queueStatus.queue_state || 'idle'}`;
    queueSummary.innerHTML = `<span>待处理 ${activeCount} 个</span><span>已结束 ${finishedCount} 个</span>`;
    const toggleFinishedBtn = document.getElementById('toggleFinishedBtn');
    const toggleTitle = showFinishedTasks ? '隐藏已结束' : '显示已结束';
    toggleFinishedBtn.title = toggleTitle;
    toggleFinishedBtn.setAttribute('aria-label', toggleTitle);
    document.getElementById('cleanupFinishedBtn').disabled = finishedCount <= 0;
    renderQueueTaskControl(currentTask());
}

function renderQueueTaskControl(task) {
    const slot = document.getElementById('queueTaskControlSlot');
    if (!slot) return;
    if (!task) {
        slot.innerHTML = '';
        return;
    }
    const id = escapeJs(task.task_id);
    const cancelButton = isCancelableTaskState(task.state)
        ? `
            <button type="button" onclick="cancelTask('${id}')" title="取消当前任务" aria-label="取消当前任务" class="btn btn-icon-lg btn-danger">
                <svg aria-hidden="true" viewBox="0 0 24 24" class="h-5 w-5" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="12" cy="12" r="9"></circle>
                    <path d="M9 9l6 6"></path>
                    <path d="M15 9l-6 6"></path>
                </svg>
            </button>`
        : '';
    if (isPausableTaskState(task.state)) {
        slot.innerHTML = `
            <div class="flex gap-2">
                <button type="button" onclick="pauseTask('${id}')" title="暂停当前任务" aria-label="暂停当前任务" class="btn btn-icon-lg btn-warning">
                    <svg aria-hidden="true" viewBox="0 0 24 24" class="h-5 w-5" fill="currentColor">
                        <path d="M7 5h4v14H7z"></path>
                        <path d="M13 5h4v14h-4z"></path>
                    </svg>
                </button>
                ${cancelButton}
            </div>`;
        return;
    }
    if (isResumableTaskState(task.state)) {
        slot.innerHTML = `
            <div class="flex gap-2">
                <button type="button" onclick="resumeTaskById('${id}')" title="恢复当前任务" aria-label="恢复当前任务" class="btn btn-icon-lg btn-info">
                    <svg aria-hidden="true" viewBox="0 0 24 24" class="h-5 w-5" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M4 7v6h6"></path>
                        <path d="M20 17a8 8 0 0 0-13.66-5.66L4 13"></path>
                    </svg>
                </button>
                ${cancelButton}
            </div>`;
        return;
    }
    slot.innerHTML = '';
}

function taskActions(task, options = {}) {
    const id = escapeJs(task.task_id);
    const actions = [];
    if (!options.hidePauseResume && isPausableTaskState(task.state)) actions.push(`<button onclick="pauseTask('${id}')" class="btn btn-sm btn-warning">暂停</button>`);
    if (!options.hidePauseResume && isResumableTaskState(task.state)) actions.push(`<button onclick="resumeTaskById('${id}')" class="btn btn-sm btn-info">恢复</button>`);
    if (task.state === 'waiting_cookie') actions.push(`<button onclick="refreshCookie('${id}')" class="btn btn-sm btn-warning">读安卓 Cookie</button>`);
    if (task.state === 'waiting_choice') {
        actions.push(`<button onclick="setTaskModeById('${id}', 'incremental')" class="btn btn-sm btn-info">增量</button>`);
        actions.push(`<button onclick="setTaskModeById('${id}', 'overwrite')" class="btn btn-sm btn-danger">覆盖</button>`);
    }
    if (!options.hideCancel && isCancelableTaskState(task.state)) actions.push(`<button onclick="cancelTask('${id}')" class="btn btn-sm btn-danger">取消</button>`);
    return actions.join(' ');
}

function renderTaskList() {
    const box = document.getElementById('task-list');
    const visibleTasks = showFinishedTasks ? tasksCache : tasksCache.filter(task => !isFinishedTask(task));
    box.dataset.visibleTaskCount = String(visibleTasks.length);
    if (!visibleTasks.length) {
        box.innerHTML = '<div class="empty-state p-4">暂无任务</div>';
        scheduleFitTasksLayout();
        return;
    }
    box.innerHTML = visibleTasks.map(task => {
        const rawName = task.final_filename || task.filename || '自动命名';
        const taskId = escapeJs(task.task_id);
        const pct = progressPercent(task.progress);
        const progressClass = progressBarClass(task.state);
        const copyIncrementalBtn = task.can_copy_incremental_magnets
            ? `<button onclick="copyTaskIncrementalMagnets('${taskId}')" title="复制新增影片磁力" aria-label="复制新增影片磁力" class="btn btn-info h-6 w-6 p-0 text-sm">⧉</button>`
            : '';
        return `
        <div class="relative grid grid-cols-[minmax(0,1fr)_42px_72px_52px] items-center gap-1 overflow-hidden px-3 py-1 text-xs ${task.task_id === queueStatus.current_task_id ? 'bg-info-soft' : 'bg-surface'}">
            <div class="min-w-0">
                <div class="truncate font-bold text-xs leading-tight" title="${escapeHtml(displayName(rawName))}">${escapeHtml(displayName(rawName))}</div>
                <div class="truncate font-mono text-[10px] leading-tight text-[color:var(--c-text-subtle)]">${escapeHtml((task.task_id || '').slice(0, 8))}</div>
            </div>
            <div class="font-mono text-[color:var(--c-text-muted)] text-right">${escapeHtml(task.progress || '0/0')}</div>
            <div class="text-right">
                <span class="badge w-[72px] ${stateClass(task.state)}">${stateLabel(task.state)}</span>
            </div>
            <div class="flex justify-end gap-1">
                ${copyIncrementalBtn}
                <button onclick="deleteTaskById('${taskId}')" title="删除任务" aria-label="删除任务" class="btn btn-danger h-6 w-6 p-0 text-sm">×</button>
            </div>
            <div class="absolute inset-x-0 bottom-0 h-[2px] bg-[color:var(--c-bg)]">
                <div class="h-full ${progressClass} transition-all duration-300" style="width:${pct}%"></div>
            </div>
        </div>
    `}).join('');
    scheduleFitTasksLayout();
}

function toggleFinishedTasks() {
    showFinishedTasks = !showFinishedTasks;
    renderTaskList();
    renderQueueControls();
}

async function cleanupFinishedTasks() {
    const finishedCount = queueStatus?.finished_count || 0;
    if (finishedCount <= 0) return;
    if (!confirm(`确定清理 ${finishedCount} 个已结束任务吗？\n不会删除数据库集合或已爬取数据。`)) return;
    const res = await apiFetch('/api/tasks/cleanup', { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '清理失败');
    showToast(res.msg || '已清理已结束任务');
    await refreshMonitor();
}

function currentTask() {
    return tasksCache.find(t => t.task_id === queueStatus.current_task_id)
        || tasksCache.find(t => ['running', 'pause_requested', 'cancel_requested', 'waiting_cookie', 'waiting_choice', 'paused'].includes(t.state))
        || null;
}

async function pollStatus() {
    const data = await apiFetch('/api/status').then(r => r.json());
    renderCurrentTask(data);
}

function renderCurrentTask(data) {
    const task = currentTask();
    const logs = data.logs || [];
    document.getElementById('logContainer').innerHTML = logs.length
        ? logs.map(log => `<div class="mb-1 border-b border-[color:var(--c-border-soft)] pb-1">${escapeHtml(log)}</div>`).join('')
        : '<div>等待任务启动...</div>';
    renderCurrentActions(task);
}

function renderCurrentActions(task) {
    const box = document.getElementById('currentActions');
    const wasHidden = box.classList.contains('hidden');
    if (!task) {
        box.innerHTML = '';
        box.classList.add('hidden');
        renderQueueTaskControl(null);
        if (!wasHidden) scheduleFitTasksLayout();
        return;
    }
    box.innerHTML = taskActions(task, { hidePauseResume: true, hideCancel: true });
    box.classList.toggle('hidden', !box.innerHTML.trim());
    renderQueueTaskControl(task);
    const isHidden = box.classList.contains('hidden');
    if (wasHidden !== isHidden) scheduleFitTasksLayout();
}

async function refreshMonitor() {
    await loadTasks();
    await pollStatus();
}

async function pauseTask(taskId) {
    await taskAction(taskId, 'pause');
}

async function cancelTask(taskId) {
    if (!confirm('确定取消这个任务吗？')) return;
    await taskAction(taskId, 'cancel');
}

async function deleteTaskById(taskId) {
    if (!confirm('删除这个任务记录吗？不会删除数据库集合或已爬取数据。')) return;
    await taskAction(taskId, '', { method: 'DELETE', path: `/api/tasks/${encodeURIComponent(taskId)}` });
}

async function resumeTaskById(taskId) {
    await saveRuntimeConfig(false);
    await taskAction(taskId, 'resume');
    startMonitorPolling(2000);
}

async function refreshCookie(taskId) {
    await taskAction(taskId, 'refresh_cookie');
    startMonitorPolling(2000);
}

async function setTaskModeById(taskId, mode) {
    await taskAction(taskId, 'mode', { body: { mode } });
    startMonitorPolling(2000);
}

async function taskAction(taskId, action, options = {}) {
    const method = options.method || 'POST';
    const path = options.path || `/api/tasks/${encodeURIComponent(taskId)}/${action}`;
    try {
        const requestOptions = { method };
        if (options.body !== undefined) {
            requestOptions.headers = { 'Content-Type': 'application/json' };
            requestOptions.body = JSON.stringify(options.body);
        }
        const res = await apiFetchJson(path, requestOptions);
        if (res.code !== 200) return showToast(res.msg || '操作失败');
        if (res.msg) showToast(res.msg);
        await refreshMonitor();
    } catch (err) {
        showToast(err.message || '操作失败');
    }
}

async function copyTaskIncrementalMagnets(taskId) {
    try {
        const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}/incremental_magnets`).then(r => r.json());
        if (res.code !== 200) return showToast(res.msg || '读取失败');
        const links = res.data || [];
        if (!links.length) return showToast('暂无新增影片磁力可复制');
        const copied = await copyText(links.join('\n'));
        showToast(copied ? `已复制 ${links.length} 条新增影片磁力` : '自动复制失败，请在弹窗中手动复制新增影片磁力');
    } catch (err) {
        console.error(err);
        showToast(err.message || '复制失败');
    }
}

async function clearLogs() {
    const res = await apiFetch('/api/clear_logs', { method: 'POST' }).then(r => r.json());
    showToast(res.msg);
    await refreshMonitor();
}
