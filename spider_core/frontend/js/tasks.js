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
        return `<button type="button" onclick="toggleTag('${escapeJs(tag.value)}')" class="px-3 py-1.5 rounded text-xs border ${selected ? 'bg-indigo-600 text-white border-indigo-600' : 'bg-white text-slate-600 border-slate-300'}">${escapeHtml(tag.name)}</button>`;
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

function renderLogPanelState() {
    document.getElementById('logContainer').classList.toggle('hidden', logCollapsed);
    document.getElementById('log-toggle-icon').innerText = logCollapsed ? '▼' : '▲';
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

function stateLabel(state) {
    return {
        pending: '排队中',
        running: '运行中',
        pause_requested: '暂停中',
        paused: '已暂停',
        waiting_cookie: '等待 Cookie',
        waiting_choice: '等待模式',
        cancel_requested: '取消中',
        canceled: '已取消',
        finished: '已完成',
        failed: '失败'
    }[state] || state || '-';
}

function stateClass(state) {
    if (state === 'running') return 'bg-blue-50 text-blue-700';
    if (state === 'pending') return 'bg-slate-100 text-slate-700';
    if (state === 'finished') return 'bg-green-50 text-green-700';
    if (state === 'waiting_cookie') return 'bg-orange-50 text-orange-700';
    if (state === 'waiting_choice') return 'bg-purple-50 text-purple-700';
    if (state === 'paused' || state === 'pause_requested') return 'bg-amber-50 text-amber-700';
    if (state === 'failed' || state === 'canceled' || state === 'cancel_requested') return 'bg-red-50 text-red-700';
    return 'bg-slate-100 text-slate-700';
}

function isFinishedTask(task) {
    return ['finished', 'canceled', 'failed'].includes(task.state);
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
    btn.innerText = queueStatus.queue_state === 'running' ? '队列运行中' : '开始任务队列';
    const activeCount = queueStatus.active_count || 0;
    const finishedCount = queueStatus.finished_count || 0;
    document.getElementById('queueSummary').innerText = `待处理 ${activeCount} 个 · 已结束 ${finishedCount} 个 · ${queueStatus.queue_state || 'idle'}`;
    document.getElementById('toggleFinishedBtn').innerText = showFinishedTasks ? '隐藏已结束' : '显示已结束';
    document.getElementById('cleanupFinishedBtn').disabled = finishedCount <= 0;
}

function taskActions(task) {
    const id = escapeJs(task.task_id);
    const actions = [];
    if (['running', 'pending', 'pause_requested'].includes(task.state)) actions.push(`<button onclick="pauseTask('${id}')" class="text-xs px-2 py-1 rounded bg-amber-50 text-amber-700 font-bold">暂停</button>`);
    if (['paused', 'waiting_cookie', 'waiting_choice'].includes(task.state)) actions.push(`<button onclick="resumeTaskById('${id}')" class="text-xs px-2 py-1 rounded bg-blue-50 text-blue-700 font-bold">恢复</button>`);
    if (task.state === 'waiting_cookie') actions.push(`<button onclick="refreshCookie('${id}')" class="text-xs px-2 py-1 rounded bg-orange-50 text-orange-700 font-bold">读安卓 Cookie</button>`);
    if (task.state === 'waiting_choice') {
        actions.push(`<button onclick="setTaskModeById('${id}', 'incremental')" class="text-xs px-2 py-1 rounded bg-purple-50 text-purple-700 font-bold">增量</button>`);
        actions.push(`<button onclick="setTaskModeById('${id}', 'overwrite')" class="text-xs px-2 py-1 rounded bg-red-50 text-red-700 font-bold">覆盖</button>`);
    }
    if (['pending', 'running', 'pause_requested', 'paused', 'waiting_cookie', 'waiting_choice'].includes(task.state)) actions.push(`<button onclick="cancelTask('${id}')" class="text-xs px-2 py-1 rounded bg-red-50 text-red-700 font-bold">取消</button>`);
    return actions.join(' ');
}

function renderTaskList() {
    const box = document.getElementById('task-list');
    const visibleTasks = showFinishedTasks ? tasksCache : tasksCache.filter(task => !isFinishedTask(task));
    if (!visibleTasks.length) {
        box.innerHTML = '<div class="p-4 text-center text-slate-400 text-sm">暂无任务</div>';
        return;
    }
    box.innerHTML = visibleTasks.map(task => {
        const rawName = task.final_filename || task.filename || '自动命名';
        const taskId = escapeJs(task.task_id);
        const copyIncrementalBtn = task.can_copy_incremental_magnets
            ? `<button onclick="copyTaskIncrementalMagnets('${taskId}')" title="复制新增影片磁力" aria-label="复制新增影片磁力" class="inline-flex h-6 w-6 items-center justify-center rounded bg-blue-50 text-sm font-bold text-blue-700 hover:bg-blue-100">⧉</button>`
            : '';
        return `
        <div class="grid grid-cols-[minmax(0,1fr)_42px_72px_52px] items-center gap-1 px-3 py-2 text-xs ${task.task_id === queueStatus.current_task_id ? 'bg-blue-50' : 'bg-white'}">
            <div class="min-w-0">
                <div class="truncate font-bold text-xs leading-tight" title="${escapeHtml(displayName(rawName))}">${escapeHtml(displayName(rawName))}</div>
                <div class="truncate font-mono text-[10px] leading-tight text-slate-400">${escapeHtml((task.task_id || '').slice(0, 8))}</div>
            </div>
            <div class="font-mono text-slate-600 text-right">${escapeHtml(task.progress || '0/0')}</div>
            <div class="text-right">
                <span class="inline-flex w-[72px] justify-center px-1.5 py-0.5 rounded text-xs font-bold ${stateClass(task.state)}">${stateLabel(task.state)}</span>
            </div>
            <div class="flex justify-end gap-1">
                ${copyIncrementalBtn}
                <button onclick="deleteTaskById('${taskId}')" title="删除任务" aria-label="删除任务" class="inline-flex h-6 w-6 items-center justify-center rounded bg-red-50 text-sm font-bold text-red-700 hover:bg-red-100">×</button>
            </div>
        </div>
    `}).join('');
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
    const title = task ? displayName(task.final_filename || task.filename || '当前任务') : '空闲';
    document.getElementById('currentTaskTitle').innerText = title;
    document.getElementById('currentTaskMeta').innerText = task ? `${stateLabel(task.state)} · ${task.start_url || ''}` : '没有正在执行或阻塞的任务';
    document.getElementById('statusLabel').innerText = `状态: ${stateLabel(data.state)}`;
    document.getElementById('progressText').innerText = data.progress || '0/0';
    document.getElementById('currentCode').innerText = data.current || '-';
    const parts = String(data.progress || '0/0').split('/');
    const pct = Number(parts[1]) ? Math.min(100, Math.round(Number(parts[0]) / Number(parts[1]) * 100)) : 0;
    document.getElementById('progressBar').style.width = `${pct}%`;
    const logs = data.logs || [];
    document.getElementById('logContainer').innerHTML = logs.length
        ? logs.map(log => `<div class="mb-1 border-b border-slate-100 pb-1">${escapeHtml(log)}</div>`).join('')
        : '<div>等待任务启动...</div>';
    renderCurrentActions(task);
}

function renderCurrentActions(task) {
    const box = document.getElementById('currentActions');
    if (!task) {
        box.innerHTML = '';
        return;
    }
    box.innerHTML = taskActions(task);
}

async function refreshMonitor() {
    await loadTasks();
    await pollStatus();
}

async function pauseTask(taskId) {
    const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}/pause`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) showToast(res.msg);
    await refreshMonitor();
}

async function cancelTask(taskId) {
    if (!confirm('确定取消这个任务吗？')) return;
    const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}/cancel`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) showToast(res.msg);
    await refreshMonitor();
}

async function deleteTaskById(taskId) {
    if (!confirm('删除这个任务记录吗？不会删除数据库集合或已爬取数据。')) return;
    const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}`, { method: 'DELETE' }).then(r => r.json());
    if (res.code !== 200) return showToast(res.msg || '删除任务失败');
    await refreshMonitor();
}

async function resumeTaskById(taskId) {
    await saveRuntimeConfig(false);
    const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}/resume`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) showToast(res.msg);
    startMonitorPolling(2000);
    await refreshMonitor();
}

async function refreshCookie(taskId) {
    const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}/refresh_cookie`, { method: 'POST' }).then(r => r.json());
    if (res.code !== 200) showToast(res.msg);
    startMonitorPolling(2000);
    await refreshMonitor();
}

async function setTaskModeById(taskId, mode) {
    const res = await apiFetch(`/api/tasks/${encodeURIComponent(taskId)}/mode`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ mode })
    }).then(r => r.json());
    if (res.code !== 200) showToast(res.msg);
    startMonitorPolling(2000);
    await refreshMonitor();
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
