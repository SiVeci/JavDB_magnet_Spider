/*
 * meta.js — 状态元数据单一事实源
 * 任务状态 / 磁力检测状态 / 集合健康度 的「值 → 文案 / 配色」映射集中于此，
 * 消除原先散落在 tasks.js、movies.js、magnets.js 里的重复映射逻辑。
 *
 * 以普通脚本加载（非 ES Module），所有函数挂全局作用域，
 * 加载顺序在 utils.js 之后、state.js 之前（纯数据 + 纯函数，无外部依赖）。
 *
 * 配色一律返回设计系统的语义组件类（badge-* / bg-*），不再返回裸 Tailwind 原子类，
 * 调用点统一为 class="badge ${...}" 形式。
 */

/* ===== 任务状态 ===== */
// badge: 徽章语义类（配 .badge 使用）；bar: 进度条填充色（语义 bg-* 类）
const TASK_STATE_META = {
    pending:          { label: '排队中',      badge: 'badge-neutral', bar: 'bg-info' },
    running:          { label: '运行中',      badge: 'badge-info',    bar: 'bg-info' },
    pause_requested:  { label: '暂停中',      badge: 'badge-warning', bar: 'bg-warning' },
    paused:           { label: '已暂停',      badge: 'badge-warning', bar: 'bg-warning' },
    waiting_cookie:   { label: '等待 Cookie', badge: 'badge-warning', bar: 'bg-info' },
    waiting_choice:   { label: '等待模式',    badge: 'badge-info',    bar: 'bg-info' },
    cancel_requested: { label: '取消中',      badge: 'badge-danger',  bar: 'bg-danger' },
    canceled:         { label: '已取消',      badge: 'badge-danger',  bar: 'bg-danger' },
    finished:         { label: '已完成',      badge: 'badge-success', bar: 'bg-success' },
    failed:           { label: '失败',        badge: 'badge-danger',  bar: 'bg-danger' },
};

function taskStateMeta(state) {
    return TASK_STATE_META[state] || { label: state || '-', badge: 'badge-neutral', bar: 'bg-info' };
}

/* ===== 任务状态分组 =====
 * 此前 ['finished','canceled','failed'] 等状态数组散落在 tasks.js 多处，易漂移。
 * 集中为命名常量 + 谓词，调用点统一走 isXxxTaskState()。 */
const TERMINAL_TASK_STATES = ['finished', 'canceled', 'failed'];     // 已结束（不可再操作）
const PAUSABLE_TASK_STATES = ['running', 'pending', 'pause_requested']; // 可暂停
const RESUMABLE_TASK_STATES = ['paused', 'waiting_cookie', 'waiting_choice']; // 可恢复
// 可取消 = 可暂停 ∪ 可恢复（派生避免与上面漂移）
const CANCELABLE_TASK_STATES = [...PAUSABLE_TASK_STATES, ...RESUMABLE_TASK_STATES];

function isTerminalTaskState(state) { return TERMINAL_TASK_STATES.includes(state); }
function isPausableTaskState(state) { return PAUSABLE_TASK_STATES.includes(state); }
function isResumableTaskState(state) { return RESUMABLE_TASK_STATES.includes(state); }
function isCancelableTaskState(state) { return CANCELABLE_TASK_STATES.includes(state); }

// 兼容旧调用名（转发到单一源）。注意：stateClass 现返回 badge-* 组件类，
// 调用点须为 class="badge ${stateClass(...)}"。
function stateLabel(state) { return taskStateMeta(state).label; }
function stateClass(state) { return taskStateMeta(state).badge; }
function progressBarClass(state) { return taskStateMeta(state).bar; }

/* ===== 磁力检测状态 ===== */
// 单条磁力的检测结果 → { icon(emoji), title, text(文字色类，主要影响 emoji 周边) }
const MAGNET_STATUS_META = {
    active: { icon: '🟢', title: '有效', text: 'text-success-text' },
    weak:   { icon: '🟡', title: '弱',   text: 'text-warning-text' },
    dead:   { icon: '🔴', title: '无效', text: 'text-danger-text' },
};

function magnetStatusMeta(magnet) {
    if (magnet.check_error && !magnet.check_status) {
        return { icon: '❌', title: magnet.check_error, text: 'text-[color:var(--c-text-muted)]' };
    }
    if (!magnet.checked_at) {
        return { icon: '⚪', title: '未检测', text: 'text-[color:var(--c-text-subtle)]' };
    }
    const meta = MAGNET_STATUS_META[magnet.check_status];
    if (meta) {
        // dead 态优先展示后端错误原因为 title
        if (magnet.check_status === 'dead') {
            return { ...meta, title: magnet.check_error || meta.title };
        }
        return meta;
    }
    return { icon: '❌', title: magnet.check_error || '检测失败', text: 'text-[color:var(--c-text-muted)]' };
}

/* ===== 集合健康度（四宫格统计） ===== */
// 顺序即展示顺序；badge 为徽章语义类
const HEALTH_ITEMS = [
    { key: 'active', title: '有效影片',     badge: 'badge-success' },
    { key: 'weak',   title: '弱影片',       badge: 'badge-warning' },
    { key: 'dead',   title: '无效影片',     badge: 'badge-danger'  },
    { key: 'failed', title: '检测失败影片', badge: 'badge-neutral' },
];
