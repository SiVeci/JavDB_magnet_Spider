/*
 * state.js — 集中式全局状态
 * 原先散落在 index.html 顶部的全部可变状态集中到此文件。
 * 仍以全局 let 形式保存，确保各功能模块与内联事件处理可直接读写，
 * 行为与重构前完全一致。
 */

// 轮询句柄（重构后保存 setTimeout 链的句柄，由 tasks.js / magnets.js 管理）
let pollInterval = null;
let magnetCheckPollInterval = null;

// 鉴权
let apiToken = sessionStorage.getItem('javdb_auth_token') || '';
let authRequired = false;
let activeView = 'tasks';

// 任务与队列
let tasksCache = [];
let queueStatus = null;

// 数据库集合
let collectionsCache = [];
let collectionSearchQuery = '';

// 任务配置（起始 URL / 标签）
let actorBaseUrl = '';
let actorBaseParams = new URLSearchParams();
let availableTags = [];
let selectedTags = new Set();

// 面板折叠状态
let tagsCollapsed = false;
let runtimeConfigCollapsed = true;
let logCollapsed = true;
let showFinishedTasks = false;

// 集合 / 影片展开与过滤
let expandedCollectionName = null;
let expandedMovieId = null;
let collectionMovieCache = {};
let rankingMovieCache = {};
let collectionTagFilters = {};
let collectionExcludeFilters = {};
let rankingTagFilters = {};
let rankingExcludeFilters = {};

// 下拉菜单与磁力检测任务
let openTagDropdown = null;
let openExcludeDropdown = null;
let openMagnetCheckMenu = null;
let activeMagnetCheckJob = null;
