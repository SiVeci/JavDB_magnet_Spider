# 前端重写计划：原生 JS → Vue3 + Vite 全量重写

> 目标：用 Vue3 + Vite 全量重写前端，解决「状态变更后需手动刷新整页」的核心痛点，并为后续视觉/动效升级打好组件化基础。

## 0. 背景与关键前提

- 现状：`spider_core/frontend/` 为纯静态前端，`index.html`(428 行) + 14 个原生 JS(~3500 行) + 4 个 CSS，无任何工具链。
- 痛点根因：全局裸 `let` 状态（`state.js`）与视图无绑定，每次数据变更需手动调用一串 `renderXxx()`；只有任务页轮询，跨视图联动靠用户手动刷整页兜底。
- **构建约束的准确范围**：禁止的是在云服务器上**编译 Android APK**（Gradle/SDK 重负载，机器扛不住）。**前端 `vite build` / dev server 不在禁止范围内**——它与现有 Python 服务同量级。
- **环境已就绪**：本机 Node `v20.20.2` + npm `11.17.0`，2 核 / 3.5G 内存（约 1.5G 可用），跑 Vite 构建与 dev server 绰绰有余。
- 终点形态：Vue3 + Vite + SFC（可选 TS），构建产物 `dist/` 由本机或 CI 生成，FastAPI 托管 `dist/`。
- Android 端不受影响：App 的 WebView 仅用于登录目标站取 cookie 与隐蔽爬取，**前端始终由外部浏览器**访问 FastAPI 服务（`http://127.0.0.1:8000` 或局域网地址）。重写不触发任何 APK 重新编译。

## 1. 技术选型

| 项 | 选择 | 理由 |
|---|---|---|
| 框架 | Vue 3（`<script setup>` SFC） | 响应式直接消灭手动 render；组件化拆解巨型 js 文件 |
| 构建 | Vite | 本机即可构建/热更新；产物为纯静态 `dist/` |
| 语言 | TypeScript（推荐，可选） | 对照后端 `schemas.py` 建类型，编译期捕获字段不匹配；无安全网时尤其值钱 |
| 状态 | Pinia | 集中状态 + 跨视图共享，实现自动联动 |
| 路由 | Vue Router（hash 模式） | 沿用现有 `#/` 风格，与现有 URL 兼容；替代手写 `routing.js` |
| 样式 | Tailwind（构建版，PostCSS） | 从 CDN 运行时改为构建期，按需裁剪；消除「无外网时丢样式」隐患 |
| 测试 | Vitest（按需） | 给 store / 纯逻辑写单测，补足回归安全网 |

> 安全网说明：本机可构建后，dev server 热更新、`vue-tsc` 类型检查、SFC 编译期报错、Vitest 全部可用——「改错就白屏且无法定位」的零构建困境不复存在，开发期有完整护栏。

## 2. 目标目录结构

```
spider_core/
├── frontend/                  # 旧前端：迁移期保留，全部迁完后整体删除
└── web/                       # 新增：Vue3 + Vite 工程（源码）
    ├── package.json
    ├── vite.config.ts         # 关键：base 与 build.outDir 指向后端可托管路径
    ├── tsconfig.json
    ├── tailwind.config.ts
    ├── postcss.config.js
    ├── index.html             # 单一入口
    └── src/
        ├── main.ts            # createApp + Pinia + Router
        ├── App.vue            # 根组件（鉴权壳 + 导航 + <router-view>）
        ├── router/index.ts    # 路由（替代 routing.js）
        ├── api/               # apiFetch 封装 + 各域接口（迁移 js/api.js）
        ├── stores/
        │   ├── auth.ts        # 鉴权（替代 apiToken 等裸 let）
        │   ├── tasks.ts       # 任务/队列 + 轮询（替代 tasksCache/queueStatus）
        │   └── database.ts    # 集合/影片/排行榜
        ├── composables/       # useXxx 复用逻辑（轮询、菜单互斥、布局自适应）
        ├── components/        # 跨视图复用组件（磁力表、面包屑、任务卡片等）
        ├── views/
        │   ├── TasksView.vue
        │   ├── DatabaseView.vue
        │   ├── ActorsView.vue
        │   └── SettingsView.vue
        └── types/             # 对照后端 schemas.py 的接口类型
```

构建产物 `dist/` 输出位置见第 4 节。

## 3. 总体路线

```
阶段0  搭 Vite+Vue+TS+Tailwind 工程骨架，跑通「构建→FastAPI托管→浏览器访问→鉴权」最小闭环
阶段1  迁任务页：Pinia + 轮询入 store，验证「自动刷新」核心收益
阶段2  逐视图迁移(设置→演员→排行榜→数据库)，共享 store 实现跨视图自动联动
阶段3  收尾：删除旧 frontend/，Tailwind 完全构建化，固化构建/部署流程
阶段4  (可选) SSE 替代轮询，实现真正实时
```

- 阶段0、1 之间**不停**（同属一个最小可验证闭环）。
- **阶段1 完成后做一次产品/架构验收**（见第 9 节）：自动刷新手感是否如预期、视觉/动效方向是否影响组件结构。这是产品判断而非技术风险，建议拍板后再铺开阶段2。
- 阶段2 起有编译期类型检查 + 热更新护航，可较快推进，无需每页强停。

## 4. 构建产物托管方案（FastAPI 改动）

两种方式，**推荐方式 A**：

### 方式 A：构建到独立目录，后端按目录托管（推荐）

- `vite.config.ts` 设 `build.outDir = '../frontend_dist'`、`base = '/'`。
- `main.py` 中把前端目录指向 `frontend_dist`，沿用现有 `StaticFiles` 挂载与 `/` 返回 `index.html` 的逻辑：
  ```python
  # 现状约第 54 行
  _FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
  # 改为构建产物目录（保留旧目录作迁移期回退）
  _FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend_dist")
  ```
- Vite 默认把资源放在 `dist/assets/`，需让 `main.py` 挂载 `assets`（或整目录）静态路径；`/` 读 `frontend_dist/index.html`。
- 鉴权中间件只拦 `/api/`，静态资源天然豁免，无需改动。

### 方式 B：直接构建覆盖 `frontend/`

- 让 `outDir` 直接写入 `frontend/`，`main.py` 零改动。
- 缺点：旧文件与产物混在一起、回退不干净。**不推荐**。

### dist 入库 vs CI 出包（二选一，可后定）

- **本机构建 + 产物入库**：最简单，`git add frontend_dist`。缺点仓库变大。
- **CI 构建**（已有 `.github/workflows/`）：仓库干净，运行环境只拉产物。需加 build 步骤。
- 迁移期建议先用「本机构建 + 产物入库」跑通，阶段3 再决定是否转 CI。

### 关键校验点

- 产物里资源引用路径（`base`）必须与后端实际挂载路径一致，否则 JS/CSS 404 → 白屏。这是本方案唯一需要重点验证的技术点。

## 5. 阶段0：工程骨架与最小闭环

目标：跑通整条链路，**现有前端保持不动**（旧 `frontend/` 仍可用作回退）。

### 步骤

1. `spider_core/web/` 下初始化 Vite + Vue + TS 工程；装 `vue-router`、`pinia`、`tailwindcss` + `postcss` + `autoprefixer`、`@vitejs/plugin-vue`。
2. 配置 `vite.config.ts`（`base`、`build.outDir` 指向 `frontend_dist`）、`tailwind.config.ts`（迁移现有内联 `tailwind.config` 与 CSS 变量 token）、`tsconfig.json`。
3. 迁移现有 4 个 CSS 的设计 token（CSS 变量、`@apply` 组件类）到构建式 Tailwind。**注意现有设计系统：语义色类须在 config 注册否则静默失效**。
4. 写最小 `App.vue`：鉴权壳（迁移 `js/app.js` 的 token 校验流程，保持 `sessionStorage` + 请求头机制不变）+ 空 `<router-view>`。
5. `api/` 封装 `apiFetch`（迁移 `js/api.js`），保留 token 注入逻辑。
6. 本机 `npm run build` 出 `frontend_dist/`；按方式 A 改 `main.py` 指向产物目录。

### 验证（本机起 Python 服务，禁 APK 编译不受影响）

- `spider_core/.venv-test` 起服务，浏览器访问 `/`。
- DevTools Network 确认 `index.html`、`assets/*.js`、`assets/*.css` 均 200（重点查 `base` 路径是否对）。
- 鉴权流程可正常输入 token 并通过 `/api/status` 校验。
- dev 阶段用 `npm run dev`（Vite 热更新）开发，仅最终产物经后端托管。

### 回退

- `main.py` 的 `_FRONTEND_DIR` 改回 `frontend` 即恢复旧前端；新工程独立于 `web/`，不影响现有运行。

## 6. 阶段1：迁移任务页（兑现「自动刷新」核心收益）

任务页是痛点最集中处（轮询 + 多处手动 render + 操作后手动 refreshMonitor），打头阵以最快验证收益。

### 步骤

1. **`stores/tasks.ts` 承载状态与轮询**：把 `tasks.js` 的 `startMonitorPolling`/`refreshMonitor` 搬入 store，拉到数据写入响应式 state。操作（删除/暂停/清理）结束只调一次 `refreshMonitor()`，**所有依赖该 store 的视图自动更新**，不再逐个 `renderTaskList()/renderQueueControls()/renderCurrentTask()`。
2. **`TasksView.vue`**：`v-for` 渲染任务列表，`computed` 派生 `currentTask`，`@click` 绑操作，`<Transition>` 预留动效位。替代 `tasks.js` 全部手写 `innerHTML` 与 `renderXxx`。
3. **菜单互斥**抽成 `composables/useExclusiveMenu.ts`（替代 `app.js` 的 `openExclusiveMenu`/`closeOpenMenus` 全局变量）。
4. 路由注册任务页为首个 `<router-view>` 视图；导航层兼容：已迁视图走 router，未迁视图暂留旧入口（迁移期 `web/` 与旧 `frontend/` 不同时托管，先以 `web/` 为准并补齐任务页，其余视图占位提示「迁移中」或临时内嵌旧逻辑——以一次性把任务页做完整为准）。

### 验证 / 验收标准

- 起服务，添加/删除/暂停任务，**不手动刷新**，确认列表、队列控件、当前任务卡片自动更新。
- 2.5s 轮询仍工作；日志面板、折叠态、`waiting_cookie`/`waiting_choice` 等任务态逐项回归。
- ✅ **核心验收：任务页任何状态变更后无需手动刷新，相关 UI 自动体现。**

## 7. 阶段2：逐视图迁移 + 跨视图自动联动

按依赖度从低到高：**设置页 → 演员页 → 排行榜 → 数据库(影片)**（数据库最复杂，含 `routing.js` 多级 hash 解析，最后做）。

### 要点

- 每视图一个 `views/XxxView.vue` + 对应 `stores/` 切片 + 必要的 `components/`。
- **跨视图联动**：任务完成后数据库集合应更新——让任务 store 与数据库 store 共享或互相 `watch`，实现「任务页变更 → 数据库页自动刷新」，**彻底移除手动刷整页**。
- Vue Router 完整替代 `routing.js`：`databaseRouteInfo` 的 `#/database/actor/:name/:movieId` 等改为路由参数。
- 复用组件优先抽取：磁力表（`magnet-table.js`）、面包屑（`routing.js` 的 breadcrumb）、任务卡片、标签过滤下拉。
- 有 TS 类型 + 热更新护航，可一次推进多个视图；每个视图迁完单独回归。

### 验证

- 每页迁移后单独回归；专门验证跨视图：任务跑完 → 数据库页集合数量/影片自动更新，无需刷新。

## 8. 阶段3：收尾与流程固化

1. 全部视图迁完后，**删除旧 `frontend/` 目录**及 `main.py` 中旧路径引用。
2. Tailwind 完全构建化：确认无残留 CDN 引用，内联 config 已迁出，`@apply` 走 PostCSS。
3. 固化构建/部署流程，二选一定案：
   - 本机 `npm run build` + `frontend_dist/` 入库；或
   - CI（`.github/workflows/`）构建出包，运行环境只托管产物。
4. 更新 `README.md` / `AGENTS.md` 的前端开发说明（dev：`npm run dev`；发布：`npm run build`）。
5. 清理 `package.json` 依赖，锁定版本（`package-lock.json` 入库）。

## 9. 阶段1 后的验收闸门（产品/架构判断）

此停顿与构建无关，纯产品决策，建议保留：

- **自动刷新手感**：任务页实际体验是否达到预期，是否需要调整轮询节奏或改用 SSE（阶段4）。
- **视觉/动效方向**：你的「更酷炫」诉求需在此具体化——决定组件是否要为列表进出场动画、转场预留结构（影响 `components/` 拆分粒度）。方向定了再铺开阶段2，避免全站返工。
- **状态架构**：Pinia 切片划分在任务页验证是否合理，再套用到其余视图。

## 10. 阶段4（可选）：SSE 替代轮询，真正实时

- FastAPI 新增 `/api/events`（`text/event-stream`），任务状态变更时推送。
- `composables/useEventStream.ts` 用 `EventSource` 订阅，事件直接写入 Pinia → 所有视图即时更新，消除 2.5s 延迟。
- 鉴权：`EventSource` 不能自定义请求头，需用 query 参数或 cookie 传 token，`/api/events` 单独处理鉴权（勿破坏现有 token 中间件）。
- 与框架迁移正交，可独立评估；外部浏览器均支持 `EventSource`。

## 11. 全程约束与验证基线

- **禁止在云服务器编译 Android APK**（Gradle/`gradlew`/`cap sync` 等）。**前端 `vite build`/`npm run dev` 允许**，与 Python 服务同量级。
- 改文件一律用编辑器工具，改前先读；不用 `cat/echo/sed` 改文件。
- 前端有了编译期安全网（`vue-tsc` 类型检查 + SFC 编译报错 + Vite 构建报错），但**运行时行为仍需起 Python 服务 + 外部浏览器手动点测**——编译通过 ≠ 功能正确。
- 鉴权流程（`sessionStorage` token + 请求头 + `/api/` 中间件）全程不得破坏。
- 不破坏用户自加的局域网访问功能；Android 端无需重新编译（仅外部浏览器换访问地址）。
- 每阶段交付前：① 构建无报错 + 类型检查通过 ② 目标视图功能回归 ③ 外部浏览器实测一次。

## 12. 关键风险与缓解

| 风险 | 缓解 |
|---|---|
| 产物资源路径（`base`）与后端挂载不匹配 → 白屏 | 阶段0 重点验证 Network 路径；`base` 与 `main.py` 挂载约定写死并对齐 |
| 迁移期新旧前端切换/导航冲突 | 以 `web/` 为唯一托管源逐步补齐；旧 `frontend/` 仅作回退，不同时托管 |
| 现有 Tailwind 设计系统迁移遗漏（语义色静默失效） | 迁移 token 后逐页对比视觉；保留 CSS 变量 token 体系 |
| 大文件重写引入回归 | 逐视图迁移 + 类型检查 + 每步手动回归 |
| 本机内存有限（1.5G 可用）构建偶发吃紧 | Vite 构建峰值远低于此；必要时关闭 dev server 再 build，或转 CI 构建 |
| `dist` 入库致仓库膨胀 | 阶段3 评估转 CI 出包 |

## 13. 里程碑清单

- [ ] 阶段0：Vite+Vue+TS+Tailwind 骨架，构建→托管→鉴权最小闭环通过
- [ ] 阶段1：任务页迁移，自动刷新兑现，删除 `tasks.js` 手动 render
- [ ] **验收闸门**：自动刷新手感 + 视觉/动效方向 + 状态架构 拍板
- [ ] 阶段2：设置/演员/排行榜/数据库逐页迁移，跨视图联动生效
- [ ] 阶段3：删除旧 `frontend/`，Tailwind 完全构建化，构建/部署流程固化
- [ ] 阶段4（可选）：SSE 实时推送

> 建议先完成 **阶段0 + 阶段1**，验证「构建链路通畅 + 自动刷新兑现 + 视觉方向」三件事，再按验收结果推进阶段2。




