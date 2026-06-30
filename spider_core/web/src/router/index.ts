import { createRouter, createWebHashHistory } from 'vue-router'
const TasksView = () => import('@/views/TasksView.vue')
const DatabaseView = () => import('@/views/DatabaseView.vue')
const ActorsView = () => import('@/views/ActorsView.vue')
const SettingsView = () => import('@/views/SettingsView.vue')

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', redirect: '/tasks' },
    { path: '/tasks', name: 'tasks', component: TasksView },
    // 数据库：演员链 3 级（type/category/movieId），排行链 4 级（type/category/period/movieId）。
    // 用显式路由区分，避免演员的 movieId 落到 period 槽位导致进不去磁力页。
    { path: '/database', name: 'database', component: DatabaseView },
    { path: '/database/actor', name: 'database-actor', component: DatabaseView },
    { path: '/database/actor/:category', name: 'database-actor-movies', component: DatabaseView },
    { path: '/database/actor/:category/:movieId', name: 'database-actor-magnets', component: DatabaseView },
    { path: '/database/ranking', name: 'database-ranking', component: DatabaseView },
    { path: '/database/ranking/:category', name: 'database-ranking-period', component: DatabaseView },
    { path: '/database/ranking/:category/:period', name: 'database-ranking-movies', component: DatabaseView },
    { path: '/database/ranking/:category/:period/:movieId', name: 'database-ranking-magnets', component: DatabaseView },
    { path: '/actors', name: 'actors', component: ActorsView },
    { path: '/settings', name: 'settings', component: SettingsView },
    { path: '/:pathMatch(.*)*', redirect: '/tasks' },
  ],
})

export default router
