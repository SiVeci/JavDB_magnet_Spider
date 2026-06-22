import { createRouter, createWebHashHistory } from 'vue-router'
import TasksView from '@/views/TasksView.vue'
import DatabaseView from '@/views/DatabaseView.vue'
import ActorsView from '@/views/ActorsView.vue'
import SettingsView from '@/views/SettingsView.vue'

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', redirect: '/tasks' },
    { path: '/tasks', name: 'tasks', component: TasksView },
    {
      path: '/database',
      name: 'database',
      component: DatabaseView,
      children: [
        { path: ':type', name: 'database-type', component: DatabaseView },
        { path: ':type/:category', name: 'database-category', component: DatabaseView },
        { path: ':type/:category/:period', name: 'database-period', component: DatabaseView },
        { path: ':type/:category/:period/:movieId', name: 'database-movie', component: DatabaseView },
      ],
    },
    { path: '/actors', name: 'actors', component: ActorsView },
    { path: '/settings', name: 'settings', component: SettingsView },
    { path: '/:pathMatch(.*)*', redirect: '/tasks' },
  ],
})

export default router
