// 对照后端 schemas.py 的接口类型定义

export type TaskState =
  | 'pending'
  | 'running'
  | 'pause_requested'
  | 'paused'
  | 'waiting_cookie'
  | 'waiting_choice'
  | 'cancel_requested'
  | 'canceled'
  | 'finished'
  | 'failed'

export interface Task {
  task_id: string
  filename: string
  final_filename?: string
  state: TaskState
  progress: string
  can_copy_incremental_magnets?: boolean
}

export interface QueueStatus {
  queue_state: string
  can_start: boolean
  active_count: number
  finished_count: number
  current_task_id?: string
}

export interface RuntimeConfig {
  cookie: string
  remember_cookie: boolean
  user_agent: string
  proxies: string
  trackers: string[]
}

export interface Collection {
  name: string
  filename: string
  movie_count: number
  size_mb: number
  has_unchecked: boolean
  has_weak: boolean
  has_dead: boolean
}

export interface Movie {
  id: number
  code: string
  title?: string
  tags?: string[]
  magnet_health?: string
  magnets?: Magnet[]
}

export interface Magnet {
  id: number
  url: string
  name?: string
  size_mb?: number
  is_hd?: boolean
  has_subtitle?: boolean
  checked_at?: string
  check_status?: string
  check_error?: string
  is_selected?: boolean
}

export interface Actor {
  actor_id: string
  actor_name: string
  actor_url: string
  category: string
  refreshed_at?: number
  has_collection: boolean
  collection_filename?: string
  last_task_tags?: { name: string; value: string }[]
}

export interface ActorCategory {
  key: string
  label: string
}

export interface ActorsData {
  categories: ActorCategory[]
  actors: Actor[]
  failed?: { category: string; label: string; msg: string }[]
}

export interface Tag {
  name: string
  value: string
}

export interface ApiResponse<T = unknown> {
  code: number
  msg?: string
  data?: T
}

export interface RankingCategory {
  key: string
  label: string
  periods?: RankingPeriod[]
  dynamicOptions?: boolean
}

export interface RankingPeriod {
  key: string
  label: string
}

export interface RankingMovie extends Movie {
  rank?: number
}
