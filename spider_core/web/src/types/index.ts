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
  count: number
  collection_type?: string
  ranking_category?: string
  ranking_period?: string
  tags?: string[]
  time?: string
  timestamp?: number
  has_source_url?: boolean
}

export interface Movie {
  id: number
  code: string
  title?: string
  url?: string
  tags?: string[]
  best_magnet_name?: string
  best_magnet_link?: string
  priority_score?: number
  magnet_date?: string
  size_mb?: number
  candidate_count?: number
  active_count?: number
  weak_count?: number
  dead_count?: number
  failed_count?: number
  checked_count?: number
  magnet_health?: string
  magnets?: Magnet[]
}

export interface Magnet {
  id: number
  movie_id?: number
  name?: string
  link: string
  base_priority_score?: number
  priority_score?: number
  magnet_date?: string
  size_mb?: number
  is_selected?: boolean
  position?: number
  created_at?: number
  check_status?: string
  seeders?: number
  leechers?: number
  checked_at?: string
  check_error?: string
}

export interface MagnetCheckJob {
  job_id: string
  scope: string
  target: string | number
  running: boolean
  cancelled?: boolean
  completed?: number
  total?: number
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
