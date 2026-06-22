import { ref } from 'vue'

const THEME_KEY = 'javdb_theme'

export function useTheme() {
  const theme = ref<'light' | 'dark'>(
    (localStorage.getItem(THEME_KEY) === 'dark' ? 'dark' : 'light') as 'light' | 'dark'
  )

  function applyTheme(t: 'light' | 'dark') {
    document.documentElement.setAttribute('data-theme', t)
    document.documentElement.style.colorScheme = t
    theme.value = t
  }

  function cycleTheme() {
    const next: 'light' | 'dark' = theme.value === 'dark' ? 'light' : 'dark'
    localStorage.setItem(THEME_KEY, next)
    applyTheme(next)
  }

  return { theme, applyTheme, cycleTheme }
}
