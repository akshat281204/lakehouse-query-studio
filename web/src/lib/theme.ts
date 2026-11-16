export interface ThemeInfo {
  id: string
  label: string
  dark: boolean
  /** [background, surface, accent, text] preview colours for the swatch. */
  swatch: [string, string, string, string]
}

export const THEMES: ThemeInfo[] = [
  {
    id: 'strata',
    label: 'Strata',
    dark: false,
    swatch: ['#f3f6f7', '#ffffff', '#2447f0', '#0b1a22'],
  },
  {
    id: 'mint',
    label: 'Mint',
    dark: false,
    swatch: ['#ecf3ef', '#fcfefd', '#0f9d6b', '#13231c'],
  },
  {
    id: 'abyss',
    label: 'Abyss',
    dark: true,
    swatch: ['#060f15', '#0c1a22', '#6c8dff', '#e7f1f4'],
  },
  {
    id: 'ocean',
    label: 'Ocean',
    dark: true,
    swatch: ['#08191f', '#0f2730', '#4cc3d1', '#e3eeee'],
  },
  {
    id: 'nord',
    label: 'Nord',
    dark: true,
    swatch: ['#2b303b', '#353c4a', '#88c0d0', '#eceff4'],
  },
  {
    id: 'terminal',
    label: 'Terminal',
    dark: true,
    swatch: ['#080a08', '#0e120e', '#34e56b', '#b8f2b8'],
  },
]

export const DEFAULT_THEME = 'strata'
const KEY = 'lqs-theme'

export const isTheme = (id: string | null | undefined): id is string =>
  THEMES.some((t) => t.id === id)

/** A `?theme=` URL parameter (for sharing a view), else the saved theme, else the OS preference (Abyss when dark), else Strata. */
export function loadTheme(): string {
  try {
    const q = new URLSearchParams(window.location.search).get('theme')
    if (isTheme(q)) return q
  } catch {
    /* ignore */
  }
  try {
    const saved = localStorage.getItem(KEY)
    if (isTheme(saved)) return saved
  } catch {
    /* storage unavailable: fall through */
  }
  try {
    if (window.matchMedia?.('(prefers-color-scheme: dark)').matches) {
      return 'abyss'
    }
  } catch {
    /* ignore */
  }
  return DEFAULT_THEME
}

export function applyTheme(id: string): void {
  const theme = THEMES.find((t) => t.id === id) ?? THEMES[0]
  document.documentElement.dataset.theme = theme.id
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute('content', theme.swatch[0])
}

export function saveTheme(id: string): void {
  try {
    localStorage.setItem(KEY, id)
  } catch {
    /* the choice just won't persist */
  }
}
