import { useEffect, useState } from 'react'
import { api } from './api/client'
import { DatasetsPage } from './pages/DatasetsPage'
import { DatasetDetailPage } from './pages/DatasetDetailPage'
import { UploadPage } from './pages/UploadPage'
import { OverviewPage } from './pages/OverviewPage'
import { QueryPage } from './pages/QueryPage'
import { HistoryPage } from './pages/HistoryPage'
import { ThemePicker } from './components/ThemePicker'

type Route =
  | { view: 'overview' }
  | { view: 'list' }
  | { view: 'detail'; id: string }
  | { view: 'upload' }
  | { view: 'query' }
  | { view: 'history' }

function parseRoute(): Route {
  const parts = window.location.hash.replace(/^#\/?/, '').split('/')
  if (parts[0] === 'datasets' && parts[1])
    return { view: 'detail', id: parts[1] }
  if (parts[0] === 'datasets') return { view: 'list' }
  if (parts[0] === 'upload') return { view: 'upload' }
  if (parts[0] === 'query') return { view: 'query' }
  if (parts[0] === 'history') return { view: 'history' }
  return { view: 'overview' }
}

type IconName = 'overview' | 'datasets' | 'sql' | 'history' | 'upload'

const ICONS: Record<IconName, string> = {
  overview: 'M3 3h7v7H3zM14 3h7v4h-7zM14 10h7v11h-7zM3 13h7v8H3z',
  datasets:
    'M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6',
  sql: 'M4 17l6-6-6-6M12 19h8',
  history: 'M3 12a9 9 0 1 0 3-6.7L3 8M3 3v5h5M12 7v5l3 2',
  upload: 'M12 16V4M7 9l5-5 5 5M4 20h16',
}

function Icon({ name }: { name: IconName }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d={ICONS[name]} />
    </svg>
  )
}

const NAV: { key: string; href: string; label: string; icon: IconName }[] = [
  { key: 'overview', href: '#/', label: 'Overview', icon: 'overview' },
  { key: 'list', href: '#/datasets', label: 'Datasets', icon: 'datasets' },
  { key: 'query', href: '#/query', label: 'SQL', icon: 'sql' },
  { key: 'history', href: '#/history', label: 'History', icon: 'history' },
  { key: 'upload', href: '#/upload', label: 'Upload', icon: 'upload' },
]

function Nav({ active }: { active: string }) {
  return (
    <nav className="top-nav" aria-label="Primary">
      {NAV.map((n) => (
        <a
          key={n.key}
          href={n.href}
          aria-current={active === n.key ? 'page' : undefined}
        >
          <Icon name={n.icon} />
          {n.label}
        </a>
      ))}
    </nav>
  )
}

/** Live readiness of the dependencies, refreshed every 30 s. */
function SystemStatus() {
  const [state, setState] = useState<'checking' | 'ok' | 'down'>('checking')
  useEffect(() => {
    let alive = true
    const check = () =>
      api
        .ready()
        .then((r) => alive && setState(r.status === 'ok' ? 'ok' : 'down'))
        .catch(() => alive && setState('down'))
    void check()
    const t = setInterval(check, 30_000)
    return () => {
      alive = false
      clearInterval(t)
    }
  }, [])
  const label =
    state === 'ok'
      ? 'All systems ready'
      : state === 'down'
        ? 'A dependency is down'
        : 'Checking…'
  return (
    <div className="sidebar-foot">
      <div className="row">
        <span
          className={`dot ${state === 'ok' ? 'dot-ok' : state === 'down' ? 'dot-bad' : ''}`}
        />
        <span>{label}</span>
      </div>
      <span>Single-node DuckDB engine</span>
    </div>
  )
}

export default function App() {
  const [route, setRoute] = useState<Route>(parseRoute)
  // A statement handed from History to the workbench (kept in memory only).
  const [rerun, setRerun] = useState<{ sql: string; n: number } | null>(null)

  useEffect(() => {
    const onChange = () => setRoute(parseRoute())
    window.addEventListener('hashchange', onChange)
    return () => window.removeEventListener('hashchange', onChange)
  }, [])

  const go = (hash: string) => {
    window.location.hash = hash
  }

  let page
  switch (route.view) {
    case 'upload':
      page = <UploadPage onOpenDataset={(id) => go(`#/datasets/${id}`)} />
      break
    case 'detail':
      page = <DatasetDetailPage id={route.id} onBack={() => go('#/datasets')} />
      break
    case 'list':
      page = <DatasetsPage onOpen={(id) => go(`#/datasets/${id}`)} />
      break
    case 'query':
      page = (
        <QueryPage
          key={rerun?.n ?? 0}
          initialStatement={rerun?.sql}
          autoRun={rerun !== null}
        />
      )
      break
    case 'history':
      page = (
        <HistoryPage
          onRerun={(sql) => {
            setRerun((r) => ({ sql, n: (r?.n ?? 0) + 1 }))
            go('#/query')
          }}
        />
      )
      break
    default:
      page = <OverviewPage />
  }
  const active = route.view === 'detail' ? 'list' : route.view
  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">
            <svg
              width="18"
              height="18"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M4 7c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3zM4 7v10c0 1.7 3.6 3 8 3s8-1.3 8-3V7" />
            </svg>
          </span>
          <span className="brand-text">
            <strong>Lakehouse</strong>
            <span>Query Studio</span>
          </span>
        </div>
        <Nav active={active} />
        <div className="top-actions">
          <SystemStatus />
          <ThemePicker />
        </div>
      </aside>
      <div className="main">{page}</div>
    </div>
  )
}
