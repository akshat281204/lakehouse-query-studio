import { useEffect, useState } from 'react'
import { DatasetsPage } from './pages/DatasetsPage'
import { DatasetDetailPage } from './pages/DatasetDetailPage'
import { UploadPage } from './pages/UploadPage'
import { OverviewPage } from './pages/OverviewPage'
import { QueryPage } from './pages/QueryPage'
import { HistoryPage } from './pages/HistoryPage'

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

const NAV: { key: string; href: string; label: string }[] = [
  { key: 'overview', href: '#/', label: 'Overview' },
  { key: 'list', href: '#/datasets', label: 'Datasets' },
  { key: 'query', href: '#/query', label: 'SQL' },
  { key: 'history', href: '#/history', label: 'History' },
  { key: 'upload', href: '#/upload', label: 'Upload' },
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
          {n.label}
        </a>
      ))}
    </nav>
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
    <>
      <Nav active={active} />
      {page}
    </>
  )
}
