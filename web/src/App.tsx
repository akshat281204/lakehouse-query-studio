import { useEffect, useState } from 'react'
import { DatasetsPage } from './pages/DatasetsPage'
import { DatasetDetailPage } from './pages/DatasetDetailPage'
import { UploadPage } from './pages/UploadPage'

type Route =
  | { view: 'list' }
  | { view: 'detail'; id: string }
  | { view: 'upload' }

function parseRoute(): Route {
  const parts = window.location.hash.replace(/^#\/?/, '').split('/')
  if (parts[0] === 'datasets' && parts[1]) {
    return { view: 'detail', id: parts[1] }
  }
  if (parts[0] === 'upload') {
    return { view: 'upload' }
  }
  return { view: 'list' }
}

function Nav({ active }: { active: 'datasets' | 'upload' }) {
  return (
    <nav className="top-nav" aria-label="Primary">
      <a
        href="#/datasets"
        aria-current={active === 'datasets' ? 'page' : undefined}
      >
        Datasets
      </a>
      <a
        href="#/upload"
        aria-current={active === 'upload' ? 'page' : undefined}
      >
        Upload
      </a>
    </nav>
  )
}

export default function App() {
  const [route, setRoute] = useState<Route>(parseRoute)

  useEffect(() => {
    const onChange = () => setRoute(parseRoute())
    window.addEventListener('hashchange', onChange)
    return () => window.removeEventListener('hashchange', onChange)
  }, [])

  if (route.view === 'upload') {
    return (
      <>
        <Nav active="upload" />
        <UploadPage
          onOpenDataset={(id) => {
            window.location.hash = `#/datasets/${id}`
          }}
        />
      </>
    )
  }
  if (route.view === 'detail') {
    return (
      <DatasetDetailPage
        id={route.id}
        onBack={() => {
          window.location.hash = '#/datasets'
        }}
      />
    )
  }
  return (
    <>
      <Nav active="datasets" />
      <DatasetsPage
        onOpen={(id) => {
          window.location.hash = `#/datasets/${id}`
        }}
      />
    </>
  )
}
