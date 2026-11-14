import { useEffect, useState } from 'react'
import { DatasetsPage } from './pages/DatasetsPage'
import { DatasetDetailPage } from './pages/DatasetDetailPage'

type Route = { view: 'list' } | { view: 'detail'; id: string }

function parseRoute(): Route {
  const parts = window.location.hash.replace(/^#\/?/, '').split('/')
  if (parts[0] === 'datasets' && parts[1]) {
    return { view: 'detail', id: parts[1] }
  }
  return { view: 'list' }
}

export default function App() {
  const [route, setRoute] = useState<Route>(parseRoute)

  useEffect(() => {
    const onChange = () => setRoute(parseRoute())
    window.addEventListener('hashchange', onChange)
    return () => window.removeEventListener('hashchange', onChange)
  }, [])

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
    <DatasetsPage
      onOpen={(id) => {
        window.location.hash = `#/datasets/${id}`
      }}
    />
  )
}
