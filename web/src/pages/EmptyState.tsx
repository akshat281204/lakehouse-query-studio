export function EmptyState() {
  return (
    <main className="empty-state">
      <h1>Lakehouse Query Studio</h1>
      <p>No datasets yet.</p>
      <p>
        Register an existing Parquet prefix or upload a CSV/JSON file to create
        your first dataset.
      </p>
    </main>
  )
}
