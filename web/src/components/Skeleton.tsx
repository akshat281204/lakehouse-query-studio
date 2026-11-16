/** Placeholder shapes shown while a page loads; announced as "Loading…" to screen readers. */
export function OverviewSkeleton({ label }: { label: string }) {
  return (
    <div className="skeleton-page" role="status" aria-live="polite">
      <span className="sr-only">{label}</span>
      <div className="sk sk-hero" />
      <div className="sk sk-hero short" />
      <div className="sk-row">
        {[0, 1, 2, 3, 4].map((i) => (
          <div key={i} className="sk sk-fig" />
        ))}
      </div>
      <div className="sk sk-chart" />
    </div>
  )
}
