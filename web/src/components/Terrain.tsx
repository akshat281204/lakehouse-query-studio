import { useEffect, useRef } from 'react'

/**
 * Decorative topographic background (nested contour rings). Lines are an SVG
 * mask filled with the theme accent. Subtle interactions, all cosmetic:
 *  - a soft glow in the lines under the pointer,
 *  - the map drifting a few pixels against the pointer,
 *  - a faint water ripple where you click.
 * Hidden from assistive tech; off for reduced motion and touch screens; no data.
 */
export function Terrain() {
  const host = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const quiet = window.matchMedia?.(
      '(prefers-reduced-motion: reduce)',
    ).matches
    const touch = window.matchMedia?.('(pointer: coarse)').matches
    if (quiet || touch) return
    const root = document.documentElement
    let raf = 0
    let x = -999
    let y = -999
    const paint = () => {
      raf = 0
      root.style.setProperty('--mx', `${x}px`)
      root.style.setProperty('--my', `${y}px`)
      // normalised -1..1 from the viewport centre, for the gentle drift
      root.style.setProperty(
        '--px',
        ((x / window.innerWidth) * 2 - 1).toFixed(3),
      )
      root.style.setProperty(
        '--py',
        ((y / window.innerHeight) * 2 - 1).toFixed(3),
      )
    }
    const schedule = () => {
      if (!raf) raf = requestAnimationFrame(paint)
    }
    const move = (e: PointerEvent) => {
      x = e.clientX
      y = e.clientY
      schedule()
    }
    const leave = () => {
      x = -999
      y = -999
      schedule()
    }
    const ripple = (e: PointerEvent) => {
      const el = host.current
      if (!el) return
      const r = document.createElement('span')
      r.className = 'ripple'
      r.style.left = `${e.clientX}px`
      r.style.top = `${e.clientY}px`
      r.addEventListener('animationend', () => r.remove(), { once: true })
      el.appendChild(r)
    }
    window.addEventListener('pointermove', move, { passive: true })
    window.addEventListener('pointerdown', ripple, { passive: true })
    document.addEventListener('pointerleave', leave)
    return () => {
      window.removeEventListener('pointermove', move)
      window.removeEventListener('pointerdown', ripple)
      document.removeEventListener('pointerleave', leave)
      if (raf) cancelAnimationFrame(raf)
    }
  }, [])

  return (
    <div className="terrain" aria-hidden="true" ref={host}>
      <i className="t-base" />
      <i className="t-glow" />
    </div>
  )
}
