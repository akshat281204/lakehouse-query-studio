import { useEffect } from 'react'

/**
 * Decorative topographic background. The lines are an SVG mask filled with the
 * theme accent; a second copy, revealed only around the pointer, makes the
 * contours glow where you move. Purely cosmetic: hidden from assistive tech,
 * skipped for reduced motion and touch screens, and carries no data.
 */
export function Terrain() {
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
    }
    const move = (e: PointerEvent) => {
      x = e.clientX
      y = e.clientY
      if (!raf) raf = requestAnimationFrame(paint)
    }
    const leave = () => {
      x = -999
      y = -999
      if (!raf) raf = requestAnimationFrame(paint)
    }
    window.addEventListener('pointermove', move, { passive: true })
    document.addEventListener('pointerleave', leave)
    return () => {
      window.removeEventListener('pointermove', move)
      document.removeEventListener('pointerleave', leave)
      if (raf) cancelAnimationFrame(raf)
    }
  }, [])

  return (
    <div className="terrain" aria-hidden="true">
      <i className="t-base" />
      <i className="t-glow" />
    </div>
  )
}
