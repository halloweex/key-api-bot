import { useEffect, useRef, useState } from 'react'

// ─── useAnimatedNumber ───────────────────────────────────────────────────────
//
// Animates a numeric value with eased interpolation. Returns the formatted
// string for the current frame so the caller can render it inside whatever
// element/style they need — no JSX is owned by this hook.

export function useAnimatedNumber(
  value: number,
  formatter: (value: number) => string,
  duration = 500,
): string {
  const [displayValue, setDisplayValue] = useState(value)
  const previousValue = useRef(value)
  const animationRef = useRef<number | null>(null)

  useEffect(() => {
    const startValue = previousValue.current
    const endValue = value
    const startTime = performance.now()

    if (startValue === endValue) {
      return
    }

    const animate = (currentTime: number) => {
      const elapsed = currentTime - startTime
      const progress = Math.min(elapsed / duration, 1)
      // Ease-out cubic for smooth deceleration
      const easeOut = 1 - Math.pow(1 - progress, 3)
      const current = startValue + (endValue - startValue) * easeOut

      setDisplayValue(current)

      if (progress < 1) {
        animationRef.current = requestAnimationFrame(animate)
      } else {
        setDisplayValue(endValue)
        previousValue.current = endValue
      }
    }

    animationRef.current = requestAnimationFrame(animate)

    return () => {
      if (animationRef.current !== null) {
        cancelAnimationFrame(animationRef.current)
      }
    }
  }, [value, duration])

  return formatter(displayValue)
}
