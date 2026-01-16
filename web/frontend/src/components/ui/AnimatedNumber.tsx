import { useEffect, useRef, useState, memo } from 'react'

interface AnimatedNumberProps {
  value: number
  formatter: (value: number) => string
  duration?: number
  className?: string
}

/**
 * Animates numeric value changes with easing.
 * Uses requestAnimationFrame for smooth 60fps animation.
 */
export const AnimatedNumber = memo(function AnimatedNumber({
  value,
  formatter,
  duration = 500,
  className,
}: AnimatedNumberProps) {
  const [displayValue, setDisplayValue] = useState(value)
  const previousValue = useRef(value)
  const animationRef = useRef<number | null>(null)

  useEffect(() => {
    const startValue = previousValue.current
    const endValue = value
    const startTime = performance.now()

    // Skip animation for initial render or zero values
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

  return (
    <span className={className} aria-live="polite">
      {formatter(displayValue)}
    </span>
  )
})
