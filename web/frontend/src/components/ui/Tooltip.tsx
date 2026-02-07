import { useState, useRef, useEffect, type ReactNode } from 'react'

type TooltipPosition = 'top' | 'bottom' | 'left' | 'right'

interface TooltipProps {
  content: ReactNode
  children: ReactNode
  position?: TooltipPosition
  delay?: number
  className?: string
}

export function Tooltip({
  content,
  children,
  position = 'top',
  delay = 200,
  className = '',
}: TooltipProps) {
  const [isVisible, setIsVisible] = useState(false)
  const [coords, setCoords] = useState({ x: 0, y: 0 })
  const triggerRef = useRef<HTMLDivElement>(null)
  const tooltipRef = useRef<HTMLDivElement>(null)
  const timeoutRef = useRef<number | undefined>(undefined)

  const showTooltip = () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current)
    }
    timeoutRef.current = window.setTimeout(() => {
      setIsVisible(true)
    }, delay)
  }

  const hideTooltip = () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current)
    }
    setIsVisible(false)
  }

  useEffect(() => {
    if (isVisible && triggerRef.current && tooltipRef.current) {
      const trigger = triggerRef.current.getBoundingClientRect()
      const tooltip = tooltipRef.current.getBoundingClientRect()

      let x = 0
      let y = 0
      const gap = 8

      switch (position) {
        case 'top':
          x = trigger.left + trigger.width / 2 - tooltip.width / 2
          y = trigger.top - tooltip.height - gap
          break
        case 'bottom':
          x = trigger.left + trigger.width / 2 - tooltip.width / 2
          y = trigger.bottom + gap
          break
        case 'left':
          x = trigger.left - tooltip.width - gap
          y = trigger.top + trigger.height / 2 - tooltip.height / 2
          break
        case 'right':
          x = trigger.right + gap
          y = trigger.top + trigger.height / 2 - tooltip.height / 2
          break
      }

      // Keep tooltip within viewport
      x = Math.max(8, Math.min(x, window.innerWidth - tooltip.width - 8))
      y = Math.max(8, Math.min(y, window.innerHeight - tooltip.height - 8))

      setCoords({ x, y })
    }
  }, [isVisible, position])

  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current)
      }
    }
  }, [])

  const arrowPosition = {
    top: 'bottom-[-4px] left-1/2 -translate-x-1/2 border-l-transparent border-r-transparent border-b-transparent border-t-slate-800',
    bottom: 'top-[-4px] left-1/2 -translate-x-1/2 border-l-transparent border-r-transparent border-t-transparent border-b-slate-800',
    left: 'right-[-4px] top-1/2 -translate-y-1/2 border-t-transparent border-b-transparent border-r-transparent border-l-slate-800',
    right: 'left-[-4px] top-1/2 -translate-y-1/2 border-t-transparent border-b-transparent border-l-transparent border-r-slate-800',
  }

  return (
    <>
      <div
        ref={triggerRef}
        onMouseEnter={showTooltip}
        onMouseLeave={hideTooltip}
        onFocus={showTooltip}
        onBlur={hideTooltip}
        className="inline-block"
      >
        {children}
      </div>

      {isVisible && (
        <div
          ref={tooltipRef}
          role="tooltip"
          style={{
            position: 'fixed',
            left: coords.x,
            top: coords.y,
            zIndex: 9999,
          }}
          className={`
            px-3 py-2 text-sm text-white bg-slate-800 rounded-lg shadow-lg
            animate-tooltip-in pointer-events-none
            ${className}
          `}
        >
          {content}
          <div
            className={`absolute w-0 h-0 border-4 ${arrowPosition[position]}`}
          />
        </div>
      )}
    </>
  )
}
