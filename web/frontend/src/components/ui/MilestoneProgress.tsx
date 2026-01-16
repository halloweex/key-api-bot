import { memo, useMemo, useEffect, useRef, useState } from 'react'
import { useFilterStore } from '../../store/filterStore'
import { formatCurrency } from '../../utils/formatters'

// ─── Types ────────────────────────────────────────────────────────────────────

interface Milestone {
  amount: number
  message: string
  emoji: string
}

interface MilestoneProgressProps {
  revenue: number
  className?: string
}

// ─── Milestone Configuration ──────────────────────────────────────────────────

const MILESTONES: Record<string, Milestone[]> = {
  daily: [
    { amount: 200000, message: '200K Daily Revenue!', emoji: '🎉' },
  ],
  weekly: [
    { amount: 800000, message: '800K Weekly Revenue!', emoji: '🔥' },
    { amount: 1000000, message: '1 MILLION Weekly!', emoji: '💰🎊' },
  ],
  monthly: [
    { amount: 4000000, message: '4 MILLION Monthly!', emoji: '👑🎇🎆' },
  ],
}

// Map period to milestone type
function getPeriodType(period: string): string | null {
  if (period === 'today' || period === 'yesterday') return 'daily'
  if (period === 'week' || period === 'last_week') return 'weekly'
  if (period === 'month' || period === 'last_month') return 'monthly'
  return null
}

// Format amount for display
function formatAmount(amount: number): string {
  if (amount >= 1000000) {
    return `₴${(amount / 1000000).toFixed(1)}M`
  }
  if (amount >= 1000) {
    return `₴${(amount / 1000).toFixed(0)}K`
  }
  return `₴${amount.toFixed(0)}`
}

// ─── Particle Effect ──────────────────────────────────────────────────────────

function createParticle(container: HTMLElement, x: number) {
  const particle = document.createElement('div')
  const colors = ['#FFD700', '#FFA500', '#FF6347', '#7C3AED', '#16A34A', '#2563EB', '#EC4899']
  const color = colors[Math.floor(Math.random() * colors.length)]
  const size = Math.random() * 8 + 4
  const angle = Math.random() * Math.PI * 2
  const velocity = Math.random() * 80 + 40
  const duration = Math.random() * 800 + 600

  particle.style.cssText = `
    position: absolute;
    width: ${size}px;
    height: ${size}px;
    background: ${color};
    border-radius: 50%;
    left: ${x}px;
    top: 50%;
    pointer-events: none;
    z-index: 100;
    box-shadow: 0 0 ${size}px ${color};
  `

  container.appendChild(particle)

  const dx = Math.cos(angle) * velocity
  const dy = Math.sin(angle) * velocity - 30

  particle.animate([
    { transform: 'translate(0, 0) scale(1)', opacity: 1 },
    { transform: `translate(${dx}px, ${dy}px) scale(0)`, opacity: 0 }
  ], {
    duration,
    easing: 'cubic-bezier(0, 0.5, 0.5, 1)',
    fill: 'forwards'
  })

  setTimeout(() => particle.remove(), duration + 100)
}

function burstParticles(container: HTMLElement, x: number, count: number = 20) {
  for (let i = 0; i < count; i++) {
    setTimeout(() => createParticle(container, x), i * 20)
  }
}

// ─── Celebration Overlay ──────────────────────────────────────────────────────

function CelebrationOverlay({ milestone, onClose }: { milestone: Milestone; onClose: () => void }) {
  useEffect(() => {
    const timer = setTimeout(onClose, 4000)
    return () => clearTimeout(timer)
  }, [onClose])

  return (
    <div
      className="fixed inset-0 bg-black/80 flex items-center justify-center z-50 animate-fade-in"
      onClick={onClose}
    >
      <div className="text-center animate-bounce-in">
        <div className="text-6xl mb-4">{milestone.emoji}</div>
        <h2 className="text-3xl font-bold text-white mb-2">{milestone.message}</h2>
        <p className="text-slate-300">
          Congratulations on reaching {formatAmount(milestone.amount)}!
        </p>
      </div>
    </div>
  )
}

// ─── Main Component ───────────────────────────────────────────────────────────

export const MilestoneProgress = memo(function MilestoneProgress({
  revenue,
  className = ''
}: MilestoneProgressProps) {
  const { period } = useFilterStore()
  const trackRef = useRef<HTMLDivElement>(null)
  const [celebration, setCelebration] = useState<Milestone | null>(null)
  const celebratedRef = useRef<Set<number>>(new Set())

  // Get milestones for current period
  const periodType = getPeriodType(period)
  const milestones = periodType ? MILESTONES[periodType] : null

  // Calculate progress metrics
  const metrics = useMemo(() => {
    if (!milestones || milestones.length === 0) return null

    const maxMilestone = milestones[milestones.length - 1].amount
    const progress = Math.min((revenue / maxMilestone) * 100, 100)
    const allCompleted = revenue >= maxMilestone

    // Find current milestone index (highest reached)
    let currentIndex = -1
    for (let i = milestones.length - 1; i >= 0; i--) {
      if (revenue >= milestones[i].amount) {
        currentIndex = i
        break
      }
    }

    // Check if near a milestone (within 10%)
    let nearMilestone = false
    for (const m of milestones) {
      const percentTo = (revenue / m.amount) * 100
      if (percentTo >= 90 && percentTo < 100) {
        nearMilestone = true
        break
      }
    }

    // Determine theme color
    let themeColor = 'bg-green-500' // default
    let textColor = 'text-green-500'
    if (allCompleted) {
      themeColor = 'bg-purple-500'
      textColor = 'text-purple-500'
    } else if (currentIndex >= 0) {
      themeColor = 'bg-amber-500'
      textColor = 'text-amber-500'
    }

    return {
      maxMilestone,
      progress,
      allCompleted,
      currentIndex,
      nearMilestone,
      themeColor,
      textColor,
    }
  }, [milestones, revenue])

  // Trigger particles and celebrations
  useEffect(() => {
    if (!milestones || !trackRef.current || !metrics) return

    milestones.forEach((m, index) => {
      if (revenue >= m.amount && !celebratedRef.current.has(m.amount)) {
        celebratedRef.current.add(m.amount)

        // Trigger particles
        const trackRect = trackRef.current!.getBoundingClientRect()
        const position = (m.amount / metrics.maxMilestone) * 100
        const xPos = (position / 100) * trackRect.width

        setTimeout(() => {
          if (trackRef.current) {
            burstParticles(trackRef.current, xPos, 25)
          }
        }, index * 300)

        // Show celebration for highest new milestone
        if (index === milestones.length - 1 || revenue < milestones[index + 1]?.amount) {
          setTimeout(() => setCelebration(m), 500)
        }
      }
    })
  }, [milestones, revenue, metrics])

  // Reset celebrations when period changes
  useEffect(() => {
    celebratedRef.current.clear()
  }, [period])

  // Don't render for custom periods or if no milestones
  if (!periodType || !milestones || milestones.length === 0 || !metrics) {
    return null
  }

  const periodLabel = periodType === 'daily' ? 'Daily Goal'
    : periodType === 'weekly' ? 'Weekly Goal'
    : 'Monthly Goal'

  return (
    <>
      <div className={`bg-white rounded-lg border border-slate-200 shadow-sm p-4 ${className}`}>
        {/* Header */}
        <div className="flex justify-between items-center mb-3">
          <span className="text-sm font-medium text-slate-600">{periodLabel}</span>
          <span className={`text-sm font-bold ${metrics.textColor}`}>
            {Math.round(metrics.progress)}%
          </span>
        </div>

        {/* Progress Track */}
        <div
          ref={trackRef}
          className="relative h-3 bg-slate-200 rounded-full overflow-visible"
        >
          {/* Fill */}
          <div
            className={`absolute inset-y-0 left-0 rounded-full transition-all duration-700 ease-out ${metrics.themeColor} ${
              metrics.nearMilestone ? 'animate-pulse' : ''
            }`}
            style={{ width: `${metrics.progress}%` }}
          />

          {/* Milestone Markers */}
          {milestones.map((m, index) => {
            const position = (m.amount / metrics.maxMilestone) * 100
            const isReached = revenue >= m.amount
            const isNext = index === metrics.currentIndex + 1

            // Tier colors
            let markerColor = 'bg-blue-500 border-blue-400'
            if (index === 0) markerColor = 'bg-amber-500 border-amber-400'
            if (index === milestones.length - 1) markerColor = 'bg-purple-500 border-purple-400'

            return (
              <div
                key={m.amount}
                className={`absolute top-1/2 -translate-y-1/2 w-4 h-4 rounded-full border-2 transition-all duration-300 ${markerColor} ${
                  isReached ? 'scale-110 shadow-lg' : 'opacity-50'
                } ${isNext ? 'ring-2 ring-slate-400/50 ring-offset-1 ring-offset-white' : ''}`}
                style={{ left: `${position}%`, transform: `translateX(-50%) translateY(-50%)` }}
                title={formatAmount(m.amount)}
              />
            )
          })}
        </div>

        {/* Labels */}
        <div className="flex justify-between mt-2">
          <span className={`text-sm font-medium ${metrics.textColor}`}>
            {formatCurrency(revenue)}
          </span>
          <span className="text-sm text-slate-600">
            {formatAmount(metrics.maxMilestone)}
          </span>
        </div>
      </div>

      {/* Celebration Overlay */}
      {celebration && (
        <CelebrationOverlay
          milestone={celebration}
          onClose={() => setCelebration(null)}
        />
      )}

      {/* Animation Styles */}
      <style>{`
        @keyframes fade-in {
          from { opacity: 0; }
          to { opacity: 1; }
        }
        @keyframes bounce-in {
          0% { transform: scale(0.5); opacity: 0; }
          50% { transform: scale(1.1); }
          100% { transform: scale(1); opacity: 1; }
        }
        .animate-fade-in { animation: fade-in 0.3s ease-out; }
        .animate-bounce-in { animation: bounce-in 0.5s ease-out; }
      `}</style>
    </>
  )
})
