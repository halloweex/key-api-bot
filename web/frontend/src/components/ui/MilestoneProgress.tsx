import { memo, useMemo, useEffect, useRef, useState, useCallback } from 'react'
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

interface Sparkle {
  id: number
  x: number
  y: number
  size: number
  delay: number
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
  if (period === 'week' || period === 'last_week' || period === 'last_7_days') return 'weekly'
  if (period === 'month' || period === 'last_month' || period === 'last_28_days') return 'monthly'
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

// ─── Sparkle Generator ───────────────────────────────────────────────────────

function generateSparkles(count: number, progressWidth: number): Sparkle[] {
  return Array.from({ length: count }, (_, i) => ({
    id: i,
    x: Math.random() * Math.max(progressWidth - 10, 10),
    y: Math.random() * 100,
    size: Math.random() * 3 + 2,
    delay: Math.random() * 2,
  }))
}

// ─── Sparkle Component ───────────────────────────────────────────────────────

function SparkleParticle({ sparkle }: { sparkle: Sparkle }) {
  return (
    <div
      className="sparkle-particle"
      style={{
        left: `${sparkle.x}px`,
        top: `${sparkle.y}%`,
        width: `${sparkle.size}px`,
        height: `${sparkle.size}px`,
        animationDelay: `${sparkle.delay}s`,
      }}
    />
  )
}

// ─── Leading Edge Glow ───────────────────────────────────────────────────────

function LeadingEdgeGlow({ progress, themeColor }: { progress: number; themeColor: string }) {
  if (progress <= 0 || progress >= 100) return null

  // Extract the main color from theme for glow
  const glowColor = themeColor.includes('green') ? '#22c55e'
    : themeColor.includes('amber') ? '#f59e0b'
    : themeColor.includes('purple') ? '#a855f7'
    : '#3b82f6'

  return (
    <div
      className="leading-edge-glow"
      style={{
        left: `${progress}%`,
        '--glow-color': glowColor,
      } as React.CSSProperties}
    />
  )
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
  const fillRef = useRef<HTMLDivElement>(null)
  const [celebration, setCelebration] = useState<Milestone | null>(null)
  const celebratedRef = useRef<Set<number>>(new Set())
  const [sparkles, setSparkles] = useState<Sparkle[]>([])

  // Regenerate sparkles when progress changes significantly
  const regenerateSparkles = useCallback(() => {
    if (fillRef.current) {
      const width = fillRef.current.offsetWidth
      if (width > 20) {
        setSparkles(generateSparkles(8, width))
      } else {
        setSparkles([])
      }
    }
  }, [])

  // Regenerate sparkles periodically for continuous effect
  useEffect(() => {
    regenerateSparkles()
    const interval = setInterval(regenerateSparkles, 3000)
    return () => clearInterval(interval)
  }, [regenerateSparkles, revenue])

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

    // Determine theme color with gradients
    let themeColor = 'bg-gradient-to-r from-green-400 to-green-500' // default
    let textColor = 'text-green-500'
    let glowColor = 'shadow-green-500/50'
    if (allCompleted) {
      themeColor = 'bg-gradient-to-r from-purple-400 via-purple-500 to-pink-500'
      textColor = 'text-purple-500'
      glowColor = 'shadow-purple-500/50'
    } else if (currentIndex >= 0) {
      themeColor = 'bg-gradient-to-r from-amber-400 to-orange-500'
      textColor = 'text-amber-500'
      glowColor = 'shadow-amber-500/50'
    }

    return {
      maxMilestone,
      progress,
      allCompleted,
      currentIndex,
      nearMilestone,
      themeColor,
      textColor,
      glowColor,
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
      <div className={`bg-white rounded-xl border border-slate-200/60 shadow-[var(--shadow-card)] hover:shadow-[var(--shadow-card-hover)] transition-all duration-300 p-5 ${className}`}>
        {/* Header */}
        <div className="flex justify-between items-center mb-4">
          <div className="flex items-center gap-2">
            <div className={`w-2 h-2 rounded-full ${metrics.themeColor}`} />
            <span className="text-sm font-semibold text-slate-700">{periodLabel}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className={`text-lg font-bold ${metrics.textColor}`}>
              {Math.round(metrics.progress)}%
            </span>
            {metrics.nearMilestone && (
              <span className="text-xs bg-amber-100 text-amber-700 px-2 py-0.5 rounded-full font-medium animate-pulse">
                Almost there!
              </span>
            )}
          </div>
        </div>

        {/* Progress Track */}
        <div
          ref={trackRef}
          className="relative h-4 bg-slate-100 rounded-full shadow-inner progress-track"
          style={{ overflow: 'visible' }}
        >
          {/* Fill with glow */}
          <div
            ref={fillRef}
            className={`progress-fill absolute inset-y-0 left-0 rounded-full transition-all duration-700 ease-out ${metrics.themeColor} ${
              metrics.nearMilestone ? 'shadow-lg ' + metrics.glowColor + ' near-milestone' : ''
            }`}
            style={{
              width: `${metrics.progress}%`,
              boxShadow: metrics.progress > 0 ? `0 0 10px rgba(0,0,0,0.1), inset 0 1px 0 rgba(255,255,255,0.3)` : 'none'
            }}
          >
            {/* Static shine gradient */}
            <div className="absolute inset-0 rounded-full overflow-hidden">
              <div className="absolute inset-0 bg-gradient-to-b from-white/30 to-transparent h-1/2" />
            </div>

            {/* Animated shimmer sweep */}
            <div className="shimmer-sweep" />

            {/* Sparkle particles */}
            {metrics.progress > 5 && sparkles.map(sparkle => (
              <SparkleParticle key={sparkle.id} sparkle={sparkle} />
            ))}

            {/* Ambient floating particles */}
            {metrics.progress > 10 && (
              <div className="floating-particles">
                <div className="floating-particle" style={{ left: '20%', animationDelay: '0s' }} />
                <div className="floating-particle" style={{ left: '50%', animationDelay: '0.5s' }} />
                <div className="floating-particle" style={{ left: '80%', animationDelay: '1s' }} />
              </div>
            )}
          </div>

          {/* Leading edge glow */}
          <LeadingEdgeGlow progress={metrics.progress} themeColor={metrics.themeColor} />

          {/* Milestone Markers */}
          {milestones.map((m, index) => {
            const position = (m.amount / metrics.maxMilestone) * 100
            const isReached = revenue >= m.amount
            const isNext = index === metrics.currentIndex + 1

            // Tier colors with gradients
            let markerBg = 'bg-gradient-to-br from-blue-400 to-blue-600'
            let markerBorder = 'border-blue-300'
            if (index === 0) {
              markerBg = 'bg-gradient-to-br from-amber-400 to-amber-600'
              markerBorder = 'border-amber-300'
            }
            if (index === milestones.length - 1) {
              markerBg = 'bg-gradient-to-br from-purple-400 to-purple-600'
              markerBorder = 'border-purple-300'
            }

            return (
              <div
                key={m.amount}
                className="absolute"
                style={{
                  left: `${position}%`,
                  top: '50%',
                  transform: 'translate(-50%, -50%)',
                  zIndex: isNext ? 20 : 10,
                }}
              >
                {/* Larger hit area for better interaction */}
                <div
                  className={`
                    w-6 h-6 rounded-full border-2 transition-all duration-300 cursor-pointer
                    flex items-center justify-center
                    ${markerBg} ${markerBorder}
                    ${isReached ? 'scale-110 shadow-lg ring-2 ring-white' : 'opacity-50 scale-90'}
                    ${isNext ? 'animate-pulse ring-2 ring-offset-2 ring-offset-slate-100 ring-slate-400/50' : ''}
                    hover:scale-150 hover:shadow-xl hover:opacity-100 hover:z-30
                  `}
                  title={`${formatAmount(m.amount)} - ${m.message}`}
                  role="button"
                  tabIndex={0}
                  aria-label={`Milestone: ${formatAmount(m.amount)}`}
                >
                  {isReached && (
                    <svg className="w-3 h-3 text-white" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                    </svg>
                  )}
                </div>
              </div>
            )
          })}
        </div>

        {/* Labels */}
        <div className="flex justify-between mt-3">
          <div>
            <span className={`text-base font-bold ${metrics.textColor}`}>
              {formatCurrency(revenue)}
            </span>
            <span className="text-xs text-slate-400 ml-1">current</span>
          </div>
          <div className="text-right">
            <span className="text-base font-semibold text-slate-600">
              {formatAmount(metrics.maxMilestone)}
            </span>
            <span className="text-xs text-slate-400 ml-1">goal</span>
          </div>
        </div>

        {/* Remaining amount */}
        {!metrics.allCompleted && (
          <div className="mt-3 pt-3 border-t border-slate-100">
            <p className="text-xs text-slate-500 text-center">
              <span className="font-medium text-slate-700">{formatCurrency(metrics.maxMilestone - revenue)}</span> remaining to reach goal
            </p>
          </div>
        )}

        {/* Completed state */}
        {metrics.allCompleted && (
          <div className="mt-3 pt-3 border-t border-slate-100">
            <p className="text-xs text-center font-medium text-purple-600 flex items-center justify-center gap-1">
              <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
              </svg>
              All milestones achieved!
            </p>
          </div>
        )}
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
        /* ─── Base Animations ─────────────────────────────────────────────── */
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

        /* ─── Shimmer Sweep Effect ────────────────────────────────────────── */
        @keyframes shimmer-sweep {
          0% { transform: translateX(-100%); }
          100% { transform: translateX(200%); }
        }
        .shimmer-sweep {
          position: absolute;
          inset: 0;
          border-radius: inherit;
          overflow: hidden;
        }
        .shimmer-sweep::after {
          content: '';
          position: absolute;
          top: 0;
          left: 0;
          width: 50%;
          height: 100%;
          background: linear-gradient(
            90deg,
            transparent 0%,
            rgba(255, 255, 255, 0.4) 50%,
            transparent 100%
          );
          animation: shimmer-sweep 2.5s ease-in-out infinite;
        }

        /* ─── Sparkle Particles ───────────────────────────────────────────── */
        @keyframes sparkle {
          0%, 100% {
            opacity: 0;
            transform: scale(0) rotate(0deg);
          }
          50% {
            opacity: 1;
            transform: scale(1) rotate(180deg);
          }
        }
        @keyframes sparkle-flicker {
          0%, 100% { opacity: 0.3; transform: scale(0.8); }
          25% { opacity: 1; transform: scale(1.2); }
          50% { opacity: 0.6; transform: scale(1); }
          75% { opacity: 1; transform: scale(1.1); }
        }
        .sparkle-particle {
          position: absolute;
          background: radial-gradient(circle, #fff 0%, rgba(255,255,255,0) 70%);
          border-radius: 50%;
          pointer-events: none;
          animation: sparkle-flicker 1.5s ease-in-out infinite;
          box-shadow: 0 0 4px 1px rgba(255, 255, 255, 0.8);
        }

        /* ─── Floating Particles ──────────────────────────────────────────── */
        @keyframes float-up {
          0% {
            opacity: 0;
            transform: translateY(100%) scale(0);
          }
          20% {
            opacity: 1;
            transform: translateY(50%) scale(1);
          }
          100% {
            opacity: 0;
            transform: translateY(-150%) scale(0.5);
          }
        }
        .floating-particles {
          position: absolute;
          inset: 0;
          overflow: visible;
          pointer-events: none;
        }
        .floating-particle {
          position: absolute;
          bottom: 0;
          width: 4px;
          height: 4px;
          background: rgba(255, 255, 255, 0.9);
          border-radius: 50%;
          box-shadow: 0 0 6px 2px rgba(255, 255, 255, 0.6);
          animation: float-up 2s ease-out infinite;
        }

        /* ─── Leading Edge Glow ───────────────────────────────────────────── */
        @keyframes edge-pulse {
          0%, 100% {
            opacity: 0.7;
            transform: translate(-50%, -50%) scale(1);
          }
          50% {
            opacity: 1;
            transform: translate(-50%, -50%) scale(1.3);
          }
        }
        @keyframes edge-ring {
          0% {
            opacity: 0.8;
            transform: translate(-50%, -50%) scale(0.8);
          }
          100% {
            opacity: 0;
            transform: translate(-50%, -50%) scale(2);
          }
        }
        .leading-edge-glow {
          position: absolute;
          top: 50%;
          width: 12px;
          height: 12px;
          border-radius: 50%;
          background: var(--glow-color, #22c55e);
          transform: translate(-50%, -50%);
          animation: edge-pulse 1.5s ease-in-out infinite;
          box-shadow:
            0 0 8px 2px var(--glow-color, #22c55e),
            0 0 16px 4px var(--glow-color, #22c55e);
          z-index: 15;
        }
        .leading-edge-glow::before {
          content: '';
          position: absolute;
          top: 50%;
          left: 50%;
          width: 20px;
          height: 20px;
          border-radius: 50%;
          border: 2px solid var(--glow-color, #22c55e);
          transform: translate(-50%, -50%);
          animation: edge-ring 1.5s ease-out infinite;
        }

        /* ─── Near Milestone Pulse ────────────────────────────────────────── */
        @keyframes near-milestone-pulse {
          0%, 100% {
            box-shadow:
              0 0 10px rgba(0,0,0,0.1),
              inset 0 1px 0 rgba(255,255,255,0.3),
              0 0 20px 2px rgba(245, 158, 11, 0.3);
          }
          50% {
            box-shadow:
              0 0 10px rgba(0,0,0,0.1),
              inset 0 1px 0 rgba(255,255,255,0.3),
              0 0 30px 6px rgba(245, 158, 11, 0.5);
          }
        }
        .progress-fill.near-milestone {
          animation: near-milestone-pulse 1.5s ease-in-out infinite;
        }

        /* ─── Track Background Pattern ────────────────────────────────────── */
        .progress-track {
          background:
            linear-gradient(90deg, rgba(0,0,0,0.02) 0%, transparent 50%, rgba(0,0,0,0.02) 100%),
            #f1f5f9;
        }
      `}</style>
    </>
  )
})
