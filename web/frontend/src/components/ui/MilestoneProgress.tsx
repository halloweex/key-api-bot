import { memo, useMemo, useEffect, useRef, useState, useCallback, lazy, Suspense } from 'react'
import { useTranslation } from 'react-i18next'
import { CheckCircle, CircleHelp } from 'lucide-react'
import { useFilterStore } from '../../store/filterStore'
import { formatCurrency } from '../../utils/formatters'
import { useSmartGoals, useRevenueTrend } from '../../hooks'
import confettiAnimation from '../../assets/animations/confetti-burst.json'

const LottiePlayer = lazy(() => import('lottie-react'))

// ─── Types ────────────────────────────────────────────────────────────────────

interface Milestone {
  amount: number
  message: string
  emoji: string
  isCustom?: boolean
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

// ─── Fallback Goals (used while loading or on error) ─────────────────────────

type TFunc = (key: string, opts?: Record<string, string | number>) => string

const getFallbackMilestones = (t: TFunc): Record<string, Milestone[]> => ({
  daily: [
    { amount: 200000, message: t('goal.dailyRevenue', { amount: '200K' }), emoji: '🎉' },
  ],
  weekly: [
    { amount: 800000, message: t('goal.weeklyRevenue', { amount: '800K' }), emoji: '🔥' },
    { amount: 1000000, message: t('goal.weeklyRevenue', { amount: '1M' }), emoji: '💰🎊' },
  ],
  monthly: [
    { amount: 1000000, message: t('goal.weekLabel', { num: 1, amount: '1M' }), emoji: '🔥' },
    { amount: 2000000, message: t('goal.weekLabel', { num: 2, amount: '2M' }), emoji: '⚡' },
    { amount: 3000000, message: t('goal.weekLabel', { num: 3, amount: '3M' }), emoji: '💪' },
    { amount: 4000000, message: t('goal.monthlyRevenue', { amount: '4M' }), emoji: '👑🎇🎆' },
  ],
})

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

// ─── SVG Sparkle Generator ───────────────────────────────────────────────────

function generateSparkles(count: number, progressWidth: number): Sparkle[] {
  return Array.from({ length: count }, (_, i) => ({
    id: i,
    x: Math.random() * Math.max(progressWidth - 10, 10),
    y: Math.random() * 100,
    size: Math.random() * 3 + 2,
    delay: Math.random() * 2,
  }))
}

// ─── SVG Sparkle Star (4-point diamond sparkle) ─────────────────────────────

function SvgSparkle({ sparkle }: { sparkle: Sparkle }) {
  const s = sparkle.size
  return (
    <svg
      className="absolute pointer-events-none"
      style={{ left: sparkle.x, top: `${sparkle.y}%`, width: s * 3, height: s * 3, transform: 'translate(-50%, -50%)' }}
      viewBox="0 0 24 24"
    >
      {/* 4-point diamond sparkle - cleaner than star */}
      <path
        d="M12 0 L14.5 9.5 L24 12 L14.5 14.5 L12 24 L9.5 14.5 L0 12 L9.5 9.5 Z"
        fill="white"
        fillOpacity="0.95"
      >
        <animate attributeName="opacity" values="0.2;1;0.2" dur={`${1.0 + sparkle.delay * 0.3}s`} repeatCount="indefinite" />
        <animateTransform attributeName="transform" type="scale" values="0.4;1.2;0.4" dur={`${1.2 + sparkle.delay * 0.2}s`} repeatCount="indefinite" additive="sum" />
        <animateTransform attributeName="transform" type="rotate" from="0 12 12" to="180 12 12" dur={`${2.5 + sparkle.delay}s`} repeatCount="indefinite" additive="sum" />
      </path>
    </svg>
  )
}

// ─── SVG Shimmer Overlay ────────────────────────────────────────────────────

function SvgShimmer({ uniqueId }: { uniqueId: string }) {
  return (
    <svg className="absolute inset-0 w-full h-full rounded-full overflow-hidden pointer-events-none" preserveAspectRatio="none">
      <defs>
        <linearGradient id={`shimmer-${uniqueId}`} x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor="white" stopOpacity="0" />
          <stop offset="40%" stopColor="white" stopOpacity="0.2" />
          <stop offset="50%" stopColor="white" stopOpacity="0.5" />
          <stop offset="60%" stopColor="white" stopOpacity="0.2" />
          <stop offset="100%" stopColor="white" stopOpacity="0" />
          <animateTransform attributeName="gradientTransform" type="translate" from="-1.5 0" to="2.5 0" dur="2.5s" repeatCount="indefinite" />
        </linearGradient>
      </defs>
      <rect width="100%" height="100%" fill={`url(#shimmer-${uniqueId})`} />
    </svg>
  )
}

// ─── SVG Liquid Wave Top Edge ───────────────────────────────────────────────

function SvgLiquidWave({ themeColor }: { themeColor: string }) {
  const color = themeColor.includes('green') ? '#22c55e'
    : themeColor.includes('amber') ? '#f59e0b'
    : themeColor.includes('purple') ? '#a855f7'
    : '#3b82f6'

  return (
    <svg
      className="absolute pointer-events-none"
      style={{ top: -4, left: 0, width: '100%', height: 8, zIndex: 5 }}
      viewBox="0 0 200 8"
      preserveAspectRatio="none"
    >
      {/* Wave 1 — darker, slower */}
      <path fill={color} fillOpacity="0.4">
        <animate
          attributeName="d"
          values="M0 4 Q10 0,20 4 Q30 8,40 4 Q50 0,60 4 Q70 8,80 4 Q90 0,100 4 Q110 8,120 4 Q130 0,140 4 Q150 8,160 4 Q170 0,180 4 Q190 8,200 4 L200 8 L0 8 Z;M0 4 Q10 8,20 4 Q30 0,40 4 Q50 8,60 4 Q70 0,80 4 Q90 8,100 4 Q110 0,120 4 Q130 8,140 4 Q150 0,160 4 Q170 8,180 4 Q190 0,200 4 L200 8 L0 8 Z;M0 4 Q10 0,20 4 Q30 8,40 4 Q50 0,60 4 Q70 8,80 4 Q90 0,100 4 Q110 8,120 4 Q130 0,140 4 Q150 8,160 4 Q170 0,180 4 Q190 8,200 4 L200 8 L0 8 Z"
          dur="3s"
          repeatCount="indefinite"
        />
      </path>
      {/* Wave 2 — lighter, faster */}
      <path fill="white" fillOpacity="0.3">
        <animate
          attributeName="d"
          values="M0 5 Q15 2,30 5 Q45 8,60 5 Q75 2,90 5 Q105 8,120 5 Q135 2,150 5 Q165 8,180 5 Q195 2,200 5 L200 8 L0 8 Z;M0 5 Q15 8,30 5 Q45 2,60 5 Q75 8,90 5 Q105 2,120 5 Q135 8,150 5 Q165 2,180 5 Q195 8,200 5 L200 8 L0 8 Z;M0 5 Q15 2,30 5 Q45 8,60 5 Q75 2,90 5 Q105 8,120 5 Q135 2,150 5 Q165 8,180 5 Q195 2,200 5 L200 8 L0 8 Z"
          dur="2s"
          repeatCount="indefinite"
        />
      </path>
    </svg>
  )
}

// ─── SVG Traveling Light Orbs ───────────────────────────────────────────────

function SvgTravelingOrbs({ uniqueId }: { uniqueId: string }) {
  const orbs = [
    { dur: '3s', delay: '0s', r: 3, opacity: 0.7 },
    { dur: '4s', delay: '1s', r: 2.5, opacity: 0.5 },
    { dur: '3.5s', delay: '2s', r: 2, opacity: 0.6 },
  ]
  return (
    <svg className="absolute inset-0 w-full h-full rounded-full overflow-hidden pointer-events-none" preserveAspectRatio="none">
      <defs>
        {orbs.map((_, i) => (
          <radialGradient key={i} id={`orb-glow-${uniqueId}-${i}`}>
            <stop offset="0%" stopColor="white" stopOpacity="1" />
            <stop offset="50%" stopColor="white" stopOpacity="0.5" />
            <stop offset="100%" stopColor="white" stopOpacity="0" />
          </radialGradient>
        ))}
      </defs>
      {orbs.map((o, i) => (
        <circle
          key={i}
          cy="50%"
          r={o.r}
          fill={`url(#orb-glow-${uniqueId}-${i})`}
          opacity={o.opacity}
        >
          <animate
            attributeName="cx"
            values="0%;100%"
            dur={o.dur}
            begin={o.delay}
            repeatCount="indefinite"
          />
          <animate
            attributeName="opacity"
            values={`0;${o.opacity};${o.opacity};0`}
            dur={o.dur}
            begin={o.delay}
            repeatCount="indefinite"
          />
          <animate
            attributeName="r"
            values={`${o.r * 0.7};${o.r * 1.3};${o.r * 0.7}`}
            dur="1.2s"
            repeatCount="indefinite"
          />
        </circle>
      ))}
    </svg>
  )
}

// ─── SVG Leading Edge ───────────────────────────────────────────────────────

function SvgLeadingEdge({ progress, themeColor }: { progress: number; themeColor: string }) {
  if (progress <= 0 || progress >= 100) return null

  const color = themeColor.includes('green') ? '#22c55e'
    : themeColor.includes('amber') ? '#f59e0b'
    : themeColor.includes('purple') ? '#a855f7'
    : '#3b82f6'

  return (
    <svg
      className="absolute pointer-events-none"
      style={{ left: `${progress}%`, top: '50%', transform: 'translate(-50%, -50%)', width: 32, height: 32, zIndex: 15 }}
      viewBox="0 0 32 32"
    >
      {/* Outer expanding ring */}
      <circle cx="16" cy="16" r="8" fill="none" stroke={color} strokeWidth="1" opacity="0.4">
        <animate attributeName="r" values="6;14;6" dur="2s" repeatCount="indefinite" />
        <animate attributeName="opacity" values="0.5;0;0.5" dur="2s" repeatCount="indefinite" />
      </circle>
      {/* Inner expanding ring */}
      <circle cx="16" cy="16" r="5" fill="none" stroke="white" strokeWidth="1" opacity="0.3">
        <animate attributeName="r" values="4;10;4" dur="2s" begin="0.5s" repeatCount="indefinite" />
        <animate attributeName="opacity" values="0.4;0;0.4" dur="2s" begin="0.5s" repeatCount="indefinite" />
      </circle>
      {/* Core dot with glow */}
      <circle cx="16" cy="16" r="5" fill={color} filter="url(#edgeGlow)">
        <animate attributeName="r" values="4;6;4" dur="1.5s" repeatCount="indefinite" />
      </circle>
      <circle cx="16" cy="16" r="3" fill="white" fillOpacity="0.6">
        <animate attributeName="r" values="2;3.5;2" dur="1.5s" repeatCount="indefinite" />
        <animate attributeName="opacity" values="0.4;0.8;0.4" dur="1.5s" repeatCount="indefinite" />
      </circle>
    </svg>
  )
}

// ─── SVG Floating Bubbles ───────────────────────────────────────────────────

function SvgFloatingBubbles() {
  const bubbles = [
    { cx: '15%', delay: '0s', dur: '2.2s', r: 1.5 },
    { cx: '35%', delay: '0.5s', dur: '2.8s', r: 2 },
    { cx: '55%', delay: '1.0s', dur: '2.5s', r: 1.8 },
    { cx: '75%', delay: '1.5s', dur: '2.0s', r: 1.5 },
    { cx: '90%', delay: '0.3s', dur: '2.6s', r: 2.2 },
  ]
  return (
    <svg className="absolute inset-0 w-full h-full pointer-events-none" style={{ overflow: 'visible' }}>
      {bubbles.map((b, i) => (
        <circle key={i} cx={b.cx} cy="100%" r={b.r} fill="white" fillOpacity="0.8">
          <animate attributeName="cy" values="110%;-20%" dur={b.dur} begin={b.delay} repeatCount="indefinite" />
          <animate attributeName="opacity" values="0;0.8;0.6;0" dur={b.dur} begin={b.delay} repeatCount="indefinite" />
          <animate attributeName="r" values={`${b.r * 0.6};${b.r};${b.r * 0.4}`} dur={b.dur} begin={b.delay} repeatCount="indefinite" />
        </circle>
      ))}
    </svg>
  )
}

// ─── SVG Color-Shifting Gradient Fill ────────────────────────────────────────

function SvgColorShiftGradient({ uniqueId, themeColor }: { uniqueId: string; themeColor: string }) {
  // Determine color palette based on theme
  const colors = themeColor.includes('purple')
    ? { c1: '#a855f7', c2: '#ec4899', c3: '#8b5cf6', c4: '#d946ef' }
    : themeColor.includes('amber')
    ? { c1: '#f59e0b', c2: '#ef4444', c3: '#f97316', c4: '#eab308' }
    : { c1: '#22c55e', c2: '#06b6d4', c3: '#10b981', c4: '#14b8a6' }

  return (
    <svg className="absolute inset-0 w-full h-full rounded-full pointer-events-none" preserveAspectRatio="none" style={{ mixBlendMode: 'overlay', opacity: 0.4 }}>
      <defs>
        <linearGradient id={`shift-${uniqueId}`} x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%">
            <animate attributeName="stop-color" values={`${colors.c1};${colors.c2};${colors.c3};${colors.c4};${colors.c1}`} dur="6s" repeatCount="indefinite" />
          </stop>
          <stop offset="50%">
            <animate attributeName="stop-color" values={`${colors.c3};${colors.c1};${colors.c4};${colors.c2};${colors.c3}`} dur="6s" repeatCount="indefinite" />
          </stop>
          <stop offset="100%">
            <animate attributeName="stop-color" values={`${colors.c2};${colors.c4};${colors.c1};${colors.c3};${colors.c2}`} dur="6s" repeatCount="indefinite" />
          </stop>
        </linearGradient>
      </defs>
      <rect width="100%" height="100%" fill={`url(#shift-${uniqueId})`} />
    </svg>
  )
}

// ─── SVG Milestone Marker ───────────────────────────────────────────────────

function SvgMilestoneMarker({ isReached, isNext, isFinal }: { isReached: boolean; isNext: boolean; isFinal: boolean }) {
  if (isReached) {
    // Animated checkmark with ring burst
    return (
      <svg viewBox="0 0 28 28" className="w-full h-full">
        <defs>
          <linearGradient id="checkGrad" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stopColor={isFinal ? '#a855f7' : '#f59e0b'} />
            <stop offset="100%" stopColor={isFinal ? '#ec4899' : '#ef4444'} />
          </linearGradient>
        </defs>
        {/* Success ring burst */}
        <circle cx="14" cy="14" r="10" fill="none" stroke={isFinal ? '#a855f7' : '#f59e0b'} strokeWidth="1" opacity="0">
          <animate attributeName="r" values="8;16" dur="1.5s" repeatCount="indefinite" />
          <animate attributeName="opacity" values="0.6;0" dur="1.5s" repeatCount="indefinite" />
        </circle>
        {/* Background circle */}
        <circle cx="14" cy="14" r="10" fill="url(#checkGrad)" />
        {/* White border */}
        <circle cx="14" cy="14" r="10" fill="none" stroke="white" strokeWidth="1.5" strokeOpacity="0.5" />
        {/* Checkmark */}
        <path d="M9 14 L12.5 17.5 L19 11" fill="none" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
          <animate attributeName="stroke-dasharray" values="0 20;20 0" dur="0.4s" fill="freeze" />
        </path>
      </svg>
    )
  }

  if (isNext) {
    // Beacon pulse for next milestone
    return (
      <svg viewBox="0 0 28 28" className="w-full h-full">
        {/* Pulsing beacon rings */}
        <circle cx="14" cy="14" r="10" fill="none" stroke="#94a3b8" strokeWidth="1" opacity="0">
          <animate attributeName="r" values="6;13" dur="1.8s" repeatCount="indefinite" />
          <animate attributeName="opacity" values="0.5;0" dur="1.8s" repeatCount="indefinite" />
        </circle>
        <circle cx="14" cy="14" r="7" fill="none" stroke="#94a3b8" strokeWidth="1" opacity="0">
          <animate attributeName="r" values="6;13" dur="1.8s" begin="0.6s" repeatCount="indefinite" />
          <animate attributeName="opacity" values="0.3;0" dur="1.8s" begin="0.6s" repeatCount="indefinite" />
        </circle>
        {/* Target circle */}
        <circle cx="14" cy="14" r="8" fill="white" stroke={isFinal ? '#8b5cf6' : '#64748b'} strokeWidth="2" />
        <circle cx="14" cy="14" r="4.5" fill="none" stroke={isFinal ? '#8b5cf6' : '#64748b'} strokeWidth="1.5" opacity="0.5" />
        <circle cx="14" cy="14" r="2" fill={isFinal ? '#8b5cf6' : '#64748b'}>
          <animate attributeName="r" values="1.5;2.5;1.5" dur="1.5s" repeatCount="indefinite" />
        </circle>
      </svg>
    )
  }

  // Unreached: flag icon (simplified)
  return (
    <svg viewBox="0 0 28 28" className="w-full h-full">
      <circle cx="14" cy="14" r="9" fill="white" stroke={isFinal ? '#c084fc' : '#cbd5e1'} strokeWidth="1.5" />
      {/* Simplified flag shape */}
      <path d="M11 8 L11 20 M11 8 L18 11 L11 14" fill={isFinal ? '#c084fc' : '#cbd5e1'} stroke={isFinal ? '#a855f7' : '#94a3b8'} strokeWidth="1" strokeLinejoin="round" fillOpacity="0.3" />
    </svg>
  )
}

// ─── Celebration Overlay with Lottie Confetti ────────────────────────────────

function CelebrationOverlay({ milestone, onClose, t }: { milestone: Milestone; onClose: () => void; t: TFunc }) {
  useEffect(() => {
    const timer = setTimeout(onClose, 5000)
    return () => clearTimeout(timer)
  }, [onClose])

  return (
    <div
      className="fixed inset-0 bg-black/80 flex items-center justify-center z-50 animate-fade-in"
      onClick={onClose}
    >
      {/* Lottie confetti bursts — positioned around the center */}
      <Suspense fallback={null}>
        <div className="absolute inset-0 pointer-events-none overflow-hidden">
          <div className="absolute top-1/4 left-1/4 w-48 h-48 -translate-x-1/2 -translate-y-1/2">
            <LottiePlayer animationData={confettiAnimation} loop={false} autoplay style={{ width: '100%', height: '100%' }} />
          </div>
          <div className="absolute top-1/4 right-1/4 w-48 h-48 translate-x-1/2 -translate-y-1/2">
            <LottiePlayer animationData={confettiAnimation} loop={false} autoplay style={{ width: '100%', height: '100%' }} />
          </div>
          <div className="absolute top-1/3 left-1/2 w-56 h-56 -translate-x-1/2 -translate-y-1/2">
            <LottiePlayer animationData={confettiAnimation} loop={false} autoplay style={{ width: '100%', height: '100%' }} />
          </div>
        </div>
      </Suspense>
      {/* Content */}
      <div className="text-center animate-bounce-in relative z-10">
        <div className="text-6xl mb-4">{milestone.emoji}</div>
        <h2 className="text-3xl font-bold text-white mb-2">{milestone.message}</h2>
        <p className="text-slate-300">
          {t('goal.congratulations', { amount: formatAmount(milestone.amount) })}
        </p>
      </div>
    </div>
  )
}

// ─── Generate Milestones from Smart Goals ─────────────────────────────────────

interface SmartGoalInput {
  periodType: string
  goalAmount: number
  isCustom: boolean
  weeklyBreakdown?: Record<number, number>  // week 1-5 -> cumulative goal amount
  weeklyGoalAmount?: number  // average weekly goal for simple calculation
}

function generateMilestonesFromGoals(input: SmartGoalInput, t: TFunc): Milestone[] {
  const { periodType, goalAmount, isCustom, weeklyBreakdown, weeklyGoalAmount } = input

  // Generate message based on amount
  const formatGoalMessage = (amount: number, type: string, weekNum?: number): string => {
    const formattedAmount = amount >= 1000000
      ? `${(amount / 1000000).toFixed(amount % 1000000 === 0 ? 0 : 1)}M`
      : `${(amount / 1000).toFixed(0)}K`

    if (weekNum) {
      return t('goal.weekLabel', { num: weekNum, amount: formattedAmount })
    }

    const key = type === 'daily' ? 'goal.dailyRevenue' : type === 'weekly' ? 'goal.weeklyRevenue' : 'goal.monthlyRevenue'
    return t(key, { amount: formattedAmount })
  }

  // Select emoji based on period type and position
  const getEmoji = (type: string, weekNum?: number): string => {
    if (weekNum) {
      const weekEmojis = ['🔥', '⚡', '💪', '🚀']
      return weekEmojis[weekNum - 1] || '🔥'
    }
    switch (type) {
      case 'daily': return '🎉'
      case 'weekly': return '💰🎊'
      case 'monthly': return '👑🎇🎆'
      default: return '🎉'
    }
  }

  // For monthly, use weekly breakdown from smart goals if available
  if (periodType === 'monthly') {
    const milestones: Milestone[] = []

    // Use weekly breakdown if provided (from smart goals API)
    if (weeklyBreakdown && Object.keys(weeklyBreakdown).length > 0) {
      // Build cumulative milestones from weekly breakdown
      // weeklyBreakdown contains week -> cumulative target
      const weeks = Object.keys(weeklyBreakdown).map(Number).sort((a, b) => a - b)
      let cumulative = 0

      for (const week of weeks) {
        cumulative += weeklyBreakdown[week]
        // Only add if it's less than the monthly goal
        if (cumulative < goalAmount * 0.95) {  // Allow 5% margin
          milestones.push({
            amount: Math.round(cumulative / 10000) * 10000,  // Round to nearest 10K
            message: formatGoalMessage(cumulative, 'weekly', week),
            emoji: getEmoji('weekly', week),
            isCustom: false,
          })
        }
      }
    } else if (weeklyGoalAmount && weeklyGoalAmount > 0) {
      // Fallback: use simple weekly multiplication
      for (let week = 1; week <= 4; week++) {
        const weekAmount = weeklyGoalAmount * week
        if (weekAmount < goalAmount) {
          milestones.push({
            amount: weekAmount,
            message: formatGoalMessage(weekAmount, 'weekly', week),
            emoji: getEmoji('weekly', week),
            isCustom: false,
          })
        }
      }
    }

    // Add final monthly goal
    milestones.push({
      amount: goalAmount,
      message: formatGoalMessage(goalAmount, periodType),
      emoji: getEmoji(periodType),
      isCustom,
    })

    return milestones
  }

  // For weekly, add an intermediate milestone at 80%
  if (periodType === 'weekly' && goalAmount >= 500000) {
    const intermediateMilestone = Math.round(goalAmount * 0.8 / 50000) * 50000
    return [
      {
        amount: intermediateMilestone,
        message: formatGoalMessage(intermediateMilestone, periodType),
        emoji: '🔥',
        isCustom: false,
      },
      {
        amount: goalAmount,
        message: formatGoalMessage(goalAmount, periodType),
        emoji: getEmoji(periodType),
        isCustom,
      },
    ]
  }

  // Single milestone for daily
  return [{
    amount: goalAmount,
    message: formatGoalMessage(goalAmount, periodType),
    emoji: getEmoji(periodType),
    isCustom,
  }]
}

// ─── Info Button (click-based, matching CustomerInsights style) ───────────────

function InfoButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className="text-slate-400 hover:text-slate-600 transition-colors"
      aria-label="Goal calculation details"
    >
      <CircleHelp className="w-4 h-4" />
    </button>
  )
}

function InfoTooltipContent({ onClose, title, children }: {
  onClose: () => void
  title: string
  children: React.ReactNode
}) {
  return (
    <div className="absolute top-8 left-0 z-50 bg-slate-800 border border-slate-700 rounded-lg shadow-xl p-4 min-w-[220px] max-w-[300px]">
      <button
        onClick={onClose}
        className="absolute top-2 right-2 text-slate-400 hover:text-slate-200 text-lg leading-none"
        aria-label="Close"
      >
        ×
      </button>
      <h4 className="text-sm font-semibold text-slate-200 mb-2">{title}</h4>
      {children}
    </div>
  )
}

// ─── Main Component ───────────────────────────────────────────────────────────

export const MilestoneProgress = memo(function MilestoneProgress({
  revenue,
  className = ''
}: MilestoneProgressProps) {
  const { t } = useTranslation()
  const { period } = useFilterStore()
  const containerRef = useRef<HTMLDivElement>(null)
  const trackRef = useRef<HTMLDivElement>(null)
  const fillRef = useRef<HTMLDivElement>(null)
  const [celebration, setCelebration] = useState<Milestone | null>(null)
  const [showGoalInfo, setShowGoalInfo] = useState(false)
  const celebratedRef = useRef<Set<number>>(new Set())
  const [sparkles, setSparkles] = useState<Sparkle[]>([])
  const isInitialMountRef = useRef(true)
  const prevRevenueRef = useRef<number>(0)
  const [isVisible, setIsVisible] = useState(false)
  // Track pending timeouts for cleanup on unmount
  const pendingTimeoutsRef = useRef<Set<ReturnType<typeof setTimeout>>>(new Set())

  // Cleanup pending timeouts on unmount
  useEffect(() => {
    const timeouts = pendingTimeoutsRef.current
    return () => {
      timeouts.forEach(clearTimeout)
      timeouts.clear()
    }
  }, [])

  // Intersection Observer to detect visibility
  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const observer = new IntersectionObserver(
      ([entry]) => setIsVisible(entry.isIntersecting),
      { threshold: 0.1 }
    )
    observer.observe(container)
    return () => observer.disconnect()
  }, [])

  // Fetch smart goals from API (with seasonality and weekly breakdown)
  const { data: goalsData, isLoading: isLoadingGoals } = useSmartGoals()

  // Fetch revenue trend data to calculate current month revenue for last_28_days
  const { data: trendData } = useRevenueTrend()

  // Calculate current month revenue from trend data when viewing last_28_days
  const currentMonthRevenue = useMemo(() => {
    // Only needed for last_28_days period
    if (period !== 'last_28_days' || !trendData?.labels?.length) {
      return revenue
    }

    // Get current month (1-indexed to match date format "dd.mm")
    const currentMonth = new Date().getMonth() + 1

    // Sum revenue only for current month days
    let monthRevenue = 0
    trendData.labels.forEach((label: string, index: number) => {
      // Parse month from "dd.mm" format (e.g., "21.01" for January 21st)
      const parts = label.split('.')
      if (parts.length >= 2) {
        const labelMonth = parseInt(parts[1], 10)
        if (labelMonth === currentMonth) {
          monthRevenue += trendData.revenue?.[index] ?? 0
        }
      }
    })

    return monthRevenue > 0 ? monthRevenue : revenue
  }, [period, trendData, revenue])

  // Calculate the excluded previous month revenue for display
  const excludedPrevMonthRevenue = useMemo(() => {
    if (period !== 'last_28_days') return 0
    return revenue - currentMonthRevenue
  }, [period, revenue, currentMonthRevenue])

  // Use current month revenue for the effective revenue in calculations
  const effectiveRevenue = currentMonthRevenue

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

  // Regenerate sparkles periodically for continuous effect (only when visible)
  useEffect(() => {
    if (!isVisible) return
    regenerateSparkles()
    const interval = setInterval(regenerateSparkles, 3000)
    return () => clearInterval(interval)
  }, [regenerateSparkles, revenue, isVisible])

  // Get period type
  const periodType = getPeriodType(period)

  // Generate milestones from smart goals data or use fallback
  const milestones = useMemo(() => {
    if (!periodType) return null

    // If smart goals loaded, use them
    if (goalsData) {
      const goalData = periodType === 'daily' ? goalsData.daily
        : periodType === 'weekly' ? goalsData.weekly
        : periodType === 'monthly' ? goalsData.monthly
        : null

      if (goalData && goalData.amount > 0) {
        // For monthly view, get weekly breakdown from smart goals
        const weeklyBreakdown = periodType === 'monthly'
          ? goalsData.weekly?.weeklyBreakdown
          : undefined
        const weeklyGoalAmount = periodType === 'monthly'
          ? goalsData.weekly?.amount
          : undefined

        return generateMilestonesFromGoals({
          periodType,
          goalAmount: goalData.amount,
          isCustom: goalData.isCustom,
          weeklyBreakdown,
          weeklyGoalAmount,
        }, t)
      }
    }

    // Fallback to hardcoded values while loading or on error
    return getFallbackMilestones(t)[periodType] || null
  }, [periodType, goalsData, t])

  // Calculate progress metrics
  const metrics = useMemo(() => {
    if (!milestones || milestones.length === 0) return null

    const maxMilestone = milestones[milestones.length - 1].amount
    const progress = Math.min((effectiveRevenue / maxMilestone) * 100, 100)
    const allCompleted = effectiveRevenue >= maxMilestone

    // Find current milestone index (highest reached)
    let currentIndex = -1
    for (let i = milestones.length - 1; i >= 0; i--) {
      if (effectiveRevenue >= milestones[i].amount) {
        currentIndex = i
        break
      }
    }

    // Check if near a milestone (within 10%)
    let nearMilestone = false
    for (const m of milestones) {
      const percentTo = (effectiveRevenue / m.amount) * 100
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
  }, [milestones, effectiveRevenue])

  // Trigger particles and celebrations (only when revenue increases, not on initial load)
  useEffect(() => {
    if (!milestones || !trackRef.current || !metrics) return

    const prevRevenue = prevRevenueRef.current
    const isInitialMount = isInitialMountRef.current

    // On initial mount, just mark already-reached milestones as celebrated (no animation)
    if (isInitialMount) {
      milestones.forEach((m) => {
        if (effectiveRevenue >= m.amount) {
          celebratedRef.current.add(m.amount)
        }
      })
      isInitialMountRef.current = false
      prevRevenueRef.current = effectiveRevenue
      return
    }

    // Only celebrate if revenue actually increased
    if (effectiveRevenue <= prevRevenue) {
      prevRevenueRef.current = effectiveRevenue
      return
    }

    milestones.forEach((m, index) => {
      // Only celebrate milestones that were just crossed (not previously reached)
      const wasReached = prevRevenue >= m.amount
      const isNowReached = effectiveRevenue >= m.amount

      if (isNowReached && !wasReached && !celebratedRef.current.has(m.amount)) {
        celebratedRef.current.add(m.amount)

        // Trigger particles (track timeout for cleanup)
        const trackRect = trackRef.current!.getBoundingClientRect()
        const position = (m.amount / metrics.maxMilestone) * 100
        const xPos = (position / 100) * trackRect.width

        const particleTimeout = setTimeout(() => {
          pendingTimeoutsRef.current.delete(particleTimeout)
          if (trackRef.current) {
            burstParticles(trackRef.current, xPos, 25)
          }
        }, index * 300)
        pendingTimeoutsRef.current.add(particleTimeout)

        // Show celebration for highest new milestone (track timeout for cleanup)
        if (index === milestones.length - 1 || effectiveRevenue < milestones[index + 1]?.amount) {
          const celebrationTimeout = setTimeout(() => {
            pendingTimeoutsRef.current.delete(celebrationTimeout)
            setCelebration(m)
          }, 500)
          pendingTimeoutsRef.current.add(celebrationTimeout)
        }
      }
    })

    prevRevenueRef.current = effectiveRevenue
  }, [milestones, effectiveRevenue, metrics])

  // Reset celebrations when period changes
  useEffect(() => {
    celebratedRef.current.clear()
  }, [period])

  // Show skeleton while loading goals
  if (isLoadingGoals && periodType) {
    return (
      <div className={`bg-white rounded-xl border border-slate-200/60 shadow-[var(--shadow-card)] p-5 ${className}`}>
        <div className="animate-pulse">
          <div className="flex items-center justify-between mb-4">
            <div className="h-5 bg-slate-200 rounded w-24" />
            <div className="h-4 bg-slate-200 rounded w-32" />
          </div>
          <div className="h-3 bg-slate-200 rounded-full w-full mb-3" />
          <div className="flex justify-between">
            <div className="h-3 bg-slate-200 rounded w-16" />
            <div className="h-3 bg-slate-200 rounded w-16" />
          </div>
        </div>
      </div>
    )
  }

  // Don't render for custom periods or if no milestones
  if (!periodType || !milestones || milestones.length === 0 || !metrics) {
    return null
  }

  const periodLabel = periodType === 'daily' ? t('goal.dailyGoal')
    : periodType === 'weekly' ? t('goal.weeklyGoal')
    : t('goal.monthlyGoal')

  // Get goal details from smart goals data
  const currentGoalData = periodType === 'daily' ? goalsData?.daily
    : periodType === 'weekly' ? goalsData?.weekly
    : periodType === 'monthly' ? goalsData?.monthly
    : null
  const isCustomGoal = currentGoalData?.isCustom ?? false

  // Get monthly-specific data for tooltip
  const monthlyGoalData = goalsData?.monthly
  const growthRate = monthlyGoalData?.growthRate
  const lastYearRevenue = monthlyGoalData?.lastYearRevenue
  const recent3MonthAvg = monthlyGoalData?.recent3MonthAvg
  const calculationMethod = monthlyGoalData?.calculationMethod
  const seasonalityIndex = monthlyGoalData?.seasonalityIndex

  // Format calculation method for display
  const getMethodLabel = (method?: string): string => {
    switch (method) {
      case 'yoy_growth': return t('goal.methodYoy')
      case 'recent_trend': return t('goal.methodTrend')
      case 'historical_avg': return t('goal.methodHistorical')
      case 'fallback': return t('goal.methodDefault')
      default: return t('goal.methodAuto')
    }
  }

  return (
    <>
      <div ref={containerRef} className={`bg-white rounded-xl border border-slate-200/60 shadow-[var(--shadow-card)] hover:shadow-[var(--shadow-card-hover)] transition-all duration-300 p-5 ${className}`}>
        {/* Header */}
        <div className="flex justify-between items-center mb-4">
          <div className="relative flex items-center gap-1.5">
            <div className={`w-2 h-2 rounded-full ${metrics.themeColor}`} />
            <span className="text-base font-semibold text-slate-800 tracking-tight">{periodLabel}</span>
            {/* Custom goal indicator */}
            {isCustomGoal && (
              <span className="text-[10px] bg-blue-100 text-blue-600 px-1.5 py-0.5 rounded font-medium">
                {t('goal.custom')}
              </span>
            )}
            {/* Info button for auto-calculated goals (click-based) */}
            {!isLoadingGoals && (excludedPrevMonthRevenue > 0 || (!isCustomGoal && (growthRate || recent3MonthAvg || lastYearRevenue))) && (
              <InfoButton onClick={() => setShowGoalInfo(!showGoalInfo)} />
            )}
            {/* Click-based tooltip */}
            {showGoalInfo && (
              <InfoTooltipContent onClose={() => setShowGoalInfo(false)} title={t('goal.calcTitle')}>
                <div className="space-y-2">
                  {growthRate !== undefined && growthRate > 0 && (
                    <p className="text-xs text-slate-300">
                      <strong className="text-emerald-400">{t('goal.yoyGrowth')}</strong> +{(growthRate * 100).toFixed(0)}%
                    </p>
                  )}
                  {recent3MonthAvg && recent3MonthAvg > 0 && (
                    <p className="text-xs text-slate-300">
                      <strong className="text-blue-400">{t('goal.recent3mo')}</strong> {formatAmount(recent3MonthAvg)}
                    </p>
                  )}
                  {lastYearRevenue && lastYearRevenue > 0 && (
                    <p className="text-xs text-slate-300">
                      <strong className="text-purple-400">{t('goal.lastYearMonth')}</strong> {formatAmount(lastYearRevenue)}
                    </p>
                  )}
                  {seasonalityIndex && (
                    <p className="text-xs text-slate-300">
                      <strong className="text-orange-400">{t('goal.seasonality')}</strong> {seasonalityIndex.toFixed(2)}x
                    </p>
                  )}
                  {calculationMethod && (
                    <p className="text-xs text-slate-400 pt-1 mt-1 border-t border-slate-600">
                      {t('goal.method')} {getMethodLabel(calculationMethod)}
                    </p>
                  )}
                  {excludedPrevMonthRevenue > 0 && (
                    <p className="text-xs text-slate-300 pt-1 mt-1 border-t border-slate-600">
                      <strong className="text-amber-400">{formatCurrency(excludedPrevMonthRevenue)}</strong> {t('goal.prevMonthExcluded')}
                    </p>
                  )}
                </div>
              </InfoTooltipContent>
            )}
            {/* Loading indicator */}
            {isLoadingGoals && (
              <span className="text-[10px] bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded animate-pulse">
                {t('goal.loading')}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <span className={`text-lg font-bold ${metrics.textColor}`}>
              {Math.round(metrics.progress)}%
            </span>
            {metrics.nearMilestone && (
              <span className="text-xs bg-amber-100 text-amber-700 px-2 py-0.5 rounded-full font-medium animate-pulse">
                {t('goal.almostThere')}
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
            {/* SVG shine gradient */}
            <svg className="absolute inset-0 w-full h-full rounded-full pointer-events-none" preserveAspectRatio="none">
              <defs>
                <linearGradient id="shineGradMP" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="white" stopOpacity="0.3" />
                  <stop offset="50%" stopColor="white" stopOpacity="0" />
                </linearGradient>
              </defs>
              <rect width="100%" height="100%" fill="url(#shineGradMP)" />
            </svg>

            {/* Color-shifting gradient overlay */}
            {metrics.progress > 5 && <SvgColorShiftGradient uniqueId="mp" themeColor={metrics.themeColor} />}

            {/* SVG animated shimmer sweep */}
            <SvgShimmer uniqueId="mp" />

            {/* Traveling light orbs */}
            {metrics.progress > 10 && <SvgTravelingOrbs uniqueId="mp" />}

            {/* SVG sparkle stars */}
            {metrics.progress > 5 && sparkles.map(sparkle => (
              <SvgSparkle key={sparkle.id} sparkle={sparkle} />
            ))}

            {/* SVG floating bubbles */}
            {metrics.progress > 10 && <SvgFloatingBubbles />}

            {/* Liquid wave on top edge */}
            {metrics.progress > 3 && <SvgLiquidWave themeColor={metrics.themeColor} />}
          </div>

          {/* SVG leading edge glow */}
          <SvgLeadingEdge progress={metrics.progress} themeColor={metrics.themeColor} />

          {/* Milestone Markers — SVG animated */}
          {milestones.map((m, index) => {
            const position = (m.amount / metrics.maxMilestone) * 100
            const isReached = effectiveRevenue >= m.amount
            const isNext = index === metrics.currentIndex + 1
            const isFinal = index === milestones.length - 1

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
                <div
                  className={`
                    w-7 h-7 transition-all duration-300 cursor-pointer
                    ${isReached ? 'scale-110' : isNext ? '' : 'opacity-60 scale-90'}
                    hover:scale-150 hover:opacity-100 hover:z-30
                  `}
                  title={`${formatAmount(m.amount)} - ${m.message}`}
                  role="button"
                  tabIndex={0}
                  aria-label={`Milestone: ${formatAmount(m.amount)}`}
                >
                  <SvgMilestoneMarker isReached={isReached} isNext={isNext} isFinal={isFinal} />
                </div>
              </div>
            )
          })}
        </div>

        {/* Labels */}
        <div className="flex justify-between mt-3">
          <div>
            <span className={`text-base font-bold ${metrics.textColor}`}>
              {formatCurrency(effectiveRevenue)}
            </span>
            <span className="text-xs text-slate-400 ml-1">{t('goal.current')}</span>
            {/* Show note about excluded previous month revenue */}
            {excludedPrevMonthRevenue > 0 && (
              <span className="text-xs text-slate-400 ml-1">
                {t('goal.thisMonthOnly')}
              </span>
            )}
          </div>
          <div className="text-right">
            <span className="text-base font-semibold text-slate-600">
              {formatAmount(metrics.maxMilestone)}
            </span>
            <span className="text-xs text-slate-400 ml-1">{t('goal.goalLabel')}</span>
          </div>
        </div>

        {/* Remaining amount */}
        {!metrics.allCompleted && (
          <div className="mt-3 pt-3 border-t border-slate-100">
            <p className="text-xs text-slate-500 text-center">
              <span className="font-medium text-slate-700">{formatCurrency(metrics.maxMilestone - effectiveRevenue)}</span> {t('goal.remaining')}
            </p>
          </div>
        )}

        {/* Completed state */}
        {metrics.allCompleted && (
          <div className="mt-3 pt-3 border-t border-slate-100">
            <p className="text-xs text-center font-medium text-purple-600 flex items-center justify-center gap-1">
              <CheckCircle className="w-4 h-4" />
              {t('goal.allAchieved')}
            </p>
          </div>
        )}

        {/* Last year comparison (for monthly goals) */}
        {periodType === 'monthly' && lastYearRevenue && lastYearRevenue > 0 && !isCustomGoal && (
          <div className="mt-3 pt-3 border-t border-slate-100">
            <div className="flex justify-between items-center text-xs">
              <span className="text-slate-500">{t('goal.vsLastYear')}</span>
              <span className="font-medium text-slate-700">
                {formatAmount(lastYearRevenue)}
                {effectiveRevenue > lastYearRevenue && (
                  <span className="text-emerald-600 ml-1">
                    (+{(((effectiveRevenue - lastYearRevenue) / lastYearRevenue) * 100).toFixed(0)}%)
                  </span>
                )}
                {effectiveRevenue < lastYearRevenue && effectiveRevenue > 0 && (
                  <span className="text-amber-600 ml-1">
                    ({(((effectiveRevenue - lastYearRevenue) / lastYearRevenue) * 100).toFixed(0)}%)
                  </span>
                )}
              </span>
            </div>
          </div>
        )}
      </div>

      {/* Celebration Overlay */}
      {celebration && (
        <CelebrationOverlay
          milestone={celebration}
          onClose={() => setCelebration(null)}
          t={t}
        />
      )}

      {/* Animation Styles (only CSS that can't be done in SVG) */}
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
        .progress-track {
          background:
            linear-gradient(90deg, rgba(0,0,0,0.02) 0%, transparent 50%, rgba(0,0,0,0.02) 100%),
            #f1f5f9;
        }
      `}</style>
    </>
  )
})
