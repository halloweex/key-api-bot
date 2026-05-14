import { lazy, Suspense } from 'react'

// ─── LottieAnimation ─────────────────────────────────────────────────────────
//
// Renders a Lottie JSON animation at one of three preset sizes. Sizing is the
// only "visual" knob exposed; layout (margins, alignment) is the parent's job.

const LottiePlayer = lazy(() => import('lottie-react'))

type LottieSize = 'sm' | 'md' | 'lg'

interface LottieAnimationProps {
  animationData: object
  loop?: boolean
  autoplay?: boolean
  size?: LottieSize
}

const sizeClass: Record<LottieSize, string> = {
  sm: 'w-16 h-16',
  md: 'w-20 h-20',
  lg: 'w-24 h-24',
}

export function LottieAnimation({
  animationData,
  loop = true,
  autoplay = true,
  size = 'md',
}: LottieAnimationProps) {
  const dimensions = sizeClass[size]
  return (
    <Suspense
      fallback={
        <div className={`flex items-center justify-center ${dimensions}`}>
          <div className="w-8 h-8 border-2 border-slate-200 border-t-slate-400 rounded-full animate-spin" />
        </div>
      }
    >
      <LottiePlayer
        animationData={animationData}
        loop={loop}
        autoplay={autoplay}
        className={dimensions}
      />
    </Suspense>
  )
}
