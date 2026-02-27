import { lazy, Suspense } from 'react'

const LottiePlayer = lazy(() => import('lottie-react'))

interface LottieAnimationProps {
  animationData: object
  loop?: boolean
  autoplay?: boolean
  className?: string
}

export function LottieAnimation({
  animationData,
  loop = true,
  autoplay = true,
  className = '',
}: LottieAnimationProps) {
  return (
    <Suspense
      fallback={
        <div className={`flex items-center justify-center ${className}`}>
          <div className="w-8 h-8 border-2 border-slate-200 border-t-slate-400 rounded-full animate-spin" />
        </div>
      }
    >
      <LottiePlayer
        animationData={animationData}
        loop={loop}
        autoplay={autoplay}
        className={className}
      />
    </Suspense>
  )
}
