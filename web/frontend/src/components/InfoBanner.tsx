import { memo, type ReactNode } from 'react'
import { Wrapper } from './Wrapper'

// ─── InfoBanner ──────────────────────────────────────────────────────────────
//
// Subtle blue informational panel used to explain a chart, table, or
// page-level concept. Two compositions:
//
//   <InfoBanner>{paragraph}</InfoBanner>
//   <InfoBanner icon={icon} title="...">{rich content}</InfoBanner>
//
// Frame (background, border, padding) owned here. Layout/margin against
// siblings handled by the caller via <Wrapper>.

interface InfoBannerProps {
  children: ReactNode
  /** Optional leading icon (rendered on the left at the same baseline as title/body). */
  icon?: ReactNode
  /** Optional bold title rendered above the body. */
  title?: string
}

export const InfoBanner = memo(function InfoBanner({ children, icon, title }: InfoBannerProps) {
  const body = title ? (
    <div className="text-sm text-blue-800">
      <p className="font-medium">{title}</p>
      <div className="mt-1 text-blue-700">{children}</div>
    </div>
  ) : (
    <p className="text-sm text-blue-800">{children}</p>
  )

  return (
    <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg" role="note">
      {icon ? (
        <Wrapper dir="row" gap="md" align="start">
          <span className="text-blue-500 flex-shrink-0 mt-0.5">{icon}</span>
          {body}
        </Wrapper>
      ) : (
        body
      )}
    </div>
  )
})
