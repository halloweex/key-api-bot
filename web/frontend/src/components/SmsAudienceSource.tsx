import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { SegmentedControl } from './SegmentedControl'
import { useIsPhone } from '../hooks/useMediaQuery'

// ─── SmsAudienceSource ───────────────────────────────────────────────────────
//
// Where a campaign's people come from: a new audience, or the people a sent
// campaign already reached.
//
// The second is its own choice, at the top of the first step, rather than a
// filter among the others — because it is not a narrowing of anything. A
// follow-up has no window, no tiers and no control of its own, and the wizard
// skips the steps that would ask for them.

export const SmsAudienceSource = memo(function SmsAudienceSource({
  followUp,
  onChange,
}: {
  followUp: boolean
  onChange: (followUp: boolean) => void
}) {
  const { t } = useTranslation()
  const phone = useIsPhone()

  return (
    <SegmentedControl
      ariaLabel={t('sms.sourceLabel')}
      size="sm"
      fullWidth={phone}
      value={followUp ? 'followUp' : 'audience'}
      onChange={(v) => onChange(v === 'followUp')}
      options={[
        { value: 'audience', label: t('sms.sourceAudience') },
        { value: 'followUp', label: t('sms.sourceFollowUp') },
      ]}
    />
  )
})
