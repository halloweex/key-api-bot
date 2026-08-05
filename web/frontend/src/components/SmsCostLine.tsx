import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { formatNumber } from '../utils/formatters'
import type { SmsCost } from '../utils/smsCost'

// ─── SmsCostLine ─────────────────────────────────────────────────────────────
//
// What the text costs, under the box it is typed into.
//
// A plain character counter hides the only number that matters here: a single
// Cyrillic character switches the whole message to UCS-2, which drops the
// segment from 160 characters to 70. Crossing that boundary silently doubles
// the bill for every recipient, so the part count is stated outright and the
// campaign multiple is shown when there is a roster to multiply by.

export const SmsCostLine = memo(function SmsCostLine({
  cost,
  limit,
  recipients,
}: {
  cost: SmsCost
  limit: number
  /** Target-arm size, when this text is about to go to a campaign. */
  recipients?: number
}) {
  const { t } = useTranslation()
  const over = cost.characters > limit

  return (
    <span className="flex flex-wrap items-baseline gap-x-2 text-[11px] tabular-nums">
      <span className={over ? 'text-red-600' : 'text-slate-400'}>
        {cost.characters} / {limit}
      </span>
      <span className="text-slate-500">
        {t('sms.costParts', {
          parts: cost.parts,
          encoding: t(`sms.encoding.${cost.encoding}`),
        })}
      </span>
      {recipients != null && cost.parts > 0 && (
        <span className={cost.parts > 1 ? 'text-amber-700' : 'text-slate-400'}>
          {t('sms.costTotal', {
            total: formatNumber(cost.parts * recipients),
          })}
        </span>
      )}
    </span>
  )
})
