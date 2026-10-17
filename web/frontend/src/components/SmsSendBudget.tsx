import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { formatCurrency, formatNumber } from '../utils/formatters'
import { balanceIsShort, smsSendRequirement, type SmsBalanceReading } from '../utils/smsCost'

// ─── SmsSendBudget ───────────────────────────────────────────────────────────
//
// Who this goes to, what it costs, and whether the balance covers it — beside
// the field where the send is confirmed, because that is the last moment the
// three can be read together.
//
// It exists because the gateway's balance was nowhere on the page. TurboSMS
// bills a batch it then fails to dispatch, so a campaign larger than the
// balance did not stop cleanly: it took the account to −675 ₴. The server now
// refuses such a send outright; this says so before the press instead of
// after it.
//
// The balance is what the gateway said when the dialog opened. The send reads
// it again, and that reading is the one that decides.

interface SmsSendBudgetProps {
  recipients: number
  /** Billable parts of the SMS text; 0 before anything is typed. */
  parts: number
  /** The configured tariff, in ₴ per part; null before it has loaded. */
  pricePerPart: number | null
  balance: SmsBalanceReading
}

export const SmsSendBudget = memo(function SmsSendBudget({
  recipients, parts, pricePerPart, balance,
}: SmsSendBudgetProps) {
  const { t } = useTranslation()
  // Before a text exists the floor is one part, and the line says "from".
  const billed = Math.max(parts, 1)
  const need = pricePerPart == null
    ? null
    : smsSendRequirement(recipients, billed, pricePerPart)
  const short = balanceIsShort(balance, recipients, parts, pricePerPart)

  return (
    <div
      className={`rounded-md border px-3 py-2 text-xs leading-snug tabular-nums ${
        short ? 'border-red-200 bg-red-50' : 'border-slate-200 bg-slate-50'
      }`}
      aria-live="polite"
    >
      <p className="text-slate-700">
        {need == null
          ? t('sms.budgetRecipients', { recipients: formatNumber(recipients) })
          : t(parts > 0 ? 'sms.budgetCost' : 'sms.budgetCostFrom', {
              recipients: formatNumber(recipients),
              parts: billed,
              price: pricePerPart,
              cost: formatCurrency(need.cost),
            })}
      </p>

      <p className={`mt-1 ${short ? 'text-red-700 font-medium' : 'text-slate-600'}`}>
        {balance.state === 'loading' && t('sms.budgetLoading')}
        {balance.state === 'unknown' && t('sms.budgetUnknown')}
        {balance.state === 'known' && (
          <>
            {t('sms.budgetBalance', { balance: formatCurrency(balance.amount) })}
            {need != null && (
              <>
                {' · '}
                {short
                  ? t('sms.budgetShort', {
                      // Up, never down: whole hryvnias on screen must not
                      // read "₴0 short" for a send the server will refuse.
                      shortfall: formatCurrency(Math.ceil(need.required - balance.amount)),
                      required: formatCurrency(need.required),
                    })
                  : t('sms.budgetCovered', { required: formatCurrency(need.required) })}
              </>
            )}
          </>
        )}
      </p>
    </div>
  )
})
