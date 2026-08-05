import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronDown, ChevronUp } from 'lucide-react'
import { formatCurrency, formatNumber } from '../utils/formatters'
import type { SmsFunnelStep, SmsSegmentsResponse } from '../types/api'

// ─── SmsSelectionCriteria ────────────────────────────────────────────────────
//
// Why the list is the size it is.
//
// The tier totals on their own read as arbitrary: three numbers with no
// account of the base they came from. Most of the story is in what was
// removed — a customer who bought 200 days ago, a phone number that no gateway
// will accept, someone who opted out — and none of that is visible in a total.
// So the funnel is shown as remaining-after-each-rule, in the order the query
// applies them, with the rule stated in words next to its own number.
//
// Collapsed by default because it is an explanation, not a control; the
// headline (base → sendable) stays visible so the drop is never hidden.

function Rule({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-1">
      <dt className="text-slate-600">{label}</dt>
      <dd className="text-slate-800 font-medium tabular-nums text-right">{value}</dd>
    </div>
  )
}

function FunnelRow({ step, previous }: { step: SmsFunnelStep; previous: number | null }) {
  const { t } = useTranslation()
  const dropped = previous === null ? 0 : previous - step.remaining

  return (
    <tr className="border-t border-slate-100">
      <td className="py-1.5 pr-3 text-slate-600">{t(`sms.funnel.${step.stage}`)}</td>
      <td className="py-1.5 px-3 text-right tabular-nums text-slate-800 font-medium">
        {formatNumber(step.remaining)}
      </td>
      <td className="py-1.5 pl-3 text-right tabular-nums text-slate-400 w-16">
        {dropped > 0 ? `−${formatNumber(dropped)}` : ''}
      </td>
    </tr>
  )
}

export const SmsSelectionCriteria = memo(function SmsSelectionCriteria({
  data,
}: {
  data: SmsSegmentsResponse
}) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)

  const { criteria, funnel } = data
  const base = funnel[0]?.remaining ?? 0
  const sendable = funnel[funnel.length - 1]?.remaining ?? 0
  const basis = criteria.ltvBasis === 'margin' ? t('sms.basisMargin') : t('sms.basisRevenue')

  return (
    <div className="mt-4 pt-4 border-t border-slate-100">
      <button
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        className="w-full flex items-center justify-between gap-3 text-left group"
      >
        <span className="text-xs text-slate-600">
          <span className="font-medium text-slate-700">{t('sms.criteriaTitle')}</span>
          {' — '}
          <span className="tabular-nums">
            {t('sms.criteriaSummary', {
              base: formatNumber(base),
              sendable: formatNumber(sendable),
            })}
          </span>
        </span>
        <span className="text-slate-400 group-hover:text-slate-600 transition-colors flex-shrink-0">
          {open ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
        </span>
      </button>

      {open && (
        <div className="mt-3 grid gap-4 lg:grid-cols-2">
          {/* ── The rules, with the numbers actually in force ───────── */}
          <div className="text-xs">
            <h4 className="font-medium text-slate-700 mb-1.5">{t('sms.criteriaRulesTitle')}</h4>
            <dl className="divide-y divide-slate-100">
              <Rule label={t('sms.ruleBasis')} value={basis} />
              <Rule
                label={t('sms.ruleVip')}
                value={t('sms.ruleLtvAtLeast', { value: formatCurrency(criteria.vipLtv) })}
              />
              <Rule
                label={t('sms.ruleCore')}
                value={t('sms.ruleCoreValue', {
                  orders: criteria.coreMinOrders,
                  value: formatCurrency(criteria.coreLtv),
                })}
              />
              <Rule
                label={t('sms.ruleReactivation')}
                value={t('sms.ruleReactivationValue', {
                  days: criteria.reactivationMaxRecency,
                })}
              />
              <Rule
                label={t('sms.ruleWindow')}
                value={t('sms.ruleWindowValue', { days: criteria.maxRecencyDays })}
              />
              <Rule
                label={t('sms.ruleHoldout')}
                value={t('sms.ruleHoldoutValue', { pct: criteria.holdoutPct })}
              />
            </dl>
            <p className="mt-2 text-[11px] text-slate-500 leading-snug">
              {t('sms.ruleOrderNote')}
            </p>
            <p className="mt-1.5 text-[11px] text-slate-500 leading-snug">
              {t('sms.ruleHoldoutNote')}
            </p>
          </div>

          {/* ── The same rules as counts ────────────────────────────── */}
          <div className="text-xs">
            <h4 className="font-medium text-slate-700 mb-1.5">{t('sms.criteriaFunnelTitle')}</h4>
            <table className="w-full">
              <tbody>
                {funnel.map((step, i) => (
                  <FunnelRow
                    key={step.stage}
                    step={step}
                    previous={i === 0 ? null : funnel[i - 1].remaining}
                  />
                ))}
              </tbody>
            </table>
            <p className="mt-2 text-[11px] text-slate-500 leading-snug">
              {t('sms.funnelNote')}
            </p>
          </div>
        </div>
      )}
    </div>
  )
})
