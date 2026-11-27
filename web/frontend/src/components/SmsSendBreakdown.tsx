import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { formatNumber } from '../utils/formatters'
import type { SmsSendProgress } from '../types/api'

// ─── SmsSendBreakdown ────────────────────────────────────────────────────────
//
// Where a campaign's send stands, by what the gateway said about each
// recipient: one bar split by outcome, and a line per outcome with its count.
//
// A single percentage was not enough the day a send broke halfway
// (2026-11-26). How many got the message, how many never left and how many
// may have it are three different answers, and the last one decides whether
// sending again is safe. The bar keeps them apart. The uncertain ones say in
// words that they are never sent again.
//
// While a send runs it also says which batch is going and about how long is
// left: the gateway takes about 4.5 s per batch of 1 000 (measured 2026-11-26).

const SECONDS_PER_BATCH = 5
const DEFAULT_BATCH_SIZE = 1000

type Part = 'accepted' | 'refused' | 'uncertain' | 'notSent' | 'pending'

const PARTS: Part[] = ['accepted', 'refused', 'uncertain', 'notSent', 'pending']

const COLOUR: Record<Part, string> = {
  accepted: 'bg-emerald-500',
  refused: 'bg-rose-500',
  uncertain: 'bg-amber-400',
  notSent: 'bg-slate-400',
  pending: 'bg-slate-200',
}

interface SmsSendBreakdownProps {
  /** The roster's counts; absent until the first read comes back. */
  progress?: SmsSendProgress
  /** The roster size to show before then. */
  total: number
  /** A send is under way: say which batch, and about how long is left. */
  running: boolean
}

export const SmsSendBreakdown = memo(function SmsSendBreakdown({
  progress, total: fallbackTotal, running,
}: SmsSendBreakdownProps) {
  const { t } = useTranslation()
  const total = progress?.total ?? fallbackTotal
  const counts: Record<Part, number> = {
    accepted: progress?.accepted ?? 0,
    refused: (progress?.failed ?? 0) + (progress?.stoplisted ?? 0),
    uncertain: progress?.uncertain ?? 0,
    notSent: progress?.notSent ?? 0,
    pending: progress ? progress.pending : total,
  }
  const done = Math.max(0, total - counts.pending)
  const batchSize = progress?.batchSize ?? DEFAULT_BATCH_SIZE
  const batches = Math.max(1, Math.ceil(total / batchSize))
  const batchesDone = Math.min(batches, Math.floor(done / batchSize))
  const share = (n: number) => (total > 0 ? (n / total) * 100 : 0)

  return (
    <div
      role={running ? 'status' : undefined}
      aria-live={running ? 'polite' : undefined}
      className="space-y-2"
    >
      <p className="text-xs font-medium text-slate-700 tabular-nums">
        {running
          ? t('sms.breakdown.running', {
              done: formatNumber(done),
              total: formatNumber(total),
              batch: Math.min(batches, batchesDone + 1),
              batches,
              eta: (batches - batchesDone) * SECONDS_PER_BATCH,
            })
          : t('sms.breakdown.done', { done: formatNumber(done), total: formatNumber(total) })}
      </p>

      {/* Not-yet-answered is the track itself, so the bar fills as it goes. */}
      <div className="flex h-2.5 w-full overflow-hidden rounded-full bg-slate-100" aria-hidden="true">
        {PARTS.filter((p) => p !== 'pending' && counts[p] > 0).map((p) => (
          <div
            key={p}
            data-part={p}
            className={`h-full ${COLOUR[p]} transition-[width] duration-500`}
            style={{ width: `${share(counts[p])}%` }}
          />
        ))}
      </div>

      <ul className="grid gap-x-4 gap-y-1 text-xs text-slate-600 sm:grid-cols-2">
        {PARTS.filter((p) => p === 'accepted' || counts[p] > 0).map((p) => (
          <li key={p} data-part={p} className="flex items-start gap-1.5">
            <span className={`mt-1 h-2 w-2 shrink-0 rounded-full ${COLOUR[p]}`} aria-hidden="true" />
            <span>
              <span className="font-medium text-slate-800 tabular-nums">
                {formatNumber(counts[p])}
              </span>{' '}
              {t(`sms.breakdown.${p}`)}
            </span>
          </li>
        ))}
      </ul>
    </div>
  )
})
