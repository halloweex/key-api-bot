import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { formatNumber } from '../utils/formatters'
import type { SmsComparison } from '../types/api'

// ─── SmsLiftForest ───────────────────────────────────────────────────────────
//
// Every arm of the campaign on one axis, so they can be compared.
//
// Each tier used to get its own interval on its own scale, with zero pinned to
// the middle. That made a decisive result and a shrug look identical, and it
// hid the thing a reader most wants: which tier moved, and by how much
// relative to the others.
//
// One shared scale fixes both. The interval is the mark and zero is the line
// through all of them, so "clears zero" is something you see rather than
// something you work out. Intervals touching zero are drawn grey — a win
// cannot be read off the chart by accident.
//
// Colour is state, not identity: rows are named on the left and their lift is
// printed on the right, so the chart still reads with the colour removed.
// Grey at 2.5:1 against white sits below the contrast floor, and those direct
// labels are what discharges it.

export interface ForestRow {
  label: string
  comparison: SmsComparison
  /** Recipients behind the estimate — a wide interval usually means few. */
  contacts: number
  emphasis?: boolean
}

/** A domain that always contains zero, padded so ends are not flush. */
function domainOf(rows: ForestRow[]): [number, number] {
  let lo = 0
  let hi = 0
  for (const r of rows) {
    lo = Math.min(lo, r.comparison.ci95Pp[0])
    hi = Math.max(hi, r.comparison.ci95Pp[1])
  }
  const pad = Math.max((hi - lo) * 0.12, 0.4)
  return [lo - pad, hi + pad]
}

/** Round tick steps: 1, 2, 5 × a power of ten. */
function ticksFor([lo, hi]: [number, number]): number[] {
  const raw = (hi - lo) / 4
  const mag = 10 ** Math.floor(Math.log10(raw))
  const step = [1, 2, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? mag * 10
  const out: number[] = []
  for (let v = Math.ceil(lo / step) * step; v <= hi; v += step) {
    out.push(Math.abs(v) < step / 1000 ? 0 : v)
  }
  return out
}

export const SmsLiftForest = memo(function SmsLiftForest({ rows }: { rows: ForestRow[] }) {
  const { t } = useTranslation()
  const [hovered, setHovered] = useState<number | null>(null)

  const domain = useMemo(() => domainOf(rows), [rows])
  const ticks = useMemo(() => ticksFor(domain), [domain])
  const pct = (v: number) => ((v - domain[0]) / (domain[1] - domain[0])) * 100

  if (rows.length === 0) return null

  return (
    <figure className="m-0">
      <figcaption className="sr-only">{t('sms.forestTitle')}</figcaption>

      {/* ── State key: colour carries meaning, so it is named ─────────── */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-slate-500 mb-2">
        <span className="flex items-center gap-1.5">
          <span className="w-2.5 h-2.5 rounded-sm bg-emerald-600" aria-hidden="true" />
          {t('sms.forestKeyProven')}
        </span>
        <span className="flex items-center gap-1.5">
          <span className="w-2.5 h-2.5 rounded-sm bg-slate-400" aria-hidden="true" />
          {t('sms.forestKeyUnproven')}
        </span>
      </div>

      <div className="relative">
        {rows.map((row, i) => {
          const c = row.comparison
          const [lo, hi] = c.ci95Pp
          const proven = c.significant
          const left = pct(lo)
          const width = Math.max(pct(hi) - left, 0.6)
          const bar = proven ? 'bg-emerald-600/30' : 'bg-slate-400/30'
          const ink = proven ? 'bg-emerald-600' : 'bg-slate-400'

          return (
            <div
              key={row.label}
              className={`grid grid-cols-[minmax(5.5rem,auto)_1fr_4.5rem] items-center gap-2
                          rounded-md px-1 -mx-1 ${
                hovered === i ? 'bg-slate-50' : ''
              } ${row.emphasis ? 'font-medium' : ''}`}
              onMouseEnter={() => setHovered(i)}
              onMouseLeave={() => setHovered(null)}
            >
              <span className="text-[11px] text-slate-600 truncate" title={row.label}>
                {row.label}
              </span>

              <div className="relative h-9">
                {/* Zero, the only reference that matters here */}
                <div
                  className="absolute inset-y-0 w-px bg-slate-300"
                  style={{ left: `${pct(0)}%` }}
                  aria-hidden="true"
                />
                {/* 95% interval */}
                <div
                  className={`absolute top-1/2 -translate-y-1/2 h-1.5 rounded-full ${bar}`}
                  style={{ left: `${left}%`, width: `${width}%` }}
                />
                {/* Interval ends */}
                {[left, pct(hi)].map((x, k) => (
                  <div
                    key={k}
                    className={`absolute top-1/2 -translate-y-1/2 h-3.5 w-0.5 ${ink}`}
                    style={{ left: `${x}%` }}
                  />
                ))}
                {/* Point estimate — ringed so it stays legible over the bar */}
                <div
                  className={`absolute top-1/2 -translate-y-1/2 -ml-[5px] h-2.5 w-2.5
                              rounded-full ring-2 ring-white ${ink}`}
                  style={{ left: `${pct(c.liftPp)}%` }}
                />
              </div>

              {/* Direct label — the relief for grey's low contrast */}
              <span className="text-[11px] text-right tabular-nums text-slate-700">
                {c.liftPp > 0 ? '+' : ''}{c.liftPp.toFixed(1)}
              </span>
            </div>
          )
        })}

        {/* ── Axis ────────────────────────────────────────────────────── */}
        <div className="grid grid-cols-[minmax(5.5rem,auto)_1fr_4.5rem] gap-2">
          <span />
          <div className="relative h-5 border-t border-slate-200">
            {ticks.map((v) => (
              <span
                key={v}
                className={`absolute top-1 -translate-x-1/2 text-[10px] tabular-nums ${
                  v === 0 ? 'text-slate-500' : 'text-slate-400'
                }`}
                style={{ left: `${pct(v)}%` }}
              >
                {v > 0 ? `+${v}` : v}
              </span>
            ))}
          </div>
          <span className="text-[10px] text-slate-400 text-right pt-1">
            {t('sms.pp')}
          </span>
        </div>
      </div>

      {/* ── Hover detail, below the plot so nothing is occluded ───────── */}
      <div className="mt-2 min-h-[2.5rem] text-[11px] text-slate-600 leading-snug">
        {hovered !== null ? (
          <p>
            <span className="font-medium text-slate-700">{rows[hovered].label}</span>
            {' — '}
            {t('sms.forestDetail', {
              target: rows[hovered].comparison.conversionTarget.toFixed(2),
              control: rows[hovered].comparison.conversionHoldout.toFixed(2),
              lo: rows[hovered].comparison.ci95Pp[0].toFixed(1),
              hi: rows[hovered].comparison.ci95Pp[1].toFixed(1),
              p: rows[hovered].comparison.pValue < 0.001
                ? '<0.001'
                : rows[hovered].comparison.pValue.toFixed(3),
              contacts: formatNumber(rows[hovered].contacts),
            })}
          </p>
        ) : (
          <p className="text-slate-400">{t('sms.forestHint')}</p>
        )}
      </div>
    </figure>
  )
})
