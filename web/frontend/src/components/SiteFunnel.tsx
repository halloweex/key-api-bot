import { Fragment, memo, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { ApiErrorState } from './ApiErrorState'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { DataTable, Td, Th, Tr } from './DataTable'
import { InfoPopover } from './InfoPopover'
import { SegmentedControl } from './SegmentedControl'
import { SkeletonChart } from './Skeleton'
import { Wrapper } from './Wrapper'
import { ApiError } from '../api/client'
import { useSiteOrders, useTrafficFunnel } from '../hooks/useApi'
import { formatNumber } from '../utils/formatters'
import {
  FOLDED_DEVICES, MIN_GROUP_PEOPLE, TRANSITIONS,
  biggestLeak, canCompare, conclude, conversion, conversionChange, enoughToClaim, foldDevices,
  funnelSteps, groupFigures, hasPeople, leak, perBase, rowsGap, stepShare,
  type Against, type GroupFigure, type StepFigure, type Transition,
} from '../utils/funnel'
import {
  convPct, fetchedAt, gapText, groupPct, perBaseText, sayConclusion, sayLeak, stepPct, toneOf,
  transitionText, wasText, type Line, type Tone,
} from '../utils/funnelText'
import { plural, type T } from '../utils/plural'
import { addDays, formatDay, formatDayRange, formatSpan, inclusiveDays, kyivToday, rangeEnds } from '../utils/smsDates'
import type { FunnelRow, TrafficFunnelResponse } from '../types/api'

// ─── The site's purchase funnel, from Google Analytics ───────────────────────
//
// What the owner asked for on 2026-11-04, in his words: "Вошли → товар →
// корзина → оформление → покупка, Итог". So the block reads top to bottom as
// that funnel: the conversion and what it says first, then the five steps as
// bars from one left edge with what each step lost written between them, then
// who gets how far. The arithmetic is `utils/funnel`, the sentences
// `utils/funnelText`.

type Breakdown = 'type' | 'channel' | 'device'

const TONE_TEXT: Record<Tone, string> = {
  good: 'text-emerald-700',
  bad: 'text-red-700',
  neutral: 'text-slate-600',
}

const VERDICT_BOX: Record<Tone, string> = {
  good: 'border-emerald-200 bg-emerald-50',
  bad: 'border-red-200 bg-red-50',
  neutral: 'border-slate-200 bg-slate-50',
}

const VERDICT_HEAD: Record<Tone, string> = {
  good: 'text-emerald-900',
  bad: 'text-red-900',
  neutral: 'text-slate-900',
}

/** GA4's device categories with a label; any other is shown as GA4 names it. */
const DEVICE_LABELS: Record<string, string> = {
  mobile: 'mobile', desktop: 'desktop', tablet: 'tablet', 'smart tv': 'smartTv',
}

/** ▲ or ▼ before a coloured verdict, so the colour never says it alone. */
function Arrow({ tone }: { tone: Tone }) {
  if (tone === 'neutral') return null
  return <span aria-hidden="true">{tone === 'good' ? '▲ ' : '▼ '}</span>
}

function groupName(row: FunnelRow, by: Breakdown, t: T): string {
  if (by === 'type') return t(`traffic.funnelType.${row.key}`)
  if (by === 'device') {
    if (row.key === FOLDED_DEVICES) return t('traffic.funnelDeviceFolded')
    const label = DEVICE_LABELS[row.key]
    return label ? t(`traffic.funnelDevice.${label}`) : row.key
  }
  return row.key
}

function groupNote(row: FunnelRow, by: Breakdown, t: T): string | null {
  if (by === 'type') return t(`traffic.funnelTypeDesc.${row.key}`)
  if (by === 'channel' && row.type) return t(`traffic.funnelType.${row.type}`)
  return null
}

// ─── Frame and messages ──────────────────────────────────────────────────────

function FunnelFrame({ aside, children }: { aside?: ReactNode; children: ReactNode }) {
  const { t } = useTranslation()
  return (
    <Card animate="chart-in">
      <CardHeader>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="flex min-w-0 max-w-xl flex-col gap-1">
            <div className="flex items-center gap-2">
              <CardTitle>{t('traffic.funnelTitle')}</CardTitle>
              <InfoPopover title={t('traffic.funnelHowTitle')} size="wide">
                <p className="text-xs leading-relaxed text-slate-300">{t('traffic.funnelHowText')}</p>
              </InfoPopover>
            </div>
            <p className="text-[13px] leading-relaxed text-slate-600">{t('traffic.funnelDesc')}</p>
          </div>
          {aside}
        </div>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  )
}

function Message({ title, text }: { title?: string; text: string }) {
  return (
    <div className="flex flex-col gap-1.5 rounded-xl bg-slate-50 p-4">
      {title && <p className="text-[15px] font-bold text-slate-900">{title}</p>}
      <p className="text-[13px] leading-relaxed text-slate-600">{text}</p>
    </div>
  )
}

function Note({ warn, children }: { warn?: boolean; children: ReactNode }) {
  return (
    <p className={`rounded-xl border px-3.5 py-3 text-[13px] leading-relaxed ${
      warn ? 'border-amber-200 bg-amber-50 text-amber-900' : 'border-slate-200 bg-slate-50 text-slate-700'
    }`}>
      {children}
    </p>
  )
}

// ─── What the period is set beside ───────────────────────────────────────────

const CompareWith = memo(function CompareWith({
  data, against, prevOk, yearOk, onChange,
}: {
  data: TrafficFunnelResponse
  against: Against | null
  prevOk: boolean
  yearOk: boolean
  onChange: (value: Against) => void
}) {
  const { t } = useTranslation()
  const since = formatDay(data.earliest, { month: 'long' })
  if (!against) {
    return <p className="max-w-xs text-xs leading-relaxed text-slate-600">{t('traffic.funnel.compare.none', { date: since })}</p>
  }
  const window = data.periods[against]
  const ends = rangeEnds(window.start, window.end)
  const days = inclusiveDays(window.start, window.end)
  const dates = against === 'previous'
    ? t('traffic.funnel.compare.datesPrevious', { ...ends, n: formatNumber(days), word: plural(t, 'word.days', days) })
    : t('traffic.funnel.compare.datesYear', ends)
  const options = [
    { value: 'previous' as const, label: t('traffic.funnel.compare.previous'), disabled: !prevOk },
    { value: 'year_ago' as const, label: t('traffic.funnel.compare.year_ago'), disabled: !yearOk },
  ]
  return (
    <div className="flex w-full flex-col gap-1.5 sm:w-auto sm:items-end">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-[13px] text-slate-600">{t('traffic.funnel.compare.label')}</span>
        <SegmentedControl<Against>
          options={options} value={against} onChange={onChange} size="sm"
          ariaLabel={t('traffic.funnel.compare.label')}
        />
      </div>
      <span className="text-xs text-slate-600">{dates}</span>
      {!yearOk && <span className="max-w-sm text-xs leading-relaxed text-slate-600 sm:text-right">{t('traffic.funnel.compare.noYear', { date: since })}</span>}
      {!prevOk && <span className="max-w-sm text-xs leading-relaxed text-slate-600 sm:text-right">{t('traffic.funnel.compare.noPrevious', { date: since })}</span>}
    </div>
  )
})

// ─── The number and what it says ─────────────────────────────────────────────

const Hero = memo(function Hero({ rate, per, was }: { rate: string; per: string | null; was: Line | null }) {
  const { t } = useTranslation()
  return (
    <div className="flex flex-[1_1_18rem] flex-col gap-1.5 rounded-xl bg-blue-50 px-4 py-4 sm:px-6 sm:py-5">
      <span className="text-[13px] font-semibold text-blue-800">{t('traffic.funnel.hero.label')}</span>
      <span className="text-[44px] font-bold leading-none tabular-nums text-slate-900 sm:text-[52px]">{rate}</span>
      {per && <span className="text-sm text-slate-900 sm:text-[15px]">{per}</span>}
      {was && (
        <span className={`text-[13px] sm:text-sm ${was.tone === 'neutral' ? 'text-slate-600' : `font-semibold ${TONE_TEXT[was.tone]}`}`}>
          <Arrow tone={was.tone} />{was.text}
        </span>
      )}
    </div>
  )
})

const Verdict = memo(function Verdict({ headline, lines }: { headline: Line; lines: Line[] }) {
  return (
    <div className={`flex flex-[2_1_28rem] flex-col gap-2 rounded-xl border px-4 py-3 sm:px-6 sm:py-5 ${VERDICT_BOX[headline.tone]}`}>
      <p className={`text-[15px] font-bold sm:text-base ${VERDICT_HEAD[headline.tone]}`}>
        <Arrow tone={headline.tone} />{headline.text}
      </p>
      {lines.map((line, i) => (
        <p key={i} className="text-[13px] leading-relaxed text-slate-700 sm:text-sm">
          {line.lead && (
            <><b className={TONE_TEXT[line.tone]}><Arrow tone={line.tone} />{line.lead}</b>{' '}</>
          )}
          {line.text}
        </p>
      ))}
    </div>
  )
})

// ─── The five steps ──────────────────────────────────────────────────────────

/** A step's people as a bar from the common left edge; the people at the step
 *  before as its pale tail, two pixels apart. Decorative: every number it
 *  draws is written beside it. */
function Bar({ reached, before, thin }: { reached: number; before: number | null; thin?: boolean }) {
  const r = Math.max(0, Math.min(1, reached)) * 100
  const b = before == null ? null : Math.max(0, Math.min(1, before)) * 100
  return (
    <div className={`relative ${thin ? 'h-[18px]' : 'h-7'}`} aria-hidden="true">
      {b != null && b > r && (
        <div
          className="absolute inset-y-0 rounded-r bg-slate-300"
          style={{ left: `calc(${r}% + 2px)`, width: `max(0px, calc(${b - r}% - 2px))` }}
        />
      )}
      <div className="absolute inset-y-0 left-0 rounded-r bg-blue-600" style={{ width: `max(3px, ${r}%)` }} />
    </div>
  )
}

/** The words between two bars: how many went on, how many left, and against
 *  the window compared with. */
function Between({ s, isLeak, short }: { s: StepFigure; isLeak: boolean; short: boolean }) {
  const { t } = useTranslation()
  const was = s.was?.ofBefore
  const judged = (s.change === 'better' || s.change === 'worse') && was != null
  return (
    <p className={`py-1 text-[13px] leading-relaxed ${isLeak ? 'text-slate-900' : 'text-slate-600'}`}>
      <span aria-hidden="true">↓ </span>
      {transitionText(s.step as Transition, t, short)}{' '}
      <b className="text-slate-900">{stepPct(s.ofBefore)}</b>
      {' · '}{t('traffic.funnel.bars.left', { n: formatNumber(s.left ?? 0) })}
      {isLeak && <>{' — '}<b>{t(short ? 'traffic.funnel.bars.leakShort' : 'traffic.funnel.bars.leak')}</b></>}
      {judged && (
        <>
          {' · '}
          <b className={TONE_TEXT[toneOf(s.change)]}>
            <Arrow tone={toneOf(s.change)} />{t(`traffic.funnel.bars.${s.change}`, { was: stepPct(was) })}
          </b>
        </>
      )}
      {!judged && !short && was != null && <>{' · '}{t('traffic.funnel.bars.was', { was: stepPct(was) })}</>}
    </p>
  )
}

const FunnelBars = memo(function FunnelBars({
  steps, leakStep, total, totalShort, totalWas,
}: {
  steps: StepFigure[]
  leakStep: Transition | null
  total: string
  totalShort: string
  totalWas: Line | null
}) {
  const { t } = useTranslation()
  const last = steps.length - 1
  const sub = (s: StepFigure, i: number) => {
    const was = s.was ? t('traffic.funnel.bars.was', { was: formatNumber(s.was.people) }) : null
    if (i === 0) return was ?? ''
    const share = t('traffic.funnel.bars.ofEntered', { share: i === last ? convPct(s.ofEntered) : stepPct(s.ofEntered) })
    return was ? `${share} · ${was}` : share
  }
  const wasClass = totalWas && totalWas.tone !== 'neutral' ? `font-semibold ${TONE_TEXT[totalWas.tone]}` : 'text-slate-600'

  return (
    <Wrapper dir="column" gap="xs">
      <div className="hidden flex-wrap gap-4 pb-1.5 text-xs text-slate-600 sm:flex">
        <span className="flex items-center gap-1.5">
          <span className="h-2.5 w-3.5 rounded-sm bg-blue-600" aria-hidden="true" />{t('traffic.funnel.bars.reached')}
        </span>
        <span className="flex items-center gap-1.5">
          <span className="h-2.5 w-3.5 rounded-sm bg-slate-300" aria-hidden="true" />{t('traffic.funnel.bars.tail')}
        </span>
      </div>

      {/* Wide: name | bar | figures, one row per step and the words between. */}
      <div className="hidden grid-cols-[11rem_minmax(0,1fr)_13rem] items-center gap-x-4 gap-y-1 sm:grid">
        {steps.map((s, i) => (
          <Fragment key={s.step}>
            {i > 0 && (
              <>
                <span />
                <Between s={s} isLeak={s.step === leakStep} short={false} />
                <span />
              </>
            )}
            <span className="text-sm font-semibold text-slate-900">{t(`traffic.funnelStep.${s.step}`)}</span>
            <Bar reached={s.ofEntered ?? 0} before={i > 0 ? steps[i - 1].ofEntered : null} />
            <span className="flex flex-col">
              <span className="text-lg font-bold tabular-nums text-slate-900">{formatNumber(s.people)}</span>
              <span className="text-xs text-slate-600">{sub(s, i)}</span>
            </span>
          </Fragment>
        ))}
        <span className="mt-2 border-t border-slate-200 pt-2.5 text-[15px] font-bold text-slate-900">
          {t('traffic.funnel.total.label')}
        </span>
        <span className="mt-2 border-t border-slate-200 pt-2.5 text-[15px] text-slate-900">{total}</span>
        <span className={`mt-2 border-t border-slate-200 pt-2.5 text-[13px] ${wasClass}`}>
          {totalWas && <><Arrow tone={totalWas.tone} />{totalWas.text}</>}
        </span>
      </div>

      {/* Narrow: the same steps stacked, the bar under its name. */}
      <ol className="flex flex-col sm:hidden">
        {steps.map((s, i) => (
          <li key={s.step} className="flex flex-col">
            {i > 0 && <Between s={s} isLeak={s.step === leakStep} short />}
            <div className="flex flex-col gap-1.5 py-2">
              <div className="flex items-baseline justify-between gap-2">
                <span className="text-sm font-semibold text-slate-900">{t(`traffic.funnelStep.${s.step}`)}</span>
                <span className="text-lg font-bold tabular-nums text-slate-900">{formatNumber(s.people)}</span>
              </div>
              <Bar reached={s.ofEntered ?? 0} before={i > 0 ? steps[i - 1].ofEntered : null} thin />
              <span className="text-xs text-slate-600">{sub(s, i)}</span>
            </div>
          </li>
        ))}
      </ol>
      <div className="flex items-baseline justify-between gap-2 border-t border-slate-200 pt-2.5 sm:hidden">
        <span className="text-[15px] font-bold text-slate-900">{t('traffic.funnel.total.label')}</span>
        <span className="text-[15px] text-slate-900">{totalShort}</span>
      </div>
      {totalWas && (
        <p className={`text-xs sm:hidden ${wasClass}`}><Arrow tone={totalWas.tone} />{totalWas.text}</p>
      )}
      <p className="text-xs text-slate-600 sm:hidden">{t('traffic.funnel.bars.legendSentence')}</p>
    </Wrapper>
  )
})

// ─── Who gets how far ────────────────────────────────────────────────────────

function HeadCell({ name, sub }: { name: string; sub: string }) {
  return (
    <span className="flex flex-col">
      <span>{name}</span>
      <span className="text-xs font-normal text-slate-500">{sub}</span>
    </span>
  )
}

function WasCell({ was, change }: { was: number | null; change: GroupFigure['change'] }) {
  const { t } = useTranslation()
  if (was == null) return <span className="text-slate-500">—</span>
  const tone = toneOf(change)
  return (
    <span className="flex flex-col items-end">
      <span className={tone === 'neutral' ? 'text-slate-600' : ''}>{convPct(was)}</span>
      {tone !== 'neutral' && (
        <span className={`text-xs font-semibold ${TONE_TEXT[tone]}`}>
          <Arrow tone={tone} />{t(`traffic.funnel.table.${change}`)}
        </span>
      )}
    </span>
  )
}

function CardWas({ was, change }: { was: number; change: GroupFigure['change'] }) {
  const { t } = useTranslation()
  const tone = toneOf(change)
  return (
    <span className="text-right">
      {t('traffic.funnel.bars.was', { was: convPct(was) })}
      {tone !== 'neutral' && (
        <b className={`ml-1 ${TONE_TEXT[tone]}`}><Arrow tone={tone} />{t(`traffic.funnel.table.${change}`)}</b>
      )}
    </span>
  )
}

const FunnelBreakdown = memo(function FunnelBreakdown({
  data, against, judgeNow, judgeThen,
}: {
  data: TrafficFunnelResponse
  against: Against | null
  judgeNow: boolean
  judgeThen: boolean
}) {
  const { t } = useTranslation()
  const [by, setBy] = useState<Breakdown>('type')
  const site = data.total.current
  const siteWas = against ? data.total[against] : null
  const raw = by === 'type' ? data.by_type : by === 'channel' ? data.by_channel : data.by_device
  const { rows, folded } = by === 'device' ? foldDevices(raw, site) : { rows: raw, folded: [] as string[] }
  const groups = groupFigures(rows, site, against, judgeNow, judgeThen)
  const gapLine = gapText(rowsGap(rows, site), by, t)
  const siteChange = judgeThen && siteWas ? conversionChange(site, siteWas).change : null
  const siteConversion = conversion(site)
  const siteWasConversion = siteWas ? conversion(siteWas) : null
  const options = (['type', 'channel', 'device'] as const).map((value) => ({
    value, label: t(`traffic.funnel.table.by.${value}`),
  }))
  const names = folded.map((key) => {
    const label = DEVICE_LABELS[key]
    return (label ? t(`traffic.funnelDevice.${label}`) : key).toLocaleLowerCase()
  }).join(', ')

  return (
    <Wrapper dir="column" gap="md">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h4 className="text-[15px] font-bold text-slate-900">{t('traffic.funnel.table.title')}</h4>
        <div className="hidden flex-wrap items-center gap-2 sm:flex">
          <span className="text-[13px] text-slate-600">{t('traffic.funnel.table.byLabel')}</span>
          <SegmentedControl<Breakdown> options={options} value={by} onChange={setBy} size="sm"
            ariaLabel={t('traffic.funnel.table.byLabel')} />
        </div>
        <div className="w-full sm:hidden">
          <SegmentedControl<Breakdown> options={options} value={by} onChange={setBy} size="sm" fullWidth
            ariaLabel={t('traffic.funnel.table.byLabel')} />
        </div>
      </div>

      <div className="hidden sm:block">
        <DataTable variant="feature">
          <thead>
            <Tr header>
              <Th>{t('traffic.funnel.table.who')}</Th>
              <Th align="right"><HeadCell name={t('traffic.funnel.table.entered')} sub={t('traffic.funnel.table.enteredSub')} /></Th>
              {TRANSITIONS.map((step) => (
                <Th key={step} align="right">
                  <HeadCell name={t(`traffic.funnel.table.step.${step}`)} sub={t(`traffic.funnel.table.stepSub.${step}`)} />
                </Th>
              ))}
              <Th align="right"><HeadCell name={t('traffic.funnel.table.total')} sub={t('traffic.funnel.table.totalSub')} /></Th>
              {against && (
                <Th align="right">
                  <HeadCell name={t('traffic.funnel.table.before')}
                    sub={formatDayRange(data.periods[against].start, data.periods[against].end)} />
                </Th>
              )}
            </Tr>
          </thead>
          <tbody>
            <Tr emphasis hover={false}>
              <Td bold>{t('traffic.funnel.table.site')}</Td>
              <Td align="right" tabular>
                <span className="flex flex-col items-end">
                  <span className="font-semibold">{formatNumber(site.entered)}</span>
                  <span className="text-xs text-slate-500">{groupPct(1)}</span>
                </span>
              </Td>
              {TRANSITIONS.map((step) => (
                <Td key={step} align="right" tabular>{stepPct(stepShare(site, step))}</Td>
              ))}
              <Td align="right" tabular bold>{convPct(siteConversion)}</Td>
              {against && <Td align="right" tabular><WasCell was={siteWasConversion} change={siteChange} /></Td>}
            </Tr>
            {groups.map((g) => {
              const note = groupNote(g.row, by, t)
              return (
                <Tr key={g.row.key} faded={g.small}>
                  <Td bold>
                    <span className="flex flex-col">
                      <span>{groupName(g.row, by, t)}</span>
                      {note && <span className="text-xs font-normal text-slate-500">{note}</span>}
                    </span>
                  </Td>
                  <Td align="right" tabular>
                    <span className="flex flex-col items-end">
                      <span>{formatNumber(g.row.current.entered)}</span>
                      <span className="text-xs text-slate-500">{groupPct(g.shareOfSite)}</span>
                    </span>
                  </Td>
                  {TRANSITIONS.map((step) => (
                    <Td key={step} align="right" tabular>
                      {g.steps[step].lower
                        ? <span className="font-semibold text-red-700"><Arrow tone="bad" />{stepPct(g.steps[step].share)}</span>
                        : stepPct(g.steps[step].share)}
                    </Td>
                  ))}
                  <Td align="right" tabular bold>{convPct(g.conversion)}</Td>
                  {against && <Td align="right" tabular><WasCell was={g.was} change={g.change} /></Td>}
                </Tr>
              )
            })}
          </tbody>
        </DataTable>
      </div>

      <ul className="flex flex-col gap-2 sm:hidden">
        <li className="flex flex-col gap-1 rounded-xl bg-blue-50 p-3">
          <div className="flex justify-between gap-2">
            <span className="text-sm font-bold text-slate-900">{t('traffic.funnel.table.site')}</span>
            <span className="text-[15px] font-bold tabular-nums text-slate-900">{convPct(siteConversion)}</span>
          </div>
          <div className="flex justify-between gap-2 text-xs text-slate-600">
            <span className="tabular-nums">
              {t('traffic.funnel.table.cardSite', { entered: formatNumber(site.entered), buyers: formatNumber(site.purchased) })}
            </span>
            {siteWasConversion != null && <CardWas was={siteWasConversion} change={siteChange} />}
          </div>
        </li>
        {groups.map((g) => {
          const note = groupNote(g.row, by, t)
          const lower = TRANSITIONS.filter((step) => g.steps[step].lower)
          return (
            <li key={g.row.key} className={`flex flex-col gap-1 rounded-xl border border-slate-200 p-3 ${g.small ? 'opacity-60' : ''}`}>
              <div className="flex justify-between gap-2">
                <span className="text-sm font-semibold text-slate-900">{groupName(g.row, by, t)} · {groupPct(g.shareOfSite)}</span>
                <span className="text-[15px] font-bold tabular-nums text-slate-900">{convPct(g.conversion)}</span>
              </div>
              {note && <span className="text-xs text-slate-500">{note}</span>}
              <div className="flex justify-between gap-2 text-xs text-slate-600">
                <span className="tabular-nums">{formatNumber(g.row.current.entered)} → {formatNumber(g.row.current.purchased)}</span>
                {g.was != null && <CardWas was={g.was} change={g.change} />}
              </div>
              {lower.map((step) => (
                <span key={step} className="text-xs font-semibold text-red-700">
                  <Arrow tone="bad" />{t(`traffic.funnel.table.lowerStep.${step}`, { value: stepPct(g.steps[step].share) })}
                </span>
              ))}
            </li>
          )
        })}
      </ul>

      <div className="flex flex-col gap-1 text-xs leading-relaxed text-slate-600">
        {/* A phone's cards say each ▼ in words; there are no cells to explain. */}
        {judgeNow && <p className="hidden sm:block">{t('traffic.funnel.table.lowerNote')}</p>}
        {gapLine && <p>{gapLine}</p>}
        {groups.some((g) => g.small) && <p>{t('traffic.funnel.table.smallNote', { n: formatNumber(MIN_GROUP_PEOPLE) })}</p>}
        {folded.length > 0 && <p>{t('traffic.funnel.table.foldNote', { names })}</p>}
      </div>
    </Wrapper>
  )
})

// ─── The block ───────────────────────────────────────────────────────────────

const FunnelBody = memo(function FunnelBody({ data }: { data: TrafficFunnelResponse }) {
  const { t } = useTranslation()
  const [picked, setPicked] = useState<Against>('previous')
  const period = data.periods.current
  const now = data.total.current
  // The CRM's orders over the days GA4 counted: a period reaching before
  // `earliest` (the whole history does) has funnels for its tail alone.
  const orders = useSiteOrders(period.start < data.earliest ? data.earliest : period.start, period.end)
  const today = kyivToday()
  const since = formatDay(data.earliest, { month: 'long' })

  if (period.start > period.end) return <FunnelFrame><Message text={t('traffic.funnel.state.backwards')} /></FunnelFrame>
  if (period.end < data.earliest) return <FunnelFrame><Message text={t('traffic.funnel.state.tooOld', { date: since })} /></FunnelFrame>
  if (!hasPeople(now)) return <FunnelFrame><Message text={t('traffic.funnel.state.empty')} /></FunnelFrame>

  const prevOk = canCompare(data, 'previous')
  const yearOk = canCompare(data, 'year_ago')
  const against: Against | null = picked === 'year_ago'
    ? (yearOk ? 'year_ago' : prevOk ? 'previous' : null)
    : (prevOk ? 'previous' : yearOk ? 'year_ago' : null)
  const was = against ? data.total[against] : null
  const judgeNow = enoughToClaim(now)
  const judgeThen = judgeNow && !!was && enoughToClaim(was)

  const steps = funnelSteps(now, was, judgeThen)
  const said = judgeThen && was && against
    ? sayConclusion(conclude(now, was, data.by_type, against), t)
    : null
  const leakOnly = !said && judgeNow ? sayLeak(leak(now, null), t) : null
  const convChange = judgeThen && was ? conversionChange(now, was).change : null
  const convWas = was ? conversion(was) : null
  const rate = conversion(now)
  const { base, count } = perBase(rate ?? 0)

  const notes: ReactNode[] = []
  if (data.stale) {
    notes.push(<Note key="stale" warn>{t('traffic.funnel.state.stale', { time: fetchedAt(data.fetched_at, today) })}</Note>)
  }
  // GA4 is still counting yesterday and today: their figures can grow.
  const unsettledFrom = period.start > addDays(today, -1) ? period.start : addDays(today, -1)
  const unsettledTo = period.end < today ? period.end : today
  if (unsettledFrom <= unsettledTo) {
    notes.push(
      <Note key="unsettled">
        {t('traffic.funnel.state.incomplete', {
          days: formatSpan(unsettledFrom, unsettledTo), lastWeek: t('filter.lastWeek'), lastMonth: t('filter.lastMonth'),
        })}
      </Note>,
    )
  }
  if (period.start < data.earliest) {
    notes.push(<Note key="partial">{t('traffic.funnel.state.partial', { date: since })}</Note>)
  }
  if (!judgeNow) {
    notes.push(<Note key="few">{t('traffic.funnel.state.few', { n: formatNumber(now.entered), word: plural(t, 'word.people', now.entered) })}</Note>)
  } else if (was && !judgeThen) {
    notes.push(<Note key="fewThen">{t('traffic.funnel.state.fewThen', { n: formatNumber(was.entered), word: plural(t, 'word.people', was.entered) })}</Note>)
  }

  const crmOrders = orders.data ? ` ${t('traffic.funnel.crm.orders', { n: formatNumber(orders.data.totalOrders) })}` : ''

  return (
    <FunnelFrame aside={<CompareWith data={data} against={against} prevOk={prevOk} yearOk={yearOk} onChange={setPicked} />}>
      <Wrapper dir="column" gap="xl">
        {notes.length > 0 && <Wrapper dir="column" gap="sm">{notes}</Wrapper>}
        <Wrapper dir="row" wrap gap="lg">
          <Hero rate={convPct(rate)} per={perBaseText(now, t)} was={wasText(convWas, convChange, t, 'hero')} />
          {said && <Verdict headline={said.headline} lines={said.lines} />}
          {leakOnly && <Verdict headline={leakOnly} lines={[]} />}
        </Wrapper>
        <Wrapper dir="column" gap="sm">
          <FunnelBars
            steps={steps}
            leakStep={biggestLeak(now)}
            total={t('traffic.funnel.total.value', { rate: convPct(rate), n: formatNumber(count), base: formatNumber(base) })}
            totalShort={t('traffic.funnel.total.short', { rate: convPct(rate) })}
            totalWas={wasText(convWas, convChange, t, 'total')}
          />
          <p className="text-[13px] leading-relaxed text-slate-600">
            {t('traffic.funnel.crm.note', { n: formatNumber(now.purchased), word: plural(t, 'word.buyers', now.purchased) })}
            {crmOrders}
          </p>
        </Wrapper>
        <FunnelBreakdown data={data} against={against} judgeNow={judgeNow} judgeThen={judgeThen} />
      </Wrapper>
    </FunnelFrame>
  )
})

export const SiteFunnel = memo(function SiteFunnel() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useTrafficFunnel()

  if (isLoading) return <SkeletonChart />
  if (error instanceof ApiError && error.status === 503) {
    const notConfigured = error.message === 'GA4 is not configured'
    return (
      <FunnelFrame>
        <Message
          title={t(notConfigured ? 'traffic.funnel.state.notConfiguredTitle' : 'traffic.funnel.state.unavailableTitle')}
          text={t(notConfigured ? 'traffic.funnel.state.notConfigured' : 'traffic.funnel.state.unavailable')}
        />
      </FunnelFrame>
    )
  }
  if (error) {
    return <FunnelFrame><ApiErrorState error={error as Error} onRetry={() => void refetch()} /></FunnelFrame>
  }
  if (!data) return null
  return <FunnelBody data={data} />
})

export default SiteFunnel
