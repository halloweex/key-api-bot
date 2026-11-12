import { Fragment, memo, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { ArrowLeftRight, CalendarX, Clock, CloudOff, Users } from 'lucide-react'
import { ApiErrorState } from './ApiErrorState'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { ChoiceChip } from './ChoiceChip'
import { DataTable, Td, Th, Tr } from './DataTable'
import { HelpSheet } from './HelpSheet'
import type { IconComponent } from './icons'
import { SegmentedControl } from './SegmentedControl'
import { SkeletonChart } from './Skeleton'
import { Wrapper } from './Wrapper'
import { ApiError } from '../api/client'
import { useSiteOrders, useTrafficFunnel } from '../hooks/useApi'
import { useIsPhone } from '../hooks/useMediaQuery'
import { formatNumber } from '../utils/formatters'
import {
  FOLDED_DEVICES, MIN_GROUP_PEOPLE, TRANSITIONS,
  biggestLeak, canCompare, conclude, conversion, conversionChange, enoughToClaim, foldDevices,
  funnelSteps, groupFigures, hasPeople, rowsGap, stepShare,
  type Against, type GroupFigure, type StepFigure, type Transition,
} from '../utils/funnel'
import {
  compareChip, convPct, fetchedAt, gapText, groupPct, heroWas, perBaseText, sayConclusion, sayMixShort,
  stepPct, toneOf, type HeroWas, type Line, type Tone,
} from '../utils/funnelText'
import { plural, type T } from '../utils/plural'
import { addDays, formatDay, formatDayRange, formatSpan, inclusiveDays, kyivToday } from '../utils/smsDates'
import type { FunnelRow, TrafficFunnelResponse } from '../types/api'

// ─── The site's purchase funnel, from Google Analytics ───────────────────────
//
// What the owner asked for on 2026-11-04, in his words: "Вошли → товар →
// корзина → оформление → покупка, Итог". So the block reads top to bottom as
// that funnel: the conversion and what it says first, then the five steps as
// bars from one left edge with what each step lost written between them, then
// who gets how far. The arithmetic is `utils/funnel`, the sentences
// `utils/funnelText`.
//
// Each fact is said once (the owner, 2026-11-05: «слишком много текста… ты
// теряешься, особенно с телефона»). The conversion and its verdict stand in
// the number at the top and nowhere else; what changes how a number reads —
// the last days still counting, too few people, the CRM's different count —
// stays on screen in a line each; what is read once — how the funnel is
// counted, who is cold or warm — is under a "?" that opens a sheet on a phone.
//
// On a phone the whole funnel, to «Итог», fits the first screen inside
// Telegram (the owner, 2026-11-11; production put «Итог» at 954 px on an
// 812 px phone): the comparison is a chip on the title row, the daily "still
// counting" notice a plain line, the colours keyed in the first line under a
// bar, «из 1 000 купили N» the «Итог» row, and what moved is said after it —
// all but the traffic mix, which stays under the number it qualifies. A wide
// screen is unchanged.

type Breakdown = 'type' | 'channel' | 'device'

const TONE_TEXT: Record<Tone, string> = {
  good: 'text-emerald-700',
  bad: 'text-red-700',
  neutral: 'text-slate-600',
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

/** What a group is: a type's members; a GA4 channel in our words and its type. */
function groupNote(row: FunnelRow, by: Breakdown, t: T, gloss: (key: string) => string): string | null {
  if (by === 'type') return t(`traffic.funnelTypeDesc.${row.key}`)
  if (by === 'channel') {
    const type = row.type ? t(`traffic.funnelType.${row.type}`).toLocaleLowerCase() : ''
    return [gloss(row.key), type].filter(Boolean).join(' · ') || null
  }
  return null
}

// ─── Frame and messages ──────────────────────────────────────────────────────

function FunnelHelp() {
  const { t } = useTranslation()
  return (
    <>
      {(['steps', 'bars', 'verdict', 'buyers', 'scope'] as const).map((part) => (
        <p key={part}>{t(`traffic.funnel.help.${part}`)}</p>
      ))}
    </>
  )
}

function FunnelFrame({ aside, children }: { aside?: ReactNode; children: ReactNode }) {
  const { t } = useTranslation()
  return (
    <Card animate="chart-in">
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-x-4 gap-y-2">
          <div className="flex items-center gap-2">
            <CardTitle>{t('traffic.funnelTitle')}</CardTitle>
            <HelpSheet title={t('traffic.funnelHowTitle')} label={t('traffic.funnelHowTitle')}>
              <FunnelHelp />
            </HelpSheet>
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

interface Notice {
  key: string
  icon: IconComponent
  text: string
  /** A notice said every day of a running month, in the shorter form a phone
   *  shows as a plain line: a box that is always there teaches the eye to
   *  skip boxes, and a real warning keeps its own. */
  routine?: string
}

function NoticeBox({ items }: { items: Notice[] }) {
  return (
    <ul className="flex flex-col gap-1.5 rounded-xl border border-amber-200 bg-amber-50 px-3.5 py-2.5
                   text-[13px] leading-snug text-amber-900">
      {items.map(({ key, icon: Icon, text }) => (
        <li key={key} className="flex items-start gap-2">
          <Icon className="mt-px h-4 w-4 shrink-0 text-amber-700" aria-hidden />
          <span>{text}</span>
        </li>
      ))}
    </ul>
  )
}

/** What changes how the figures read, a line each and in one place: the data
 *  is old, the last days are still being counted, the history starts later,
 *  too few people to judge. */
function Notices({ items }: { items: Notice[] }) {
  const warnings = items.filter((n) => n.routine == null)
  const routine = items.filter((n) => n.routine != null)
  return (
    <>
      <div className="hidden sm:block"><NoticeBox items={items} /></div>
      <div className="flex flex-col gap-2 sm:hidden">
        {warnings.length > 0 && <NoticeBox items={warnings} />}
        {routine.map(({ key, icon: Icon, routine: line }) => (
          <p key={key} className="flex items-start gap-1.5 text-xs leading-snug text-amber-900">
            <Icon className="mt-px h-3.5 w-3.5 shrink-0 text-amber-700" aria-hidden />
            <span>{line}</span>
          </p>
        ))}
      </div>
    </>
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
  const phone = useIsPhone()
  const since = formatDay(data.earliest, { month: 'long' })
  if (!against) {
    return <p className="max-w-xs text-xs leading-relaxed text-slate-600">{t('traffic.funnel.compare.none', { date: since })}</p>
  }
  const datesOf = (value: Against) => {
    const w = data.periods[value]
    const span = formatSpan(w.start, w.end)
    if (value === 'year_ago') return t('traffic.funnel.compare.datesYear', { dates: span })
    const n = inclusiveDays(w.start, w.end)
    return t('traffic.funnel.compare.datesPrevious', { dates: span, n: formatNumber(n), word: plural(t, 'word.days', n) })
  }
  const window = data.periods[against]
  // A phone's comparison: the window as a chip on the title row, the choice in
  // a sheet with each window's dates. The segmented control and its dates line
  // took two rows of the first screen.
  if (phone) {
    return (
      <ChoiceChip<Against>
        icon={ArrowLeftRight}
        label={compareChip(window.start, window.end)}
        ariaLabel={t('traffic.funnel.compare.chip', { dates: formatSpan(window.start, window.end) })}
        sheetTitle={t('traffic.funnel.compare.sheetTitle')}
        closeLabel={t('traffic.close')}
        value={against}
        options={([['previous', prevOk], ['year_ago', yearOk]] as const).map(([value, ok]) => ({
          value,
          disabled: !ok,
          label: t(`traffic.funnel.compare.${value}`),
          note: ok ? datesOf(value) : t(value === 'previous' ? 'traffic.funnel.compare.noPrevious' : 'traffic.funnel.compare.noYear', { date: since }),
        }))}
        onChange={onChange}
      />
    )
  }
  const dates = datesOf(against)
  const options = [
    { value: 'previous' as const, label: t('traffic.funnel.compare.previous'), disabled: !prevOk },
    { value: 'year_ago' as const, label: t('traffic.funnel.compare.year_ago'), disabled: !yearOk },
  ]
  return (
    <div className="flex w-full flex-col gap-1 sm:w-auto sm:items-end">
      <SegmentedControl<Against>
        options={options} value={against} onChange={onChange} size="sm"
        ariaLabel={t('traffic.funnel.compare.label')}
      />
      <span className="text-xs text-slate-600">{dates}</span>
      {!yearOk && <span className="max-w-sm text-xs leading-relaxed text-slate-600 sm:text-right">{t('traffic.funnel.compare.noYear', { date: since })}</span>}
      {!prevOk && <span className="max-w-sm text-xs leading-relaxed text-slate-600 sm:text-right">{t('traffic.funnel.compare.noPrevious', { date: since })}</span>}
    </div>
  )
})

// ─── The number and what it says ─────────────────────────────────────────────

const Hero = memo(function Hero({ rate, per, was }: { rate: string; per: string | null; was: HeroWas | null }) {
  const { t } = useTranslation()
  const strong = was != null && was.tone !== 'neutral'
  return (
    <div className="flex flex-[1_1_16rem] flex-col gap-1 rounded-xl bg-blue-50 px-4 py-3 sm:px-5 sm:py-4">
      <span className="text-[13px] font-semibold text-blue-800">{t('traffic.funnel.hero.label')}</span>
      <div className="flex items-end justify-between gap-3">
        <span className="text-4xl font-bold leading-none tabular-nums text-slate-900 sm:text-[44px]">{rate}</span>
        {was && (
          <span className={`flex flex-col items-end gap-0.5 pb-0.5 text-right ${strong ? `font-semibold ${TONE_TEXT[was.tone]}` : ''}`}>
            <span className={`text-sm tabular-nums ${strong ? '' : 'text-slate-900'}`}><Arrow tone={was.tone} />{was.was}</span>
            {was.verdict && <span className={`text-xs ${strong ? '' : 'text-slate-600'}`}>{was.verdict}</span>}
          </span>
        )}
      </div>
      {/* On a phone the «Итог» row says it, and the number stands alone. */}
      {per && <span className="hidden text-[13px] text-slate-700 sm:block">{per}</span>}
    </div>
  )
})

/** What only words can add: the counts, each step that moved and what it
 *  cost, the mix. A box beside the number on a wide screen, plain lines under
 *  it on a phone. */
const Conclusion = memo(function Conclusion({ lines }: { lines: Line[] }) {
  return (
    <div className="flex flex-[2_1_24rem] flex-col justify-center gap-1.5 sm:rounded-xl sm:border sm:border-slate-200 sm:px-5 sm:py-4">
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
 *  draws is written beside it, or for a screen reader under it. */
function Bar({ reached, before, thin }: { reached: number; before: number | null; thin?: boolean }) {
  const r = Math.max(0, Math.min(1, reached)) * 100
  const b = before == null ? null : Math.max(0, Math.min(1, before)) * 100
  return (
    <div className={`relative ${thin ? 'h-2' : 'h-5'}`} aria-hidden="true">
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

/** The words between two bars: «дошли 55 % · ушли 15 774», and under them the
 *  verdict when the step moved beyond noise. A bare «↓ 55 %» read as "fell by
 *  55 %"; a «было» no test judged read as a change. */
function Between({ s, isLeak, tight = false, keyed = false }: {
  s: StepFigure
  isLeak: boolean
  /** A phone's: less air, so the five steps and «Итог» fit one screen. */
  tight?: boolean
  /** The legend's swatches in this line — on a phone, where the legend
   *  itself took a row, the first line under a bar keys the colours. */
  keyed?: boolean
}) {
  const { t } = useTranslation()
  const was = s.was?.ofBefore
  const judged = (s.change === 'better' || s.change === 'worse') && was != null
  const swatch = (colour: string) => (
    <span aria-hidden="true" className={`mr-1 inline-block h-2 w-3 rounded-sm align-middle ${colour}`} />
  )
  return (
    <div className={`flex flex-col ${tight ? 'pb-0.5 pt-1' : 'py-1'} text-[13px] leading-snug`}>
      <p className={isLeak ? 'text-slate-900' : 'text-slate-600'}>
        {keyed && swatch('bg-blue-600')}{t('traffic.funnel.bars.reached')} <b className="text-slate-900">{stepPct(s.ofBefore)}</b>
        {' · '}{keyed && swatch('bg-slate-300')}{t('traffic.funnel.bars.left', { n: formatNumber(s.left ?? 0) })}
        {isLeak && <>{' · '}<b>{t('traffic.funnel.bars.leakShort')}</b></>}
      </p>
      {judged && (
        <p className={`font-semibold ${TONE_TEXT[toneOf(s.change)]}`}>
          <Arrow tone={toneOf(s.change)} />{t(`traffic.funnel.bars.${s.change}`, { was: stepPct(was) })}
        </p>
      )}
    </div>
  )
}

const FunnelBars = memo(function FunnelBars({
  steps, leakStep, total, phoneTotal,
}: {
  steps: StepFigure[]
  leakStep: Transition | null
  total: string
  /** What a phone's «Итог» row says: «из 1 000 зашедших купили 18». */
  phoneTotal: string
}) {
  const { t } = useTranslation()
  const last = steps.length - 1
  // «было N» beside the first and the last step only: they are the counts the
  // sentence above compares, and a middle step's count nobody judged.
  const wasAt = (s: StepFigure, i: number) => (s.was && (i === 0 || i === last)
    ? t('traffic.funnel.bars.was', { was: formatNumber(s.was.people) })
    : null)
  // The share of everyone who entered is the bar's length: written out for a
  // reader who does not see the bar.
  const ofEntered = (s: StepFigure, i: number) => (i === 0
    ? null
    : t('traffic.funnel.bars.ofEntered', { share: i === last ? convPct(s.ofEntered) : stepPct(s.ofEntered) }))

  return (
    <Wrapper dir="column" gap="xs">
      <div className="hidden flex-wrap gap-4 pb-1 text-xs text-slate-600 sm:flex" aria-hidden="true">
        <span className="flex items-center gap-1.5">
          <span className="h-2.5 w-3.5 rounded-sm bg-blue-600" />{t('traffic.funnel.bars.reached')}
        </span>
        <span className="flex items-center gap-1.5">
          <span className="h-2.5 w-3.5 rounded-sm bg-slate-300" />{t('traffic.funnel.bars.tail')}
        </span>
      </div>

      {/* Wide: name | bar | figures, one row per step and the words between. */}
      <div className="hidden grid-cols-[11rem_minmax(0,1fr)_9rem] items-center gap-x-4 gap-y-1 sm:grid">
        {steps.map((s, i) => (
          <Fragment key={s.step}>
            {i > 0 && (
              <>
                <span />
                <Between s={s} isLeak={s.step === leakStep} />
                <span />
              </>
            )}
            <span className="text-sm font-semibold text-slate-900">{t(`traffic.funnelStep.${s.step}`)}</span>
            <Bar reached={s.ofEntered ?? 0} before={i > 0 ? steps[i - 1].ofEntered : null} />
            <span className="flex flex-col text-right">
              <span className="text-lg font-bold tabular-nums text-slate-900">{formatNumber(s.people)}</span>
              {wasAt(s, i) && <span className="text-xs text-slate-600">{wasAt(s, i)}</span>}
              {ofEntered(s, i) && <span className="sr-only">{ofEntered(s, i)}</span>}
            </span>
          </Fragment>
        ))}
        <span className="mt-2 border-t border-slate-200 pt-2.5 text-[15px] font-bold text-slate-900">
          {t('traffic.funnel.total.label')}
        </span>
        <span className="col-span-2 mt-2 border-t border-slate-200 pt-2.5 text-[15px] text-slate-900">{total}</span>
      </div>

      {/* Narrow: the same steps stacked, the bar under its name. */}
      <ol className="flex flex-col sm:hidden">
        {steps.map((s, i) => (
          <li key={s.step} className="flex flex-col">
            {i > 0 && <Between s={s} isLeak={s.step === leakStep} tight keyed={i === 1} />}
            <div className="flex flex-col gap-1">
              <div className="flex items-baseline justify-between gap-2">
                <span className="text-sm font-semibold text-slate-900">
                  {t(`traffic.funnelStep.${s.step}`)}
                  {wasAt(s, i) && <span className="font-normal text-slate-600"> · {wasAt(s, i)}</span>}
                </span>
                <span className="text-base font-bold tabular-nums text-slate-900">{formatNumber(s.people)}</span>
              </div>
              <Bar reached={s.ofEntered ?? 0} before={i > 0 ? steps[i - 1].ofEntered : null} thin />
              {ofEntered(s, i) && <span className="sr-only">{ofEntered(s, i)}</span>}
            </div>
          </li>
        ))}
      </ol>
      <div className="flex items-baseline justify-between gap-2 border-t border-slate-200 pt-2 sm:hidden">
        <span className="text-[15px] font-bold text-slate-900">{t('traffic.funnel.total.label')}</span>
        <span className="text-[15px] text-slate-900">{phoneTotal}</span>
      </div>
    </Wrapper>
  )
})

/** The CRM's count of the website's orders beside the funnel's buyers, in a
 *  line, and why the two differ one tap away. Without the count it still says
 *  the funnel counts people, not orders. */
function CrmLine({ buyers, orders, oneDay, abroadLeftOut }: {
  buyers: number
  orders: number | null
  oneDay: boolean
  /** The funnel counts Ukraine alone, while the CRM's orders come from anywhere. */
  abroadLeftOut: boolean
}) {
  const { t } = useTranslation()
  const line = orders != null
    ? t(oneDay ? 'traffic.funnel.crm.lineDay' : 'traffic.funnel.crm.lineDays', {
      n: formatNumber(orders), word: plural(t, 'word.orders', orders),
    })
    : t('traffic.funnel.crm.fallback')
  return (
    <div className="flex flex-col text-[13px] leading-relaxed text-slate-600 sm:flex-row sm:flex-wrap sm:items-baseline sm:gap-x-2">
      <span>{line}</span>
      <HelpSheet link title={t('traffic.funnel.crm.sheetTitle')} label={t('traffic.funnel.crm.why')}>
        {orders != null && (
          <p>
            {t('traffic.funnel.crm.sheetCounts', {
              buyers: formatNumber(buyers), buyersWord: plural(t, 'word.buyers', buyers),
              orders: formatNumber(orders), ordersWord: plural(t, 'word.orders', orders),
            })}
          </p>
        )}
        <p>{t('traffic.funnel.crm.sheetWhy')}</p>
        {abroadLeftOut && <p>{t('traffic.funnel.crm.sheetAbroad')}</p>}
      </HelpSheet>
    </div>
  )
}

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

/** A phone's row: name and share, the conversion, what it was. */
const PHONE_ROW = 'grid grid-cols-[minmax(0,1fr)_4.75rem_3.75rem] items-baseline gap-x-2'

const FunnelBreakdown = memo(function FunnelBreakdown({
  data, against, judgeNow, judgeThen,
}: {
  data: TrafficFunnelResponse
  against: Against | null
  judgeNow: boolean
  judgeThen: boolean
}) {
  const { t, i18n } = useTranslation()
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
  // A GA4 channel in our words; one GA4 adds later stays as GA4 names it.
  const gloss = (key: string) => {
    const k = `traffic.funnelChannelDesc.${key}`
    return i18n.exists(k) ? t(k) : ''
  }
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
        <div className="flex items-center gap-2">
          <h4 className="text-[15px] font-bold text-slate-900">{t('traffic.funnel.table.title')}</h4>
          <HelpSheet title={t('traffic.funnel.table.helpTitle')} label={t('traffic.funnel.table.helpTitle')}>
            {(['cold', 'warm', 'other'] as const).map((type) => (
              <p key={type}>
                <b>{t(`traffic.funnelType.${type}`)}</b> — {t(`traffic.funnelTypeDesc.${type}`)}
              </p>
            ))}
            <p>{t('traffic.funnel.table.helpMarks')}</p>
            {gapLine && <p>{gapLine}</p>}
          </HelpSheet>
        </div>
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
              const note = groupNote(g.row, by, t, gloss)
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

      {/* A phone: the conversion and what it was in two columns, read down;
          under each row what it is and, in words, where it falls short. */}
      <div className="flex flex-col sm:hidden">
        <div className={`${PHONE_ROW} px-2.5 pb-1 text-[11px] text-slate-500`}>
          <span />
          <span className="text-right">{t('traffic.funnel.table.colConversion')}</span>
          <span className="text-right">{against ? t('traffic.funnel.table.colWas') : ''}</span>
        </div>
        <div className={`${PHONE_ROW} rounded-lg bg-blue-50 px-2.5 py-2`}>
          <span className="text-sm font-bold text-slate-900">{t('traffic.funnel.table.site')}</span>
          <span className="text-right text-sm font-bold tabular-nums text-slate-900">{convPct(siteConversion)}</span>
          <span className="text-right text-[13px] tabular-nums text-slate-600">
            {siteWasConversion != null ? convPct(siteWasConversion) : ''}
          </span>
        </div>
        <ul className="flex flex-col divide-y divide-slate-100">
          {groups.map((g) => {
            const note = groupNote(g.row, by, t, gloss)
            const people = by === 'type' ? null
              : `${formatNumber(g.row.current.entered)} → ${formatNumber(g.row.current.purchased)}`
            const hint = [note, people].filter(Boolean).join(' · ')
            const lower = TRANSITIONS.filter((step) => g.steps[step].lower)
            const tone = toneOf(g.change)
            return (
              <li key={g.row.key} className={`flex flex-col gap-0.5 px-2.5 py-2 ${g.small ? 'opacity-60' : ''}`}>
                <div className={PHONE_ROW}>
                  <span className="text-sm font-semibold text-slate-900">{groupName(g.row, by, t)} · {groupPct(g.shareOfSite)}</span>
                  <span className="text-right text-sm font-bold tabular-nums text-slate-900">{convPct(g.conversion)}</span>
                  <span className="text-right text-[13px] tabular-nums text-slate-600">{g.was != null ? convPct(g.was) : '—'}</span>
                </div>
                {hint && <span className="text-xs text-slate-500">{hint}</span>}
                {tone !== 'neutral' && (
                  <span className={`text-xs font-semibold ${TONE_TEXT[tone]}`}>
                    <Arrow tone={tone} />{t(`traffic.funnel.table.${g.change}`)}
                  </span>
                )}
                {lower.map((step) => (
                  <span key={step} className="text-xs font-semibold text-red-700">
                    <Arrow tone="bad" />
                    {t('traffic.funnel.table.lowerLine', {
                      step: t(`traffic.funnel.transitionShort.${step}`), value: stepPct(g.steps[step].share),
                    })}
                  </span>
                ))}
              </li>
            )
          })}
        </ul>
      </div>

      <div className="flex flex-col gap-1 text-xs leading-relaxed text-slate-600">
        {/* A phone's rows say each ▼ in words; there are no cells to explain. */}
        {judgeNow && <p className="hidden sm:block">{t('traffic.funnel.table.lowerNote')}</p>}
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
  const ordersFrom = period.start < data.earliest ? data.earliest : period.start
  const orders = useSiteOrders(ordersFrom, period.end)
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
  const verdict = judgeThen && was && against ? conclude(now, was, data.by_type, against) : null
  const lines = verdict ? sayConclusion(verdict, t) : []
  // On a phone what moved is said after «Итог», so the funnel fits the first
  // screen — all but the mix, said in one line under the number: there a green
  // «лучше обычного» alone read as the whole story (October 2026 against 2025,
  // buyers fewer and a fifth of the rise from the mix). It is the last line
  // `sayConclusion` writes.
  const mixShort = verdict?.mix ? sayMixShort(verdict.mix, t) : null
  const phoneLines = verdict?.mix ? lines.slice(0, -1) : lines
  const convChange = judgeThen && was ? conversionChange(now, was).change : null
  const convWas = was ? conversion(was) : null
  const rate = conversion(now)

  const notices: Notice[] = []
  if (data.stale) {
    notices.push({ key: 'stale', icon: CloudOff, text: t('traffic.funnel.state.stale', { time: fetchedAt(data.fetched_at, today) }) })
  }
  // GA4 is still counting yesterday and today: their figures can grow.
  const unsettledFrom = period.start > addDays(today, -1) ? period.start : addDays(today, -1)
  const unsettledTo = period.end < today ? period.end : today
  if (unsettledFrom <= unsettledTo) {
    const days = formatSpan(unsettledFrom, unsettledTo)
    notices.push({
      key: 'unsettled', icon: Clock,
      text: t('traffic.funnel.state.incomplete', { days }),
      routine: t('traffic.funnel.state.incompleteShort', { days }),
    })
  }
  if (period.start < data.earliest) {
    notices.push({ key: 'partial', icon: CalendarX, text: t('traffic.funnel.state.partial', { date: since }) })
  }
  // Both counts, since either can be the one too small: a site with 2 000
  // visitors and 40 buyers is judged no more than one with 1 100.
  if (!judgeNow) {
    notices.push({
      key: 'few', icon: Users,
      text: t('traffic.funnel.state.few', { entered: formatNumber(now.entered), bought: formatNumber(now.purchased) }),
    })
  } else if (was && !judgeThen) {
    notices.push({
      key: 'fewThen', icon: Users,
      text: t('traffic.funnel.state.fewThen', { entered: formatNumber(was.entered), bought: formatNumber(was.purchased) }),
    })
  }

  return (
    <FunnelFrame aside={<CompareWith data={data} against={against} prevOk={prevOk} yearOk={yearOk} onChange={setPicked} />}>
      <Wrapper dir="column" gap="lg">
        {notices.length > 0 && <Notices items={notices} />}
        <Wrapper dir="row" wrap gap="lg">
          <Hero rate={convPct(rate)} per={perBaseText(now, t)} was={heroWas(convWas, convChange, t)} />
          {lines.length > 0 && <div className="hidden sm:contents"><Conclusion lines={lines} /></div>}
        </Wrapper>
        {mixShort && <p className="-mt-2 text-[13px] leading-relaxed text-slate-700 sm:hidden">{mixShort}</p>}
        <Wrapper dir="column" gap="xs">
          <FunnelBars
            steps={steps}
            leakStep={biggestLeak(now)}
            total={t('traffic.funnel.total.short', { rate: convPct(rate) })}
            phoneTotal={perBaseText(now, t) ?? t('traffic.funnel.total.short', { rate: convPct(rate) })}
          />
          <CrmLine
            buyers={now.purchased}
            orders={orders.data?.totalOrders ?? null}
            oneDay={inclusiveDays(ordersFrom, period.end) === 1}
            abroadLeftOut={data.country === 'ua'}
          />
        </Wrapper>
        {phoneLines.length > 0 && (
          <div className="sm:hidden">
            <h4 className="mb-1 text-[13px] font-semibold text-slate-900">{t('traffic.funnel.changedTitle')}</h4>
            <Conclusion lines={phoneLines} />
          </div>
        )}
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
