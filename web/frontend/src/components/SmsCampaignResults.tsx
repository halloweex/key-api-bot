import { memo, useMemo, useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Select } from './Select'
import { Badge } from './Badge'
import { Checkbox } from './Checkbox'
import { EmptyState } from './EmptyState'
import { InfoBanner } from './InfoBanner'
import { SkeletonCard } from './Skeleton'
import { DataTable, Th, Td, Tr } from './DataTable'
import { SmsResultsGuide } from './SmsResultsGuide'
import { SmsLiftForest, type ForestRow } from './SmsLiftForest'
import { useSmsCampaigns, useSmsCampaignResults } from '../hooks/useApi'
import { formatCurrency, formatNumber } from '../utils/formatters'
import type { SmsComparison, SmsGroupStats } from '../types/api'
import { ApiError } from '../api/client'

// ─── SmsCampaignResults ──────────────────────────────────────────────────────
//
// Reads a campaign against its control.
//
// The layout deliberately puts the control group beside the messaged one at
// every level. The target's own conversion is the number people want to quote,
// and on its own it is meaningless — Tier 1 buys at ~46% without any message.
// Showing the pair side by side makes the comparison the obvious reading.
//
// This used to be four blocks — an "all tiers" panel and a card per tier —
// each printing the same six figures with its own miniature interval plot.
// Twenty-four numbers, no two of them on a scale that let you compare them,
// and four grey bars that said "not proven" without ever saying it. Now the
// same six figures are columns: one row per arm, totals row first, so reading
// down a column is the comparison. The forest plot is the only chart, because
// interval width is the one thing a table cannot show.

// The window should match how long the offer lives. Days after it expires add
// ordinary trading to both arms equally — no effect, more variance — so a long
// window on a short promo weakens the reading rather than strengthening it.
// The shortest option used to be 7, which left a two-day sale unmeasurable on
// its own terms; the API has always accepted 1.
const WINDOWS = [1, 2, 3, 7, 14, 30, 90]

/** One arm of the campaign, as the table sees it. */
interface ResultRow {
  key: string
  label: string
  target: SmsGroupStats
  holdout: SmsGroupStats
  comparison: SmsComparison | null
  /** The totals row, which leads the table and carries the verdict. */
  emphasis?: boolean
}

// Three states, not two. "No effect shown" is a claim about the campaign;
// before either arm has bought enough, the only true statement is that nobody
// has measured anything yet, and saying the stronger thing reads as a failure.
function Verdict({ comparison }: { comparison: SmsComparison }) {
  const { t } = useTranslation()

  if (!comparison.verdictReady) {
    return (
      <Badge tone="orange">
        {t('sms.verdictTooEarly', { n: comparison.eventsHoldout })}
      </Badge>
    )
  }
  return comparison.significant ? (
    <Badge tone="green">
      {t('sms.verdictProven', { lift: comparison.liftPp.toFixed(1) })}
    </Badge>
  ) : (
    <Badge tone="slate">{t('sms.verdictInconclusive')}</Badge>
  )
}

/** Rate over the arm, the counts it came from, and what one person was worth.
 *
 *  The per-person line is the whole reason this cell has three rows. An arm's
 *  own money only ever appeared here as a conversion percentage, and the
 *  incremental figure two columns over is a difference — so "₴22 a head against
 *  ₴7 a head" , the one comparison anybody reads without training, was on the
 *  page nowhere. It is deliberately the arm's own revenue over its own
 *  contacts: no modelling, no interval, just what happened divided by how many
 *  it happened to. */
function RateCell({ stats }: { stats: SmsGroupStats }) {
  const { t } = useTranslation()
  const rate = stats.contacts ? (100 * stats.converted) / stats.contacts : 0
  const perPerson = stats.contacts ? stats.revenue / stats.contacts : 0

  return (
    <>
      <div className="font-medium text-slate-800">{rate.toFixed(1)}%</div>
      <div className="text-[11px] text-slate-500">
        {formatNumber(stats.converted)} / {formatNumber(stats.contacts)}
      </div>
      <div className="text-[11px] text-slate-600 mt-0.5">
        {formatCurrency(perPerson)} <span className="text-slate-400">{t('sms.perPerson')}</span>
      </div>
    </>
  )
}

/** A total, over the per-contact figure it was scaled from.
 *
 *  The total is what you weigh against what the send cost; the per-contact
 *  number is what you weigh against the ~₴1.3 a message costs to deliver.
 *  Both are wanted and neither is a column of its own.
 *
 *  `estimate` marks the figure as not yet a result. Money is far less stable
 *  than the conversion rate it sits beside — revenue per contact runs a
 *  standard deviation thirteen times its own mean, so one large order moves a
 *  total by six figures — and the p-value in the same row belongs to the rate,
 *  not to this. Printed plain, a column reading "₴12 179" gave no hint that a
 *  third of its plausible range was a loss. It stays visible, because watching
 *  it move is the point, but it must not read as the result. */
function MoneyCell({ total, perContact, estimate }: {
  total: number
  perContact: number
  estimate?: boolean
}) {
  const { t } = useTranslation()

  if (estimate) {
    return (
      <>
        <div className="text-slate-400">{formatCurrency(total)}</div>
        <div className="text-[11px] text-slate-400 italic">{t('sms.moneyEstimate')}</div>
      </>
    )
  }
  return (
    <>
      <div className="text-slate-800">{formatCurrency(total)}</div>
      <div className="text-[11px] text-slate-500">
        {perContact.toFixed(2)} {t('sms.perContact')}
      </div>
    </>
  )
}

function formatP(p: number): string {
  return p < 0.001 ? '<0.001' : p.toFixed(3)
}

/** The answer, in a sentence, before any of the apparatus.
 *
 *  What this block showed first used to be a forest plot and a table with
 *  p-values and percentage points — for a campaign whose entire dataset is one
 *  row. Every figure that mattered was on the page and none of them added up to
 *  a sentence, so the honest summary ("we spent ₴5 375, we appear to have made
 *  ₴64 000 more than the control, and it cannot be proven because one person in
 *  the control bought anything") had to be assembled by the reader.
 *
 *  The caveat is not a footnote here. Money that has not cleared the verdict is
 *  a plausible range with a loss in it, and this is the line most likely to be
 *  quoted at somebody, so the reason it might be wrong travels with it. */
function Headline({ comparison, costTotal }: {
  comparison: SmsComparison
  costTotal: number | null | undefined
}) {
  const { t } = useTranslation()
  const proven = comparison.significant
  const payback =
    costTotal != null ? comparison.incrementalMarginTotal - costTotal : null

  return (
    <div className="mb-4 rounded-lg border border-slate-200 bg-slate-50/70 px-4 py-3">
      <p className="text-[15px] leading-snug text-slate-800">
        {t(proven ? 'sms.headlineProven' : 'sms.headlineApparent', {
          revenue: formatCurrency(comparison.incrementalRevenueTotal),
        })}
      </p>

      <p className="mt-1 text-xs text-slate-600 tabular-nums">
        {t('sms.headlineMargin', {
          margin: formatCurrency(comparison.incrementalMarginTotal),
        })}
        {costTotal != null && (
          <>
            {' · '}
            {t('sms.headlineSpent', { cost: formatCurrency(costTotal) })}
            {' · '}
            <span className={payback! >= 0 ? 'text-emerald-700' : 'text-rose-700'}>
              {t(payback! >= 0 ? 'sms.headlinePaidBack' : 'sms.headlineShort', {
                amount: formatCurrency(Math.abs(payback!)),
              })}
            </span>
          </>
        )}
      </p>

      {/* Why the number above may not survive contact with more data. */}
      <p className="mt-1.5 text-xs text-slate-500 leading-snug">
        {!comparison.verdictReady
          ? t('sms.headlineWhyEarly', {
              n: comparison.eventsHoldout, min: comparison.minEvents,
            })
          : proven
            ? t('sms.headlineWhyProven')
            : t('sms.headlineWhyWide')}
      </p>
    </div>
  )
}

/** Controlled from the page when the campaign list drives the choice; the
 *  props stay optional so the block still works — and still tests — alone. */
interface SmsCampaignResultsProps {
  campaign?: string | null
  onCampaignChange?: (campaign: string) => void
}

export const SmsCampaignResults = memo(function SmsCampaignResults({
  campaign: campaignProp,
  onCampaignChange,
}: SmsCampaignResultsProps = {}) {
  const { t } = useTranslation()
  const [ownCampaign, setOwnCampaign] = useState<string | null>(null)
  const [windowDays, setWindowDays] = useState(30)
  const [deliveredOnly, setDeliveredOnly] = useState(false)
  const [showStats, setShowStats] = useState(false)

  const { data: list } = useSmsCampaigns()
  const sent = useMemo(
    () => (list?.campaigns ?? []).filter((c) => c.sentAt),
    [list],
  )

  const campaign = campaignProp !== undefined ? campaignProp : ownCampaign
  // `Select` hands back `string | null` even with `allowEmpty={false}`; an
  // empty choice is not a campaign, so it is ignored rather than propagated.
  const setCampaign = (c: string | null) => {
    if (!c) return
    setOwnCampaign(c)
    onCampaignChange?.(c)
  }
  const selected = campaign ?? sent[0]?.campaign ?? null
  const { data, isLoading, error } = useSmsCampaignResults(
    selected, windowDays, deliveredOnly,
  )
  // Anyone the gateway never took is in the target arm without ever having
  // been messaged, which pulls the measured lift toward nothing. Saying so is
  // more use than quietly dropping them.
  const notSent = data?.overall.target.notSent ?? 0

  // Totals first, then the tiers. Both the table and the chart read from this,
  // so the two can never fall out of step.
  const rows = useMemo<ResultRow[]>(() => {
    if (!data) return []
    return [
      {
        key: 'overall',
        label: t('sms.overall'),
        target: data.overall.target,
        holdout: data.overall.holdout,
        comparison: data.overall.comparison,
        emphasis: true,
      },
      ...data.segments.map((s) => ({
        key: s.tier,
        label: t(`sms.tier.${s.tier}`),
        target: s.target,
        holdout: s.holdout,
        comparison: s.comparison,
      })),
    ]
  }, [data, t])

  // One shared scale is the whole point, so the chart takes every arm that
  // produced a comparison at all.
  const forestRows = useMemo<ForestRow[]>(
    () =>
      rows
        .filter((r): r is ResultRow & { comparison: SmsComparison } => !!r.comparison)
        .map((r) => ({
          label: r.label,
          comparison: r.comparison,
          contacts: r.target.contacts,
          emphasis: r.emphasis,
        })),
    [rows],
  )

  // Every interval crossing zero is a state in its own right, not four grey
  // bars to squint at. It means the campaign has not been measured yet — which
  // is emphatically not the same as it having done nothing.
  const nothingProven =
    forestRows.length > 0 && forestRows.every((r) => !r.comparison.significant)
  // And there are two ways to arrive there. Too few purchases to judge is the
  // ordinary state of a campaign in its first days; intervals that span zero on
  // arms that have bought plenty is a much later, much weaker result. They
  // deserve different sentences.
  const nothingMeasurable =
    nothingProven && forestRows.every((r) => !r.comparison.verdictReady)
  const minEvents = forestRows[0]?.comparison.minEvents ?? 5

  if (sent.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>{t('sms.resultsTitle')}</CardTitle>
        </CardHeader>
        <CardContent>
          <EmptyState
            message={t('sms.noSentCampaigns')}
            hint={t('sms.noSentCampaignsHint')}
          />
        </CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <Badge tone="slate" shape="tag">{t('sms.step', { n: 3 })}</Badge>
              <CardTitle>{t('sms.resultsTitle')}</CardTitle>
              <SmsResultsGuide />
            </div>
            <p className="text-xs text-slate-500 mt-0.5">{t('sms.resultsDesc')}</p>
          </div>
          <div className="flex items-center gap-2">
            <Select
              options={sent.map((c) => ({ value: c.campaign, label: c.campaign }))}
              value={selected}
              onChange={setCampaign}
              allowEmpty={false}
              variant="compact"
              aria-label={t('sms.campaignName')}
            />
            <Select
              options={WINDOWS.map((w) => ({
                value: String(w),
                label: t('sms.windowDays', { days: w }),
              }))}
              value={String(windowDays)}
              onChange={(v) => setWindowDays(Number(v) || 30)}
              allowEmpty={false}
              variant="compact"
              aria-label={t('sms.window')}
            />
          </div>
        </div>
      </CardHeader>

      <CardContent>
        {isLoading && <SkeletonCard />}

        {error && (
          <EmptyState
            message={
              error instanceof ApiError && error.status === 409
                ? t('sms.notSentYet')
                : t('sms.resultsFailed')
            }
          />
        )}

        {data && !isLoading && (
          <>
            {/* ── How much of the arm was actually treated ────────────── */}
            {notSent > 0 && (
              <div className="mb-4 text-xs text-amber-800 bg-amber-50 rounded-md px-3 py-2 leading-snug">
                {t('sms.notSentWarning', { n: formatNumber(notSent) })}
              </div>
            )}

            <label className="mb-4 flex items-start gap-2 cursor-pointer">
              <Checkbox checked={deliveredOnly} onChange={setDeliveredOnly} size="sm" />
              <span className="text-xs text-slate-600 leading-snug">
                <span className="font-medium text-slate-700">
                  {t('sms.deliveredOnlyLabel')}
                </span>
                {' — '}
                {t('sms.deliveredOnlyHint')}
              </span>
            </label>

            {/* ── Nothing clears zero: say so once, plainly ───────────── */}
            {nothingProven && (
              <div className="mb-4">
                <InfoBanner
                  title={t(nothingMeasurable
                    ? 'sms.tooEarlyTitle' : 'sms.insufficientTitle')}
                >
                  {nothingMeasurable
                    ? t('sms.tooEarlyBody', { n: minEvents })
                    : t('sms.insufficientBody')}
                </InfoBanner>
              </div>
            )}

            {/* ── The answer, before the apparatus ────────────────────── */}
            {data.overall.comparison && (
              <Headline
                comparison={data.overall.comparison}
                costTotal={data.costTotal}
              />
            )}

            {/* ── Every arm on one axis — folded away by default ───────
                Interval width is worth a chart, but it is an expert's chart,
                and it was the first thing on screen above a table nobody had
                got to yet. It stays one click away rather than gone. */}
            {forestRows.length > 0 && (
              <div className="mb-4">
                <button
                  type="button"
                  onClick={() => setShowStats((s) => !s)}
                  aria-expanded={showStats}
                  className="flex items-center gap-1.5 text-xs font-medium text-slate-600
                             hover:text-purple-800"
                >
                  {showStats
                    ? <ChevronDown className="w-3.5 h-3.5" />
                    : <ChevronRight className="w-3.5 h-3.5" />}
                  {t('sms.statsToggle')}
                </button>

                {showStats && (
                  <div className="mt-2 rounded-lg border border-slate-200 p-3 sm:p-4">
                    <h3 className="text-sm font-medium text-slate-700 mb-1">
                      {t('sms.forestTitle')}
                    </h3>
                    <p className="text-xs text-slate-500 mb-3">{t('sms.forestDesc')}</p>
                    <SmsLiftForest rows={forestRows} />
                  </div>
                )}
              </div>
            )}

            {/* ── The figures, one row per arm ────────────────────────── */}
            {rows.length > 0 && (
              <div className="rounded-lg border border-slate-200">
                <DataTable>
                  <thead>
                    <Tr header>
                      <Th>{t('sms.colGroup')}</Th>
                      <Th align="right">{t('sms.colMessaged')}</Th>
                      <Th align="right">{t('sms.colControl')}</Th>
                      <Th align="right">{`${t('sms.lift')}, ${t('sms.pp')}`}</Th>
                      {showStats && <Th align="right">{t('sms.pValue')}</Th>}
                      <Th align="right">{t('sms.guideRevenueTerm')}</Th>
                      <Th align="right">{t('sms.guideMarginTerm')}</Th>
                    </Tr>
                  </thead>
                  <tbody>
                    {rows.map((r) => {
                      const c = r.comparison
                      // Colour is state, and it is named in the forest key
                      // above — an unproven lift must not read as a win.
                      const liftInk = c?.significant
                        ? 'text-emerald-700 font-medium'
                        : 'text-slate-600'

                      return (
                        <Tr key={r.key} hover={false}>
                          <Td bold={r.emphasis}>
                            <span className="flex flex-wrap items-center gap-2">
                              {r.label}
                              {r.emphasis && c && <Verdict comparison={c} />}
                            </span>
                          </Td>
                          <Td align="right" tabular>
                            <RateCell stats={r.target} />
                          </Td>
                          <Td align="right" tabular>
                            <RateCell stats={r.holdout} />
                          </Td>
                          {c ? (
                            <>
                              <Td align="right" tabular>
                                <span className={liftInk}>
                                  {c.liftPp > 0 ? '+' : ''}{c.liftPp.toFixed(1)}
                                </span>
                              </Td>
                              {showStats && (
                                <Td align="right" tabular>{formatP(c.pValue)}</Td>
                              )}
                              <Td align="right" tabular>
                                <MoneyCell
                                  total={c.incrementalRevenueTotal}
                                  perContact={c.incrementalRevenuePerContact}
                                  estimate={!c.verdictReady}
                                />
                              </Td>
                              <Td align="right" tabular>
                                <MoneyCell
                                  total={c.incrementalMarginTotal}
                                  perContact={c.incrementalMarginPerContact}
                                  estimate={!c.verdictReady}
                                />
                              </Td>
                            </>
                          ) : (
                            <Td align="right" colSpan={showStats ? 4 : 3}>
                              <span className="text-[11px] text-slate-500">
                                {t('sms.noControlInTier')}
                              </span>
                            </Td>
                          )}
                        </Tr>
                      )
                    })}
                  </tbody>
                </DataTable>
              </div>
            )}

            {/* ── Did it pay for itself ───────────────────────────────── */}
            {data.costTotal != null && data.overall.comparison && (
              <div className="mt-3 flex flex-wrap items-baseline gap-x-2 gap-y-1
                              text-sm tabular-nums">
                <span className="text-slate-600">{t('sms.payback')}</span>
                <span className="font-medium text-slate-800">
                  {formatCurrency(
                    data.overall.comparison.incrementalMarginTotal - data.costTotal,
                  )}
                </span>
                <span className="text-xs text-slate-500">
                  {t('sms.paybackParts', {
                    margin: formatCurrency(data.overall.comparison.incrementalMarginTotal),
                    cost: formatCurrency(data.costTotal),
                  })}
                </span>
                {/* The verdict gates this the same way it gates the money
                    columns: a margin nobody has measured minus a cost that is
                    exact is not a profit, it is an exact number of a guess. */}
                {!data.overall.comparison.verdictReady && (
                  <span className="text-xs text-slate-400 italic">
                    {t('sms.moneyEstimate')}
                  </span>
                )}
              </div>
            )}

            {data.promocode && (
              <p className="mt-3 text-xs text-slate-500">
                {t('sms.promoOrders', {
                  code: data.promocode,
                  count: data.overall.target.promoOrders,
                })}
              </p>
            )}

            <p className="mt-3 text-[11px] text-slate-400 leading-snug">
              {t('sms.deliveryCaveat')}
            </p>

            {data.segments.length === 0 && <EmptyState message={t('sms.noRosterData')} />}
          </>
        )}
      </CardContent>
    </Card>
  )
})
