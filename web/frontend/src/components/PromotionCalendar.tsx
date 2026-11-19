import { memo, useCallback, useMemo, useRef, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { Plus } from 'lucide-react'
import { ApiErrorState } from './ApiErrorState'
import { Button } from './Button'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Checkbox } from './Checkbox'
import { PromotionForm } from './PromotionForm'
import { PromotionItem } from './PromotionItem'
import { SkeletonTable } from './Skeleton'
import { useToast } from './Toast'
import { Wrapper } from './Wrapper'
import {
  useBrands,
  useCancelPromotion,
  useCategories,
  usePromotions,
  useRestorePromotion,
  useSavePromotion,
  useSmsCampaigns,
} from '../hooks/useApi'
import { usePermission } from '../hooks/useAuth'
import { useHashTarget, useScrollIntoViewWhenSettled } from '../hooks/useDeepLink'
import { PROMOTIONS_ANCHOR } from '../utils/deepLink'
import { splitPromotions } from '../utils/promotions'
import { kyivToday } from '../utils/smsDates'
import type { Promotion, PromotionIn } from '../types/api'

// ─── PromotionCalendar ───────────────────────────────────────────────────────
//
// «Календар акцій» on /marketing (phase 1, 2026-11-19). The revenue forecast
// cannot know a promotion in advance, so the calendar records what is known
// before one starts, and the month's badge on the dashboard says a line for
// each planned one. The lists are what every reader of /marketing sees; the
// form, «Змінити», «Скасувати» and «Відновити» are `marketing` edit's.
//
// `/marketing#promotions` lands here — the bot's Monday [Додати] and the
// badge's «+ акція» both link to it — and opens the form for somebody who may
// add; for a reader who may not, it scrolls to the lists.

/** What the form is open on: a new promotion, or one being changed. */
type Editing = null | 'new' | Promotion

function ListBlock({
  title, hint, empty, children,
}: {
  title: string
  hint?: string
  /** Said instead of the list when there is nothing in it. */
  empty: string | null
  children: ReactNode
}) {
  return (
    <Wrapper dir="column" gap="sm">
      <h4 className="text-xs font-semibold uppercase tracking-wide text-slate-500">
        {title}
        {hint && <span className="ml-1 font-normal normal-case tracking-normal">{hint}</span>}
      </h4>
      {empty ? (
        <p className="text-sm text-slate-500">{empty}</p>
      ) : (
        <ul className="divide-y divide-slate-100">{children}</ul>
      )}
    </Wrapper>
  )
}

export const PromotionCalendar = memo(function PromotionCalendar() {
  const { t } = useTranslation()
  const { addToast } = useToast()
  const { canEdit, isLoading: permsLoading } = usePermission('marketing')
  const { canView: canViewSms } = usePermission('sms')
  const today = kyivToday()

  const { data, isLoading, error, refetch } = usePromotions()
  const [showCancelled, setShowCancelled] = useState(false)
  const [editing, setEditing] = useState<Editing>(null)
  const [pendingId, setPendingId] = useState<number | null>(null)
  const save = useSavePromotion()
  const cancel = useCancelPromotion()
  const restore = useRestorePromotion()

  // The link opens the form once per arrival; closing it is the reader's.
  // Set while rendering, from the hash, rather than in an effect: the form
  // is there in the first frame the section is.
  const targeted = useHashTarget(PROMOTIONS_ANCHOR)
  const opensForm = targeted && canEdit
  const [answered, setAnswered] = useState(false)
  if (opensForm !== answered) {
    setAnswered(opensForm)
    if (opensForm && editing === null) setEditing('new')
  }
  const sectionRef = useRef<HTMLElement>(null)
  useScrollIntoViewWhenSettled(sectionRef, targeted && !permsLoading)

  const formOpen = canEdit && editing !== null
  // Asked only while the form is open, and only of a reader of SMS campaigns.
  const campaigns = useSmsCampaigns(formOpen && canViewSms)
  const brands = useBrands()
  const categories = useCategories()
  const brandOptions = useMemo(
    () => (brands.data ?? []).map((b) => ({ value: b.name, label: b.name })), [brands.data])
  const categoryOptions = useMemo(
    () => (categories.data ?? []).map((c) => ({ value: c.name, label: c.name })), [categories.data])

  const { upcoming, past, cancelled } = useMemo(
    () => splitPromotions(data?.promotions ?? [], today, showCancelled),
    [data, today, showCancelled],
  )

  const close = useCallback(() => setEditing(null), [])

  const submit = useCallback((body: PromotionIn) => {
    const id = editing !== null && editing !== 'new' ? editing.id : undefined
    save.mutate({ id, body }, {
      onSuccess: (saved) => {
        setEditing(null)
        addToast({ type: 'success', title: t('promo.saved'), message: saved.name, duration: 3000 })
      },
      // The API names the field and the rule; it is said as it came.
      onError: (e) => addToast({ type: 'error', title: t('promo.saveFailed'), message: e.message }),
    })
  }, [editing, save, addToast, t])

  const onEdit = useCallback((p: Promotion) => setEditing(p), [])

  const onCancel = useCallback((p: Promotion) => {
    setPendingId(p.id)
    cancel.mutate(p.id, {
      onSuccess: () => {
        // Its form would only be refused now (409).
        setEditing((e) => (e !== null && e !== 'new' && e.id === p.id ? null : e))
        addToast({ type: 'info', title: t('promo.cancelled', { name: p.name }), message: t('promo.cancelledHint') })
      },
      onError: (e) => addToast({ type: 'error', title: t('promo.cancelFailed'), message: e.message }),
      onSettled: () => setPendingId(null),
    })
  }, [cancel, addToast, t])

  const onRestore = useCallback((p: Promotion) => {
    setPendingId(p.id)
    restore.mutate(p.id, {
      onSuccess: () => addToast({ type: 'success', title: t('promo.restored', { name: p.name }), duration: 3000 }),
      onError: (e) => addToast({ type: 'error', title: t('promo.restoreFailed'), message: e.message }),
      onSettled: () => setPendingId(null),
    })
  }, [restore, addToast, t])

  const item = (p: Promotion) => (
    <PromotionItem
      key={p.id}
      promotion={p}
      today={today}
      canEdit={canEdit}
      busy={pendingId === p.id}
      onEdit={onEdit}
      onCancel={onCancel}
      onRestore={onRestore}
    />
  )

  return (
    <section
      id={PROMOTIONS_ANCHOR}
      ref={sectionRef}
      aria-label={t('promo.title')}
      // Below the sticky header when a link scrolls here.
      className="scroll-mt-20"
    >
      <Card>
        <CardHeader>
          <Wrapper dir="row" justify="between" align="start" gap="sm">
            <Wrapper dir="column" gap="xs">
              <CardTitle>{t('promo.title')}</CardTitle>
              <p className="text-xs text-slate-500">{t('promo.intro')}</p>
            </Wrapper>
            {canEdit && editing === null && (
              <Button size="sm" variant="primary" onClick={() => setEditing('new')}>
                <Plus className="h-3.5 w-3.5" aria-hidden />
                {t('promo.add')}
              </Button>
            )}
          </Wrapper>
        </CardHeader>
        <CardContent>
          <Wrapper dir="column" gap="xl">
            {formOpen && (
              <div className="rounded-lg border border-blue-200 bg-blue-50/40 p-3 sm:p-4">
                <PromotionForm
                  key={editing === 'new' ? 'new' : `${editing.id}:${editing.version}`}
                  promotion={editing === 'new' ? null : editing}
                  smsCampaigns={canViewSms ? campaigns.data?.campaigns ?? [] : undefined}
                  brandOptions={brandOptions}
                  categoryOptions={categoryOptions}
                  saving={save.isPending}
                  onSubmit={submit}
                  onClose={close}
                />
              </div>
            )}

            {isLoading ? (
              <SkeletonTable />
            ) : error ? (
              <ApiErrorState error={error} onRetry={() => void refetch()} title={t('promo.loadFailed')} />
            ) : (
              <>
                <ListBlock title={t('promo.upcoming')} empty={upcoming.length ? null : t('promo.noUpcoming')}>
                  {upcoming.map(item)}
                </ListBlock>
                <ListBlock
                  title={t('promo.past')}
                  hint={t('promo.pastHint')}
                  empty={past.length ? null : t('promo.noPast')}
                >
                  {past.map(item)}
                </ListBlock>
                {cancelled > 0 && (
                  <label className="inline-flex cursor-pointer items-center gap-2 text-xs text-slate-600">
                    <Checkbox size="sm" checked={showCancelled} onChange={setShowCancelled} />
                    {t('promo.showCancelled', { count: cancelled })}
                  </label>
                )}
              </>
            )}
          </Wrapper>
        </CardContent>
      </Card>
    </section>
  )
})
