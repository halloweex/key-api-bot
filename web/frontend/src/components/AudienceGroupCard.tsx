import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import {
  Banknote, CalendarClock, Copy, MapPin, Receipt, ShoppingBag, Store, Tag, Trash2,
  UserPlus, VenusAndMars,
} from 'lucide-react'
import { AddConditionMenu, type ConditionMenuFamily } from './AddConditionMenu'
import { BottomSheet } from './BottomSheet'
import { Button } from './Button'
import { ConditionRow } from './ConditionRow'
import { SmsConditionEditor } from './SmsConditionEditor'
import { Wrapper } from './Wrapper'
import type { PickOption } from './MultiPick'
import type { IconComponent } from './icons'
import {
  allowedPeriodKinds, canAdd, CONDITION_FAMILIES, conditionProblem, moveCountIntoPeriod,
  newCondition, suspiciousCount, type Condition, type ConditionGroup, type ConditionType,
} from '../utils/smsConditions'
import { conditionSentence, plural } from '../utils/smsConditionText'
import type { GroupDiagnosis } from '../utils/smsGroupDiagnosis'
import { formatNumber } from '../utils/formatters'
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'
import type { SmsAudienceGroupShare } from '../types/api'

// ─── AudienceGroupCard ───────────────────────────────────────────────────────
//
// One group of an audience: «Группа 1 · клиент подходит, если верно всё»,
// its conditions as sentences joined by a small «и», and «+ Условие». An
// audience is one or more of these, joined by ИЛИ; a customer is in it if
// they match any group.
//
// The card says what it brings — «2 612 человек · 1 035 только здесь» — once
// a preview has counted it, and says so when it brings nobody, because a
// group that empties itself is the one place a slip shows as a number. And
// it says WHICH condition did it, once the caller has asked
// (`utils/smsGroupDiagnosis.ts`): «Никого: условие «Сделал(а) 92+ заказа за
// всё время» не подходит ни одному клиенту».
//
// One condition is edited at a time, across the whole audience (the caller
// holds which). On desktop its inputs open under its sentence and every
// change counts at once; on a phone they open in a bottom sheet and the
// change counts when the sheet closes, so the count is not re-asked on every
// tap of a stepper.

const ICONS: Record<ConditionType, IconComponent> = {
  orders: ShoppingBag,
  lastOrder: CalendarClock,
  firstOrder: UserPlus,
  spent: Banknote,
  aov: Receipt,
  bought: Tag,
  source: Store,
  city: MapPin,
  gender: VenusAndMars,
}

export function AudienceGroupCard({
  index,
  group,
  onChange,
  onRemove,
  onDuplicate,
  share,
  single,
  editingId,
  onEditingChange,
  phone,
  brandOptions,
  categoryOptions,
  diagnosis,
}: {
  /** 0-based; the card is titled with it, counted from one. */
  index: number
  group: ConditionGroup
  onChange: (next: ConditionGroup) => void
  /** Absent for a group that cannot be removed — the only one. */
  onRemove?: () => void
  /** Absent once the audience holds as many groups as it may. */
  onDuplicate?: () => void
  /** This group's part of the audience, from the latest preview. */
  share?: SmsAudienceGroupShare
  /** The only group: "only here" would say nothing. */
  single: boolean
  editingId: string | null
  onEditingChange: (id: string | null) => void
  phone: boolean
  brandOptions: PickOption[]
  categoryOptions: PickOption[]
  /** Which condition brings the group to nobody, when it does: being asked,
   *  answered, or null when no one condition explains it. */
  diagnosis?: GroupDiagnosis | 'pending' | null
}) {
  const { t } = useTranslation()
  // On a phone the sheet edits a copy; it becomes the condition when the
  // sheet closes.
  const [sheetDraft, setSheetDraft] = useState<Condition | null>(null)
  const categoryName = (id: number) => categoryOptions.find((o) => o.value === String(id))?.label

  const editing = group.conditions.find((c) => c.id === editingId) ?? null
  const sheetCondition = phone && editing
    ? (sheetDraft && sheetDraft.id === editing.id ? sheetDraft : editing)
    : null

  function replace(next: Condition) {
    onChange({ ...group, conditions: group.conditions.map((c) => (c.id === next.id ? next : c)) })
  }

  function remove(id: string) {
    if (editingId === id) onEditingChange(null)
    onChange({ ...group, conditions: group.conditions.filter((c) => c.id !== id) })
  }

  function add(type: string) {
    const c = newCondition(type as ConditionType, group.conditions)
    onChange({ ...group, conditions: [...group.conditions, c] })
    setSheetDraft(null)
    onEditingChange(c.id)
  }

  function closeSheet() {
    if (sheetCondition) replace(sheetCondition)
    setSheetDraft(null)
    onEditingChange(null)
  }

  const families: ConditionMenuFamily[] = CONDITION_FAMILIES.map((family) => ({
    title: t(`sms.cond.family.${family.key}`),
    items: family.types.map((type) => ({
      key: type,
      icon: ICONS[type],
      label: t(`sms.cond.menu.${type}`),
      example: t(`sms.cond.example.${type}`),
      disabled: !canAdd(type, group.conditions),
      disabledNote: t(type === 'orders' ? 'sms.cond.menu.ordersTaken' : 'sms.cond.menu.taken'),
    })),
  }))

  const title = t('sms.groupTitle', { n: index + 1 })

  // A condition named in a sentence of its own: «Сделал(а) 92+ заказа за всё время».
  const named = (id: string) => {
    const c = group.conditions.find((x) => x.id === id)
    if (!c) return '…'
    const { label, blanks } = conditionSentence(c, t, { categoryName })
    return [label, ...blanks].join(' ')
  }
  const listOf = (ids: string[]) => new Intl.ListFormat(
    LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US', { type: 'conjunction' },
  ).format(ids.map((id) => t('sms.groupWhy.quoted', { condition: named(id) })))
  const people = (n: number) => ({ n: formatNumber(n), word: plural(t, 'word.people', n) })
  const why = (() => {
    if (!share || !diagnosis) return null
    if (diagnosis === 'pending') return t('sms.groupWhy.checking')
    switch (diagnosis.kind) {
      case 'alone': return t('sms.groupWhy.alone', { condition: named(diagnosis.conditionId) })
      case 'cuts':
        return t('sms.groupWhy.cuts', {
          condition: named(diagnosis.conditionId), ...people(diagnosis.without),
        })
      case 'clash': return t('sms.groupWhy.clash', { conditions: listOf(diagnosis.conditionIds) })
      case 'neither':
        return t('sms.groupWhy.neither', {
          a: named(diagnosis.conditionIds[0]), b: named(diagnosis.conditionIds[1]),
        })
      case 'together': return t('sms.groupWhy.together')
      case 'level':
        return t('sms.groupWhy.level', {
          matched: formatNumber(share.matched), ...people(diagnosis.without),
        })
      case 'reach':
        return t('sms.groupWhy.reach', {
          matched: formatNumber(share.matched), condition: named(diagnosis.conditionId),
          ...people(diagnosis.without),
        })
    }
  })()

  // What is wrong with a condition, said the same way under its row and inside
  // its sheet — a phone edits in the sheet and must not lose the warning there.
  const problemFor = (c: Condition) => {
    const problem = conditionProblem(c)
    return problem ? t(`sms.cond.problem.${problem}`) : null
  }
  const hintFor = (c: Condition, siblings: Condition[]) => {
    if (c.type !== 'orders' || !suspiciousCount(c)) return undefined
    const n = c.count ?? 0
    // The one-tap fix moves the number into the period — only where the group
    // has no period yet: a second one would be refused by the server, or
    // silently replace the first.
    const canMove = allowedPeriodKinds(c, siblings).includes('last')
    return (
      <Wrapper dir="row" gap="sm" align="center" wrap>
        <span className="text-xs text-amber-800">
          {share && share.matched === 0
            ? t('sms.cond.manyOrdersNobody', { n, days: plural(t, 'word.days', n) })
            : t('sms.cond.manyOrders', {
                n, orders: plural(t, 'word.orders', n), days: plural(t, 'word.days', n),
              })}
        </span>
        {canMove && (
          <Button size="sm" variant="secondary" onClick={() => replace(moveCountIntoPeriod(c))}>
            {t('sms.cond.makeItDays', { n, days: plural(t, 'word.days', n) })}
          </Button>
        )}
      </Wrapper>
    )
  }

  return (
    <section
      aria-label={title}
      className="rounded-xl border border-slate-200 bg-slate-50/60 p-3 sm:p-4"
    >
      <Wrapper dir="column" gap="md">
        <Wrapper dir="row" gap="sm" align="start" justify="between">
          <div className="min-w-0">
            <h3 className="text-sm font-semibold text-slate-800">
              {title}
              <span className="font-normal text-slate-500"> · {t('sms.group.allMustHold')}</span>
            </h3>
            {share && share.matched === 0 && (!why || diagnosis === 'pending') && (
              <p className="mt-0.5 text-xs font-medium text-amber-700">{t('sms.groupMatchesNobody')}</p>
            )}
            {share && share.matched > 0 && share.recipients === 0 && (!why || diagnosis === 'pending') && (
              <p className="mt-0.5 text-xs text-amber-700">
                {t('sms.groupUnreachable', { matched: formatNumber(share.matched) })}
              </p>
            )}
            {why && (
              <p className={`mt-0.5 text-xs text-amber-700 ${diagnosis === 'pending' ? '' : 'font-medium'}`}
                 aria-live="polite">
                {why}
              </p>
            )}
            {share && share.recipients > 0 && (
              <p className="mt-0.5 text-xs text-slate-600 tabular-nums">
                {single
                  ? t('sms.group.count', {
                      n: formatNumber(share.recipients),
                      word: plural(t, 'word.people', share.recipients),
                    })
                  : t('sms.groupShare', {
                      recipients: formatNumber(share.recipients),
                      word: plural(t, 'word.people', share.recipients),
                      only: formatNumber(share.onlyHere),
                    })}
              </p>
            )}
          </div>
          <Wrapper dir="row" gap="xs">
            {onDuplicate && (
              <Button variant="ghost" size="sm" onClick={onDuplicate}
                      aria-label={t('sms.group.duplicate', { n: index + 1 })}>
                <Copy className="h-3.5 w-3.5" aria-hidden />
                {!phone && t('sms.group.duplicateShort')}
              </Button>
            )}
            {onRemove && (
              <Button variant="ghost" size="sm" onClick={onRemove}
                      aria-label={t('sms.groupRemove', { n: index + 1 })}>
                <Trash2 className="h-3.5 w-3.5" aria-hidden />
                {!phone && t('sms.group.removeShort')}
              </Button>
            )}
          </Wrapper>
        </Wrapper>

        {group.conditions.length === 0 && (
          <p className="text-xs text-slate-500">{t('sms.group.empty')}</p>
        )}

        {group.conditions.length > 0 && (
          <ol className="flex flex-col">
            {group.conditions.map((c, i) => {
              const { label, blanks } = conditionSentence(c, t, { categoryName })
              const isEditing = editingId === c.id
              return (
                <li key={c.id} className="flex flex-col">
                  {i > 0 && (
                    <span className="py-1 pl-7 text-[11px] font-semibold uppercase tracking-wide
                                     text-slate-400" aria-hidden>
                      {t('sms.group.and')}
                    </span>
                  )}
                  <ConditionRow
                    icon={ICONS[c.type]}
                    label={label}
                    blanks={blanks}
                    editing={isEditing}
                    onEdit={() => {
                      setSheetDraft(null)
                      onEditingChange(isEditing && !phone ? null : c.id)
                    }}
                    onDone={() => onEditingChange(null)}
                    onRemove={() => remove(c.id)}
                    removeLabel={t('sms.cond.remove', { what: label })}
                    editLabel={(b) => t('sms.cond.change', { what: b })}
                    doneLabel={t('sms.cond.done')}
                    problem={problemFor(c)}
                    hint={hintFor(c, group.conditions)}
                  >
                    {!phone && isEditing && (
                      <SmsConditionEditor
                        condition={c}
                        siblings={group.conditions}
                        onChange={replace}
                        brandOptions={brandOptions}
                        categoryOptions={categoryOptions}
                      />
                    )}
                  </ConditionRow>
                </li>
              )
            })}
          </ol>
        )}

        <div>
          <AddConditionMenu
            families={families}
            onPick={add}
            triggerLabel={t('sms.cond.add')}
            menuTitle={t('sms.cond.addTitle')}
            phone={phone}
            closeLabel={t('sms.cond.close')}
          />
        </div>
      </Wrapper>

      {sheetCondition && (
        <BottomSheet
          open
          title={conditionSentence(sheetCondition, t, { categoryName }).label}
          onClose={closeSheet}
          closeLabel={t('sms.cond.close')}
          footer={<Button fullWidth variant="primary" onClick={closeSheet}>{t('sms.cond.done')}</Button>}
        >
          <Wrapper dir="column" gap="md">
            <SmsConditionEditor
              condition={sheetCondition}
              siblings={group.conditions}
              onChange={setSheetDraft}
              brandOptions={brandOptions}
              categoryOptions={categoryOptions}
            />
            {problemFor(sheetCondition) && (
              <p className="text-xs text-amber-800">{problemFor(sheetCondition)}</p>
            )}
            {sheetCondition.type === 'orders' && suspiciousCount(sheetCondition) && (
              <p className="text-xs text-amber-800">
                {t('sms.cond.manyOrders', {
                  n: sheetCondition.count,
                  orders: plural(t, 'word.orders', sheetCondition.count ?? 0),
                  days: plural(t, 'word.days', sheetCondition.count ?? 0),
                })}
              </p>
            )}
          </Wrapper>
        </BottomSheet>
      )}
    </section>
  )
}
