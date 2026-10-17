import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { AudienceGroupCard } from './AudienceGroupCard'
import type { PickOption } from './MultiPick'
import type { Condition, ConditionGroup } from '../utils/smsConditions'
import type { SmsAudienceGroupShare } from '../types/api'

/**
 * One group of an audience: its conditions as sentences joined by a small
 * «and», and «+ Condition». An audience is one or more of these joined by OR.
 *
 * The card says what it brings once a preview has counted it — «2,612 people
 * · 1,025 only here» — and says so when it brings nobody, because a group
 * that empties itself is the one place a slip shows as a number.
 *
 * One condition is edited at a time. On desktop its inputs open under its
 * sentence and every change counts at once; on a phone they open in a bottom
 * sheet and the change counts when the sheet closes.
 *
 * Unlike `SmsAudienceForm`, the card is handed its brand and category lists,
 * so these stories need no query cache.
 */

const BRANDS: PickOption[] = [
  'COSRX', 'Beauty of Joseon', 'Anua', 'Round Lab', 'SKIN1004', 'Medicube',
  'Torriden', 'Isntree', 'Purito', 'Klairs',
].map((name) => ({ value: name, label: name }))

const CATEGORIES: PickOption[] = [
  { value: '1', label: 'Догляд за обличчям' },
  { value: '2', label: 'Очищення' },
  { value: '3', label: 'Сонцезахист' },
  { value: '4', label: 'Маски' },
]

// The owner's audience of 2026-10-17. Fixed ids, so a story can open one of
// them for editing.
const OWNER_GROUP_1: ConditionGroup = {
  id: 'g1',
  conditions: [{
    id: 'g1-orders', type: 'orders', op: 'atLeast', count: 2, countMax: null,
    period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' },
  }],
}

const OWNER_SHARE_1: SmsAudienceGroupShare = {
  group: 1, matched: 2_800, recipients: 2_612, onlyHere: 1_025,
}

function Live({
  initial, index = 0, share, single = false, phone = false, editing = null,
  removable = true,
}: {
  initial: ConditionGroup
  index?: number
  share?: SmsAudienceGroupShare
  single?: boolean
  phone?: boolean
  editing?: string | null
  removable?: boolean
}) {
  const [group, setGroup] = useState(initial)
  const [editingId, setEditingId] = useState<string | null>(editing)
  return (
    <AudienceGroupCard
      index={index}
      group={group}
      onChange={setGroup}
      onRemove={removable && !single ? () => {} : undefined}
      onDuplicate={group.conditions.length > 0 ? () => {} : undefined}
      share={share}
      single={single}
      editingId={editingId}
      onEditingChange={setEditingId}
      phone={phone}
      brandOptions={BRANDS}
      categoryOptions={CATEGORIES}
    />
  )
}

const meta = {
  title: 'SMS/AudienceGroupCard',
  component: AudienceGroupCard,
  // Required props; every story renders through `Live`, which holds the group
  // and which condition is open.
  args: {
    index: 0,
    group: OWNER_GROUP_1,
    onChange: () => {},
    single: false,
    editingId: null,
    onEditingChange: () => {},
    phone: false,
    brandOptions: BRANDS,
    categoryOptions: CATEGORIES,
  },
} satisfies Meta<typeof AudienceGroupCard>

export default meta
type Story = StoryObj<typeof meta>

/** The owner's first group, counted: 2+ orders between 1 Jan and 14 Oct 2026. */
export const Counted: Story = {
  render: () => <Live initial={OWNER_GROUP_1} share={OWNER_SHARE_1} />,
}

/** The same group with its condition open: the editor is drawn under the sentence. */
export const EditingOnDesktop: Story = {
  render: () => <Live initial={OWNER_GROUP_1} share={OWNER_SHARE_1} editing="g1-orders" />,
}

/**
 * The slip the owner made on 2026-10-17: 92 typed into the order count when
 * the 92 was days. The group matches nobody, the card says so, and the row
 * offers the one-tap fix — «It's 92 days» makes it 1+ order in the last 92
 * days. The hint is there whatever the count; with a zero share it says
 * «Nobody has 92 orders» rather than «more than almost anyone has».
 */
export const TheSlip92: Story = {
  render: () => (
    <Live
      initial={{
        id: 'g1',
        conditions: [{
          id: 'g1-orders', type: 'orders', op: 'atLeast', count: 92, countMax: null,
          period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' },
        }],
      }}
      share={{ group: 1, matched: 0, recipients: 0, onlyHere: 0 }}
    />
  ),
}

/**
 * An unfinished condition: «Bought» with no brand, category or code. The row
 * is marked and says it is not counted; the step it sits in will not move on
 * until it is filled in or removed.
 */
export const UnfinishedCondition: Story = {
  render: () => (
    <Live
      index={1}
      initial={{
        id: 'g2',
        conditions: [
          {
            id: 'g2-orders', type: 'orders', op: 'atLeast', count: 1, countMax: null,
            period: { kind: 'range', from: '2026-07-16', to: '2026-10-14' },
          },
          { id: 'g2-bought', type: 'bought', brands: [], categoryIds: [], promocode: '', withinDays: null },
        ],
      }}
      share={{ group: 2, matched: 3_300, recipients: 3_076, onlyHere: 1_489 }}
    />
  ),
}

/**
 * Matched, but nobody can be messaged: no valid phone, opted out, or outside
 * the chosen level. Different from "matches nobody", and said differently.
 */
export const MatchedButUnreachable: Story = {
  render: () => (
    <Live
      initial={{
        id: 'g1',
        conditions: [{ id: 'g1-city', type: 'city', cities: ['Ужгород'] }],
      }}
      share={{ group: 1, matched: 14, recipients: 0, onlyHere: 0 }}
    />
  ),
}

/** Several conditions, all of which must hold: the small «and» between them. */
export const SeveralConditions: Story = {
  render: () => {
    const conditions: Condition[] = [
      {
        id: 'c-orders', type: 'orders', op: 'atLeast', count: 2, countMax: null,
        period: { kind: 'all' },
      },
      { id: 'c-last', type: 'lastOrder', when: { kind: 'ago', min: 90, max: 180 } },
      { id: 'c-spent', type: 'spent', min: 5_000, max: null },
      {
        id: 'c-bought', type: 'bought', brands: ['COSRX', 'Anua'], categoryIds: [3],
        promocode: '', withinDays: 365,
      },
      { id: 'c-gender', type: 'gender', genders: ['f'], confidence: 'high' },
    ]
    return (
      <Live
        initial={{ id: 'g1', conditions }}
        share={{ group: 1, matched: 512, recipients: 488, onlyHere: 302 }}
      />
    )
  },
}

/**
 * The only group: no «remove», and the count is the audience's — "only here"
 * would say nothing.
 */
export const TheOnlyGroup: Story = {
  render: () => (
    <Live
      single
      initial={{
        id: 'g1',
        conditions: [{
          id: 'g1-orders', type: 'orders', op: 'atLeast', count: 1, countMax: null,
          period: { kind: 'range', from: '2026-07-16', to: '2026-10-14' },
        }],
      }}
      share={{ group: 1, matched: 3_300, recipients: 3_076, onlyHere: 3_076 }}
    />
  ),
}

/** An empty group is every buyer; it says so, and offers «+ Condition». */
export const Empty: Story = {
  render: () => <Live single initial={{ id: 'g1', conditions: [] }} />,
}

/**
 * On a phone the editor opens in a bottom sheet; the change counts when the
 * sheet closes, so the preview is not re-asked on every tap of a stepper.
 */
export const EditingOnAPhone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => <Live initial={OWNER_GROUP_1} share={OWNER_SHARE_1} phone editing="g1-orders" />,
}
