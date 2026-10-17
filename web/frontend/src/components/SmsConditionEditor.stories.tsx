import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { useTranslation } from 'react-i18next'
import { SmsConditionEditor } from './SmsConditionEditor'
import { Wrapper } from './Wrapper'
import type { PickOption } from './MultiPick'
import { conditionProblem, type Condition } from '../utils/smsConditions'
import { conditionSentence } from '../utils/smsConditionText'

/**
 * The inputs of one condition — what the blanks of its sentence are made of.
 * The same inputs under the sentence on desktop and inside a bottom sheet on
 * a phone, each with the native control that fits: a number pad with the unit
 * beside it, the phone's own date picker, a segmented choice, a searchable
 * list.
 *
 * Every story prints, under the editor, the sentence the row would show and
 * whether the condition is counted yet — the editor and the sentence are two
 * halves of one condition, and seeing them move together is the point.
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

function Live({ initial, siblings = [] }: { initial: Condition; siblings?: Condition[] }) {
  const { t } = useTranslation()
  const [condition, setCondition] = useState(initial)
  const categoryName = (id: number) => CATEGORIES.find((o) => o.value === String(id))?.label
  const { label, blanks } = conditionSentence(condition, t, { categoryName })
  const problem = conditionProblem(condition)
  return (
    <Wrapper dir="column" gap="lg">
      <SmsConditionEditor
        condition={condition}
        siblings={[...siblings, condition]}
        onChange={setCondition}
        brandOptions={BRANDS}
        categoryOptions={CATEGORIES}
      />
      <p className="border-t border-slate-100 pt-3 text-sm text-slate-700">
        {label} <span className="font-medium text-purple-800">{blanks.join(' ')}</span>
        <span className={`ml-2 text-xs ${problem ? 'text-amber-700' : 'text-slate-400'}`}>
          {problem ? t(`sms.cond.problem.${problem}`) : '· counted'}
        </span>
      </p>
    </Wrapper>
  )
}

const meta = {
  title: 'SMS/SmsConditionEditor',
  component: SmsConditionEditor,
  // Required props; every story renders through `Live`.
  args: {
    condition: { id: 'c', type: 'city', cities: [] },
    siblings: [],
    onChange: () => {},
    brandOptions: BRANDS,
    categoryOptions: CATEGORIES,
  },
} satisfies Meta<typeof SmsConditionEditor>

export default meta
type Story = StoryObj<typeof meta>

// ─── Orders ──────────────────────────────────────────────────────────────────

/**
 * Orders in a period — the owner's first group. How many, then when: the
 * lifetime count and the count in a period are one condition with a period
 * control, and "all time" is one of its values.
 */
export const Orders: Story = {
  render: () => (
    <Live initial={{
      id: 'c', type: 'orders', op: 'atLeast', count: 2, countMax: null,
      period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' },
    }} />
  ),
}

/** Over all time, between two counts. "None" is not offered here: everybody has ordered. */
export const OrdersBetweenOverAllTime: Story = {
  render: () => (
    <Live initial={{
      id: 'c', type: 'orders', op: 'between', count: 2, countMax: 4, period: { kind: 'all' },
    }} />
  ),
}

/** "No orders in the last 90 days" — "none" is offered only for a period. */
export const NoOrdersInAPeriod: Story = {
  render: () => (
    <Live initial={{
      id: 'c', type: 'orders', op: 'none', count: null, countMax: null,
      period: { kind: 'last', days: 90 },
    }} />
  ),
}

/**
 * A group holds one lifetime count and one period count. With the lifetime
 * one already in the group, this one can only be a period: "All time" is
 * disabled and a line says why.
 */
export const OrdersBesideALifetimeCount: Story = {
  render: () => (
    <Live
      siblings={[{
        id: 'sib', type: 'orders', op: 'atLeast', count: 2, countMax: null, period: { kind: 'all' },
      }]}
      initial={{
        id: 'c', type: 'orders', op: 'atLeast', count: 1, countMax: null,
        period: { kind: 'last', days: 90 },
      }}
    />
  ),
}

/** The owner's slip as the editor sees it: 92 is a valid count, so only the row's hint catches it. */
export const OrdersCount92: Story = {
  render: () => (
    <Live initial={{
      id: 'c', type: 'orders', op: 'atLeast', count: 92, countMax: null,
      period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' },
    }} />
  ),
}

// ─── When ────────────────────────────────────────────────────────────────────

/** Last order in the last 90 days — the shape a group's window always had. */
export const LastOrder: Story = {
  render: () => <Live initial={{ id: 'c', type: 'lastOrder', when: { kind: 'within', days: 90 } }} />,
}

/** "Quiet for three to six months": between 90 and 180 days ago. */
export const LastOrderBetweenDaysAgo: Story = {
  render: () => (
    <Live initial={{ id: 'c', type: 'lastOrder', when: { kind: 'ago', min: 90, max: 180 } }} />
  ),
}

/** Reversed bounds: shown as a problem, and the condition is not counted until fixed. */
export const LastOrderInverted: Story = {
  render: () => (
    <Live initial={{ id: 'c', type: 'lastOrder', when: { kind: 'ago', min: 180, max: 90 } }} />
  ),
}

/** First order between two dates — new customers of a season. */
export const FirstOrder: Story = {
  render: () => (
    <Live initial={{
      id: 'c', type: 'firstOrder', when: { kind: 'dates', from: '2026-07-16', to: '2026-10-14' },
    }} />
  ),
}

// ─── Money ───────────────────────────────────────────────────────────────────

/** Total spent: either bound may be left empty, not both. */
export const Spent: Story = {
  render: () => <Live initial={{ id: 'c', type: 'spent', min: 5_000, max: null }} />,
}

/** Average order between two amounts. */
export const AverageOrder: Story = {
  render: () => <Live initial={{ id: 'c', type: 'aov', min: 1_500, max: 4_000 }} />,
}

// ─── What they bought ────────────────────────────────────────────────────────

/**
 * Brand, category and promo code are one condition, never split: the server
 * asks all three of one order line.
 */
export const Bought: Story = {
  render: () => (
    <Live initial={{
      id: 'c', type: 'bought', brands: ['COSRX', 'Anua'], categoryIds: [3],
      promocode: '', withinDays: 180,
    }} />
  ),
}

/** Nothing picked yet — the problem line says it is not counted. */
export const BoughtUnfinished: Story = {
  render: () => (
    <Live initial={{
      id: 'c', type: 'bought', brands: [], categoryIds: [], promocode: '', withinDays: null,
    }} />
  ),
}

/**
 * Ordering channel. Beside a «bought» condition a note says the channel is
 * then checked on that same purchase, not on any order.
 */
export const Source: Story = {
  render: () => (
    <Live
      siblings={[{
        id: 'sib', type: 'bought', brands: ['Anua'], categoryIds: [], promocode: '', withinDays: null,
      }]}
      initial={{ id: 'c', type: 'source', sourceIds: [1, 4] }}
    />
  ),
}

// ─── Who they are ────────────────────────────────────────────────────────────

/** City: free text in KeyCRM, so what is typed becomes a value. */
export const City: Story = {
  render: () => <Live initial={{ id: 'c', type: 'city', cities: ['Київ', 'Львів'] }} />,
}

/**
 * Gender, inferred from the name, with how far to trust the inference.
 * Customers whose gender could not be decided never enter a gendered audience.
 */
export const Gender: Story = {
  render: () => (
    <Live initial={{ id: 'c', type: 'gender', genders: ['f'], confidence: 'high' }} />
  ),
}
