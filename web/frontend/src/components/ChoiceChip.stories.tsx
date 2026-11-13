import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { ArrowLeftRight, Globe } from 'lucide-react'
import { ChoiceChip, type ChoiceOption } from './ChoiceChip'
import type { IconComponent } from './icons'

/**
 * One of a few, on a phone: the choice as a chip, the options in a bottom
 * sheet, each with a line on what it means. The site funnel uses it twice:
 * what the period is compared with, and whose visits it counts.
 */

type Scope = 'ua' | 'all'

const SCOPES: Array<ChoiceOption<Scope>> = [
  {
    value: 'ua',
    label: 'Только Украина',
    note: 'И те, чью страну Google Analytics не определил. Из-за границы заходят в основном боты: на 1–3 секунды и без покупок.',
  },
  { value: 'all', label: 'Все страны', note: 'Как в самом Google Analytics, вместе с ботами.' },
]
const SCOPE_CHIP: Record<Scope, string> = { ua: 'Украина', all: 'Все страны' }

type Against = 'previous' | 'year_ago'

const COMPARE: Array<ChoiceOption<Against>> = [
  { value: 'previous', label: 'с прошлым периодом', note: '21–31 окт. 2026, тоже 11 дней' },
  {
    value: 'year_ago',
    label: 'с прошлым годом',
    note: 'Год назад сравнить нельзя: данные есть с 4 сентября.',
    disabled: true,
  },
]

/** Stateful: the chip is controlled, and a frozen `value` could not be changed. */
function Live<T extends string>({ options, initial, chip, icon, sheetTitle }: {
  options: Array<ChoiceOption<T>>
  initial: T
  chip: (value: T) => string
  icon: IconComponent
  sheetTitle: string
}) {
  const [value, setValue] = useState<T>(initial)
  return (
    <ChoiceChip
      icon={icon}
      label={chip(value)}
      ariaLabel={`${sheetTitle}: ${chip(value)}`}
      sheetTitle={sheetTitle}
      closeLabel="Закрыть"
      options={options}
      value={value}
      onChange={setValue}
    />
  )
}

const meta = {
  title: 'Forms/ChoiceChip',
  component: ChoiceChip,
  // Every story renders through `Live`; these args only satisfy the type.
  args: {
    label: 'Украина',
    ariaLabel: 'Страна посетителей',
    sheetTitle: 'Чьи заходы считать',
    closeLabel: 'Закрыть',
    options: SCOPES,
    value: 'ua',
    onChange: () => {},
  },
  globals: { viewport: { value: 'mobile2', isRotated: false } },
} satisfies Meta<typeof ChoiceChip>

export default meta
type Story = StoryObj<typeof meta>

/** Whose visits the funnel counts. */
export const WhoseVisits: Story = {
  render: () => (
    <Live options={SCOPES} initial="ua" chip={(v) => SCOPE_CHIP[v]} icon={Globe} sheetTitle="Чьи заходы считать" />
  ),
}

/** What the period is compared with; a year ago is out of reach, and says why. */
export const WithADisabledChoice: Story = {
  render: () => (
    <Live
      options={COMPARE}
      initial="previous"
      chip={() => '21–31 окт.'}
      icon={ArrowLeftRight}
      sheetTitle="С чем сравнить"
    />
  ),
}
