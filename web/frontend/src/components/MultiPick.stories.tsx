import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { MultiPick, type PickOption } from './MultiPick'

/**
 * Several values out of a long list: a search box, the matching options
 * inline under it, and what is picked as removable chips above. The list is
 * drawn inline, not as a dropdown, because it lives inside a popover or a
 * bottom sheet — and a scroll container inside a scroll container is where a
 * thumb loses its place.
 *
 * Enter picks the only match; with `allowCustom` it adds what was typed.
 */

const BRANDS: PickOption[] = [
  'COSRX', 'Beauty of Joseon', 'Anua', 'Round Lab', 'SKIN1004', 'Medicube',
  'Torriden', 'Isntree', 'Purito', 'Klairs', 'Some By Mi', 'Laneige',
  'Innisfree', 'Missha', 'Etude', 'Dr. Jart+', 'Mixsoon', 'Axis-Y', 'Haruharu Wonder',
  'Numbuzin', 'Ma:nyo', 'Heimish', 'Benton', 'Pyunkang Yul',
].map((name) => ({ value: name, label: name }))

const CATEGORIES: PickOption[] = [
  { value: '1', label: 'Face care' },
  { value: '2', label: 'Cleansing' },
  { value: '3', label: 'Sun care' },
  { value: '4', label: 'Masks' },
  { value: '5', label: 'Hair' },
  { value: '6', label: 'Body' },
]

function Live({
  initial, ...rest
}: Omit<Parameters<typeof MultiPick>[0], 'values' | 'onChange'> & { initial: string[] }) {
  const [values, setValues] = useState(initial)
  return <MultiPick {...rest} values={values} onChange={setValues} />
}

const meta = {
  title: 'Forms/MultiPick',
  component: MultiPick,
  // Required props; every story renders through `Live`.
  args: {
    options: BRANDS,
    values: [],
    onChange: () => {},
    ariaLabel: 'Brands',
    placeholder: 'Search',
    emptyText: 'Nothing found',
  },
} satisfies Meta<typeof MultiPick>

export default meta
type Story = StoryObj<typeof meta>

/** Brands — hundreds in production — with two already picked. */
export const Brands: Story = {
  render: () => (
    <Live
      options={BRANDS}
      initial={['COSRX', 'Anua']}
      ariaLabel="Brands"
      placeholder="Search for a brand"
      emptyText="Nothing found"
      removeLabel={(name) => `Remove ${name}`}
    />
  ),
}

/**
 * Categories: the values are ids, the chips show names — `labelOf` maps one
 * to the other.
 */
export const Categories: Story = {
  render: () => (
    <Live
      options={CATEGORIES}
      initial={['3']}
      ariaLabel="Categories"
      placeholder="Search for a category"
      emptyText="Nothing found"
      removeLabel={(name) => `Remove ${name}`}
    />
  ),
}

/**
 * Cities are free text in KeyCRM and there is no list to offer, so
 * `allowCustom` turns what is typed into a value: type "Odesa" and press
 * Enter, or tap the "Add" row.
 */
export const CustomCities: Story = {
  render: () => (
    <Live
      options={[]}
      initial={['Київ', 'Львів']}
      allowCustom
      ariaLabel="City"
      placeholder="Type a city"
      addLabel={(value) => `Add «${value}»`}
      emptyText="Type a city and press Enter."
      removeLabel={(name) => `Remove ${name}`}
    />
  ),
}

/** Nothing picked yet: no chip row at all, only the box and the list. */
export const NothingPicked: Story = {
  render: () => (
    <Live
      options={BRANDS}
      initial={[]}
      ariaLabel="Brands"
      placeholder="Search for a brand"
      emptyText="Nothing found"
    />
  ),
}
