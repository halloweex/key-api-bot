import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import i18n from '../../lib/i18n'
import { conclude, conversion, conversionChange, leak } from '../funnel'
import {
  convPct, countsSentence, fetchedAt, gapText, groupPct, perBaseText, sayConclusion, sayLeak, stepPct, wasText,
} from '../funnelText'
import type { T } from '../plural'
import { BY_TYPE, NOW, PREVIOUS, STEADY, YEAR_AGO } from '../../test/funnelFixtures'

const t = ((key: string, opts?: Record<string, unknown>) => i18n.t(key, opts)) as T
// Intl puts a no-break space before % and between thousands in Russian.
const plain = (s: string | null | undefined) => (s ?? '').replace(/[\u00a0\u202f]/g, ' ')

beforeEach(async () => { await i18n.changeLanguage('ru') })
afterEach(async () => { await i18n.changeLanguage('en') })

describe('numbers as the page writes them', () => {
  it('writes a step to one decimal under 20 %, whole above', () => {
    expect(plain(stepPct(2420 / 18700))).toBe('12,9 %')
    expect(plain(stepPct(0.4222))).toBe('42 %')
    expect(plain(stepPct(0.03))).toBe('3,0 %')
    expect(stepPct(null)).toBe('—')
  })

  it('writes the total to two decimals and a group share whole, or to one under 1 %', () => {
    expect(plain(convPct(0.0202))).toBe('2,02 %')
    expect(plain(groupPct(0.556))).toBe('56 %')
    expect(plain(groupPct(0.0048))).toBe('0,5 %')
  })

  it('says the total as so many of a base', () => {
    expect(plain(perBaseText(NOW, t))).toBe('из 1 000 зашедших купили 20')
  })
})

describe('the conclusion in words', () => {
  it('says the counts, the one step that fell and what it cost, then where people leave', () => {
    const said = sayConclusion(conclude(NOW, PREVIOUS, BY_TYPE, 'previous'), t)
    expect([said.headline.tone, plain(said.headline.text)])
      .toEqual(['neutral', 'Покупателей почти столько же, а зашедших на 13 % больше'])
    expect(said.lines.map((l) => [l.tone, l.lead ?? null, plain(l.text)])).toEqual([
      ['neutral', null, '690 покупателей против 700: прибавка посетителей почти ничего не купила.'],
      ['bad', 'Просел один шаг:', 'из корзины до оформления дошли 42 % вместо 48 % — это около 100 покупателей.'],
      ['neutral', null, 'Больше всего людей уходит между товаром и корзиной — 87 %, как и раньше.'],
    ])
  })

  it('says a rise in conversion, the step that moved the most buyers, and the mix', () => {
    const said = sayConclusion(conclude(NOW, YEAR_AGO, BY_TYPE, 'year_ago'), t)
    expect([said.headline.tone, plain(said.headline.text)]).toEqual(['good', 'Конверсия выросла: было 1,13 %, стало 2,03 %'])
    expect(said.lines.map((l) => plain(l.text))).toEqual([
      'Сильнее всего вырос шаг «товар → корзина»: было 8,4 %, стало 12,9 % — это около 240 дополнительных покупателей.',
      'Часть роста дал состав трафика: тёплых стало больше.',
      'Больше всего людей уходит между товаром и корзиной — 87 %.',
    ])
  })

  it('says a steady month is steady', () => {
    const said = sayConclusion(conclude(NOW, STEADY, BY_TYPE, 'previous'), t)
    expect(said.headline.text).toBe('Всё в пределах обычных колебаний')
    expect(said.lines.map((l) => plain(l.text))).toEqual([
      'Больше всего людей уходит между товаром и корзиной — 87 %, как и раньше.',
    ])
  })

  it('says only where people leave when there is nothing to compare with', () => {
    expect(plain(sayLeak(leak(NOW, null), t)?.text)).toBe('Больше всего людей уходит между товаром и корзиной — 87 %.')
  })

  it('joins counts that moved the same way without a "but"', () => {
    const more = { move: 'more' as const, ratio: 0.08 }
    expect(plain(countsSentence(more, { move: 'more', ratio: 0.13 }, t))).toBe('Покупателей на 8 % больше, зашедших на 13 % больше')
    expect(countsSentence({ move: 'same', ratio: 0 }, { move: 'same', ratio: 0.01 }, t)).toBe('Зашедших и покупателей почти столько же')
  })
})

describe('the rest of the block in words', () => {
  it('says the verdict beside "было" only when it was judged', () => {
    const change = conversionChange(NOW, PREVIOUS).change
    expect(plain(wasText(conversion(PREVIOUS), change, t, 'hero')?.text)).toBe('было 2,33 % — в пределах обычных колебаний')
    expect(plain(wasText(conversion(PREVIOUS), null, t, 'hero')?.text)).toBe('было 2,33 %')
    expect(wasText(conversion(PREVIOUS), 'worse', t, 'total')?.tone).toBe('bad')
    expect(wasText(null, 'worse', t, 'hero')).toBeNull()
  })

  it('says why the rows do not add up to the site', () => {
    expect(plain(gapText(200, 'type', t))).toBe(
      'Строки не складываются в «Весь сайт»: у 200 человек канал не определён, а человек с заходами из разных каналов считается в каждом.',
    )
    expect(plain(gapText(-35, 'device', t))).toBe(
      'Строки не складываются в «Весь сайт»: человек с разных устройств считается на каждом, поэтому в сумме на 35 больше.',
    )
    expect(gapText(0, 'channel', t)).toBeNull()
  })

  it('says when the figures on screen were fetched, in Kyiv', () => {
    // 08:42 UTC is 10:42 in Kyiv in November.
    expect(fetchedAt('2026-11-04T08:42:00Z', '2026-11-04')).toBe('10:42')
    expect(fetchedAt('2026-11-03T08:42:00Z', '2026-11-04')).toBe('3 ноября, 10:42')
  })
})
