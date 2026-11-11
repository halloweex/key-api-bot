import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import i18n from '../../lib/i18n'
import { conclude, conversion, conversionChange } from '../funnel'
import {
  convPct, countsSentence, fetchedAt, gapText, groupPct, heroWas, perBaseText, sayConclusion, stepPct,
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
  it('says the counts, then the one step that fell and what it cost', () => {
    const said = sayConclusion(conclude(NOW, PREVIOUS, BY_TYPE, 'previous'), t)
    expect(said.map((l) => [l.tone, l.lead ?? null, plain(l.text)])).toEqual([
      ['neutral', null, 'Покупателей почти столько же, а зашедших на 13 % больше.'],
      ['bad', 'Просел один шаг:', 'корзина → оформление — минус около 100 покупателей.'],
    ])
  })

  it('says the step that moved the most buyers and the mix as shares, not the conversion again', () => {
    const said = sayConclusion(conclude(NOW, YEAR_AGO, BY_TYPE, 'year_ago'), t)
    expect(said.map((l) => plain(l.text))).toEqual([
      'Сильнее всего вырос шаг «товар → корзина» — плюс около 240 покупателей.',
      // Warm visitors were 20 000 a year ago and are 19 300 now: the sentence
      // says their share, which is what grew. «Тёплых стало больше» was false.
      'Часть роста дал состав трафика: доля тёплых выросла с 33 % до 57 %.',
    ])
  })

  it('says nothing about a steady month: the verdict beside the number says it', () => {
    expect(sayConclusion(conclude(NOW, STEADY, BY_TYPE, 'previous'), t)).toEqual([])
  })

  it('joins counts that moved the same way without a "but"', () => {
    const more = { move: 'more' as const, ratio: 0.08 }
    expect(plain(countsSentence(more, { move: 'more', ratio: 0.13 }, t))).toBe('Покупателей на 8 % больше, зашедших на 13 % больше')
    expect(countsSentence({ move: 'same', ratio: 0 }, { move: 'same', ratio: 0.01 }, t)).toBe('Зашедших и покупателей почти столько же')
  })
})

describe('the rest of the block in words', () => {
  it('sets "было" beside the number, and the verdict only when it was judged', () => {
    const change = conversionChange(NOW, PREVIOUS).change
    const was = heroWas(conversion(PREVIOUS), change, t)
    expect([plain(was?.was), was?.verdict, was?.tone]).toEqual(['было 2,33 %', 'в пределах колебаний', 'neutral'])
    expect(heroWas(conversion(PREVIOUS), null, t)).toMatchObject({ verdict: null, tone: 'neutral' })
    expect(heroWas(conversion(PREVIOUS), 'worse', t)).toMatchObject({ verdict: 'хуже обычного', tone: 'bad' })
    expect(heroWas(null, 'worse', t)).toBeNull()
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
