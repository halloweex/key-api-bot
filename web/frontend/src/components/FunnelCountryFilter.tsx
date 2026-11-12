import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { Globe } from 'lucide-react'
import { ChoiceChip } from './ChoiceChip'
import { SegmentedControl } from './SegmentedControl'
import { useIsPhone } from '../hooks/useMediaQuery'
import { useFilterStore } from '../store/filterStore'
import type { FunnelCountry } from '../types/filters'

// ─── Whose visits the site funnel counts ─────────────────────────────────────
//
// Ukraine by default, with the visitors Google Analytics could not place:
// visits from abroad are mostly bots. On 1–10 November 2026 they were 30 % of
// the people entering and bought 0.4 % of the time; the measurement is in
// `web/services/ga4_funnel_service.py`. "All countries" brings them back.
//
// It sits beside the period in the line under the /traffic sections, not in
// the header's filter bar. The bar shows the funnel the period alone, and a
// row of its own there would push «Итог» off a phone's first screen. A chip
// with a sheet on a phone, as the funnel's comparison; two segments on a
// wide screen.

const COUNTRIES: readonly FunnelCountry[] = ['ua', 'all']

export const FunnelCountryFilter = memo(function FunnelCountryFilter() {
  const { t } = useTranslation()
  const phone = useIsPhone()
  const country = useFilterStore((s) => s.funnelCountry)
  const setCountry = useFilterStore((s) => s.setFunnelCountry)
  const short = (value: FunnelCountry) => t(`traffic.funnel.country.${value}`)

  if (phone) {
    return (
      <ChoiceChip<FunnelCountry>
        icon={Globe}
        label={short(country)}
        ariaLabel={t('traffic.funnel.countryChip', { label: short(country) })}
        sheetTitle={t('traffic.funnel.countrySheetTitle')}
        closeLabel={t('traffic.close')}
        value={country}
        options={COUNTRIES.map((value) => ({
          value,
          label: t(`traffic.funnel.countryOption.${value}`),
          note: t(`traffic.funnel.countryNote.${value}`),
        }))}
        onChange={setCountry}
      />
    )
  }
  return (
    <SegmentedControl<FunnelCountry>
      options={COUNTRIES.map((value) => ({ value, label: short(value) }))}
      value={country}
      onChange={setCountry}
      size="sm"
      ariaLabel={t('traffic.funnel.countryLabel')}
    />
  )
})
