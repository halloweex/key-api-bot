/**
 * The smallest lift a campaign could detect — the number that decides whether
 * it is worth splitting an audience at all.
 *
 * Precision comes from the smaller arm, not from the size of the send, and
 * that is the one fact this page keeps having to prove. At a 10% holdout an
 * audience of 1 500 cannot see anything under 5.9 pp; at 30% it sees 2.7. The
 * August campaign measured +2.05 pp against a threshold of 1.92 — it cleared
 * by 0.13, and a slightly weaker offer would have come back "nothing proven"
 * on a real effect.
 *
 * Two-proportion normal approximation at 80% power, α = 5% two-sided. Good
 * enough for the decision it informs; the arms here are thousands of people
 * and the baseline is around 1%.
 */

/** Baseline conversion, from the August campaign's control arm: 7/646. */
export const BASELINE_CONVERSION = 0.0108

const Z_ALPHA_TWO_SIDED = 1.959963985   // α = .05
const Z_POWER = 0.841621234             // 80%

/** Holm's first-step α when several arms are compared at once. */
export function alphaFor(arms: number): number {
  return 0.05 / Math.max(1, arms)
}

function zFor(alpha: number): number {
  // Only the two α values this page uses are needed; anything else falls back
  // to the uncorrected quantile rather than pretending to a general inverse.
  if (Math.abs(alpha - 0.05) < 1e-9) return Z_ALPHA_TWO_SIDED
  if (Math.abs(alpha - 0.05 / 2) < 1e-9) return 2.241402728
  if (Math.abs(alpha - 0.05 / 3) < 1e-9) return 2.394034
  if (Math.abs(alpha - 0.05 / 4) < 1e-9) return 2.497705
  return Z_ALPHA_TWO_SIDED
}

/**
 * Minimum detectable lift in percentage points.
 *
 * Returns null when either arm is empty — there is no threshold to state, and
 * a made-up number would be worse than an honest blank.
 */
export function mdePercentagePoints(
  target: number,
  control: number,
  { baseline = BASELINE_CONVERSION, alpha = 0.05 } = {},
): number | null {
  if (!(target > 0) || !(control > 0)) return null

  const z = zFor(alpha) + Z_POWER
  let delta = 0.005
  for (let i = 0; i < 200; i++) {
    const treated = baseline + delta
    const pooled = (baseline * control + treated * target) / (target + control)
    const se = Math.sqrt(pooled * (1 - pooled) * (1 / target + 1 / control))
    const next = z * se
    if (Math.abs(next - delta) < 1e-9) break
    delta = next
  }
  return delta * 100
}

/**
 * How to read a threshold, in the terms the decision is made in.
 *
 * The August campaign moved conversion by ~2 pp, so a threshold above that is
 * a campaign that will send and prove nothing — which is worth saying before
 * the money is spent, not after.
 */
export type MdeVerdict = 'good' | 'tight' | 'hopeless'

export function verdictFor(mde: number | null): MdeVerdict | null {
  if (mde == null) return null
  if (mde <= 1.5) return 'good'
  if (mde <= 3) return 'tight'
  return 'hopeless'
}
