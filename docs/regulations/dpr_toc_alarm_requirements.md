# CA DPR (Title 22, Article 10, SBDDW-23-001) — TOC & Alarm Requirements

Source: `docs/regulations/CA_DPR_2023_SBDDW-23-001_reg_text.pdf` (downloaded from
[waterboards.ca.gov](https://www.waterboards.ca.gov/drinking_water/certlic/drinkingwater/docs/2023/method_15day_dpr_reg_text.pdf),
final reg text dated October 4, 2023, effective October 1, 2024). Plain-text copy at
`docs/regulations/dpr_reg_text.txt`. Citations below are `§64669.XX(subsection)`.

## Relevant definitions (§64669.05)

- **Critical limit**: a max/min value of a *continuously monitored* parameter indicating
  a treatment process is effectively controlling pathogen/chemical risk.
- **Chemical control point**: an activity/procedure/process essential for preventing,
  reducing, or eliminating a chemical hazard.
- **Surrogate parameter**: a measurable property correlated with an indicator
  compound/pathogen, used to monitor treatment efficacy or flag process failure.
- **TOC**: total organic carbon.
- **WWC** (wastewater contribution): fraction = municipal wastewater / (wastewater +
  dilution water), used to scale the TOC critical limit when DPR water is blended.

## §64669.50(h)–(j) — Reverse osmosis TOC tracking

- (h) DiPRRA must propose ≥1 surrogate/operational parameter for RO that is
  **continuously monitored, recorded, and has associated alarms** indicating membrane
  integrity compromise; must define the chemical control point and critical limit(s).
- (i) During full-scale operation: continuously monitor/record that parameter and log
  every critical-limit exceedance.
- (j) Track RO membrane TOC performance, report monthly:
  - **(j)(1)** Combined RO permeate TOC **> 0.15 mg/L continuously for > 120 hours** →
    investigate RO integrity, run a conductivity profile to find the
    underperforming vessel/element, take corrective action.
  - **(j)(2)** Combined RO permeate TOC **> 0.1 mg/L continuously for > 24 hours** →
    collect a grab sample and perform a 5-day total trihalomethane formation
    potential (TTHMFP) study. *(This is the rule the user cited.)*

## §64669.50(n) — TOC chemical control point (post-AOP, pre-distribution)

- Continuous TOC monitoring, recorded **≥ every 15 minutes**.
- TOC of wastewater origin **> 0.5 mg/L must be prevented from entering distribution**.
- Critical limit = **0.5 mg/L / WWC** (= 0.5 mg/L when WWC = 1, i.e. unblended).
  - (n)(1) Adjusted critical limit when blended with an approved source/finished water.
  - (n)(2) Reservoir mixing may temporarily raise the critical limit if justified by
    hydrodynamic modeling/tracer testing (independent advisory panel review required).
- **(n)(3)** Critical limit exceeded → **immediately discontinue delivery**; notify
  State Board and each receiving public water system **within 24 hours of knowledge**.
- **(n)(4)** TOC **> half the critical limit continuously for > 60 minutes** → evaluate
  treatment system, initiate source-control investigation, collect lab samples,
  report in the monthly compliance report.

## §64669.50(o) — Aggregate non-compliance

- If any chemical-control-point process pursuant to (e)/(h)/(k) critical limits is out
  of compliance for **> 10% of production time in a calendar month** → evaluate cause,
  take corrective action, summarize in monthly report.
- §64669.85(c): if that >10% condition persists for **> 2 consecutive months**, it is
  deemed a *chronic exposure threat* — water must be prevented from entering
  distribution.

## §64669.85(d) — SCADA / alarm system design requirements

The SCADA system must:
1. **(d)(1)** Alarm the operator when a pathogen or chemical control point is not
   operating as designed, and halt flow if necessary.
2. **(d)(2)** Identify **trending degradation and significant excursions** of water
   quality/surrogate/operational parameters that indicate a need for treatment
   adjustment or operator intervention, alert the operator, and **generate a record**.
   — *This clause is the regulatory hook for statistical trend/excursion detection
   (e.g. a rolling sign test), not just static threshold alarms.*
3. **(d)(3)** Interoperate with all DPR-project treatment plant SCADA systems.
4. **(d)(4)** Be physically/electronically secured.
5. **(d)(5)** Be tested per the approved operations plan protocol.

§64669.85(b): SCADA must auto-discontinue delivery within the flow-path travel time if
pathogen log-reduction or the §64669.50(n) TOC critical limit is exceeded.

## Summary: distinct TOC alarm/response conditions to implement

| # | Trigger | Threshold | Duration | Required response |
|---|---|---|---|---|
| 1 | RO permeate TOC | > 0.15 mg/L | continuous > 120 h | investigate + conductivity profile + corrective action (§64669.50(j)(1)) |
| 2 | RO permeate TOC | > 0.1 mg/L | continuous > 24 h | grab sample + 5-day TTHMFP study (§64669.50(j)(2)) |
| 3 | Post-AOP TOC (control point) | > critical limit (0.5/WWC mg/L) | any/immediate | discontinue delivery; notify State Board + water systems within 24 h (§64669.50(n)(3)) |
| 4 | Post-AOP TOC (control point) | > half critical limit | continuous > 60 min | evaluate + source-control investigation + sampling (§64669.50(n)(4)) |
| 5 | Any chemical control point | out of critical limit | > 10% of monthly production time | evaluate + corrective action, monthly report (§64669.50(o)) |
| 6 | Condition 5 | — | persists > 2 consecutive months | chronic exposure threat — block distribution (§64669.85(c)) |

All six are stated as **continuous-monitoring** requirements with **"continuously
exceeds for more than N hours/minutes"** language — i.e. sustained excursion above a
fixed threshold, not a statistical hypothesis test. The sign test's role (per the
earlier discussion) is to make the per-sample "is TOC above threshold right now?" call
robust to non-normal/noisy data, while the alarm logic itself tracks elapsed
continuous-exceedance time against the regulatory durations above (24h, 120h, 60min).
