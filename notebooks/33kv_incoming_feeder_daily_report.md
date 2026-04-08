---
output:
  html_document: default
  word_document: default
  pdf_document: default
---
# 33kV Incoming Feeder Daily Analysis Report

## Scope

This report summarizes the analysis built in [33kv_incoming_feeder_daily_long_transform.ipynb](/f:/Secure/CashFlowMgmt/notebooks/33kv_incoming_feeder_daily_long_transform.ipynb).

Primary analysis window:

- `2025-04-01` to `2026-03-31`

Primary datasets used:

- feeder daily energy data reshaped from `20260407_33kVIncomingFeederDailyDataWide.xlsx`
- recharge data summarized from `20260407_RechargeToDate.csv`

Primary processed outputs:

- `data/processed/33kv_incoming_feeder_daily_long.parquet`
- `data/processed/vend_amounts_daily.parquet`

Important note:

- the combined daily summary file includes earlier recharge dates outside the feeder period because the energy and recharge series were outer-joined
- the notebook analysis itself correctly filters to `2025-04-01` through `2026-03-31` for the weekday and special-day sections

## Headline Findings

- Average daily feeder energy is very stable across weekdays, staying in a narrow band of roughly `11.59M` to `11.78M kWh`.
- Average daily vend amount is much more uneven than energy, with a strong Monday effect and a clear weekend drop.
- Average `Rs/kWh` is highest on Monday at about `2.66` and falls steadily toward the weekend, reaching about `0.71` on Sunday.
- Variability is materially lower for energy than for the money-based measures. Overall CV is about `0.22` for `kWh`, versus about `0.63` for `Rs` and `0.62` for `Rs/kWh`.
- In the special-day comparison, `Mon Excluding Weekday After Public Holiday` has the highest average vend amount and the highest average `Rs/kWh`.
- `Weekends` have the lowest average vend amount and among the lowest average `Rs/kWh`.
- `Weekday After Public Holiday` has the highest CV for `Rs/kWh`, suggesting post-holiday recharge intensity is less predictable than normal weekday behavior.

## Weekday Summary

Overall weekday-level averages and variability from the saved summary workbook:

| Day | Avg kWh | StdDev kWh | Avg vend_amount | StdDev vend_amount | Avg Rs/kWh | StdDev Rs/kWh | CV kWh | CV Rs | CV Rs/kWh |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Monday | 11,681,703 | 2,540,128 | 30,780,013 | 15,665,479 | 2.656 | 1.281 | 0.217 | 0.509 | 0.482 |
| Tuesday | 11,781,846 | 2,573,307 | 23,514,814 | 12,490,391 | 2.073 | 1.161 | 0.218 | 0.531 | 0.560 |
| Wednesday | 11,645,852 | 2,453,609 | 19,897,364 | 8,525,622 | 1.746 | 0.751 | 0.211 | 0.428 | 0.430 |
| Thursday | 11,590,076 | 2,613,811 | 18,539,450 | 7,982,094 | 1.618 | 0.633 | 0.226 | 0.431 | 0.391 |
| Friday | 11,781,144 | 2,610,837 | 19,517,346 | 8,327,925 | 1.691 | 0.683 | 0.222 | 0.427 | 0.404 |
| Saturday | 11,698,460 | 2,743,752 | 10,617,199 | 5,005,672 | 0.914 | 0.381 | 0.235 | 0.471 | 0.417 |
| Sunday | 11,608,262 | 2,444,689 | 8,299,721 | 3,821,874 | 0.713 | 0.282 | 0.211 | 0.460 | 0.396 |
| Overall | 11,684,174 | 2,550,095 | 18,751,074 | 11,847,590 | 1.631 | 1.015 | 0.218 | 0.632 | 0.622 |

Interpretation:

- Energy is operationally steady across the week.
- Recharge value is behaviorally front-loaded, with Monday materially above all other days.
- Weekend recharge softness is large enough that it pulls down both `vend_amount` and `Rs/kWh`, even though energy stays broadly flat.


## Special-Day Summary

Special-day group summary from the revised grouping, where the post-holiday bucket is restricted to weekdays only:

| Group | Days | Avg kWh | StdDev kWh | Avg vend_amount | StdDev vend_amount | Avg Rs/kWh | StdDev Rs/kWh | CV kWh | CV Rs | CV Rs/kWh |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Overall | 365 | 11,684,174 | 2,550,095 | 18,751,074 | 11,847,590 | 1.631 | 1.015 | 0.218 | 0.632 | 0.622 |
| Weekends | 104 | 11,653,361 | 2,586,279 | 9,458,460 | 4,582,020 | 0.813 | 0.348 | 0.222 | 0.484 | 0.428 |
| Weekday After Public Holiday | 14 | 10,719,746 | 1,716,537 | 24,046,202 | 14,342,328 | 2.268 | 1.428 | 0.160 | 0.596 | 0.630 |
| Mon Excluding Weekday After Public Holiday | 48 | 11,725,874 | 2,529,814 | 31,324,385 | 15,980,766 | 2.688 | 1.299 | 0.216 | 0.510 | 0.483 |
| Tue-Thu Excluding Weekday After Public Holiday | 150 | 11,738,406 | 2,562,232 | 20,659,214 | 9,687,979 | 1.803 | 0.843 | 0.218 | 0.469 | 0.467 |
| Fri Excluding Weekday After Public Holiday | 49 | 11,818,262 | 2,686,068 | 18,803,322 | 7,584,098 | 1.624 | 0.600 | 0.227 | 0.403 | 0.369 |

Interpretation:

- `Mon Excluding Weekday After Public Holiday` is the strongest recharge bucket in both absolute `vend_amount` and `Rs/kWh`.
- `Weekends` remain the weakest recharge bucket while energy usage stays close to the overall level.
- `Weekday After Public Holiday` is not the highest average recharge bucket, but it remains the most unstable in price-per-energy terms, with the highest `CV of Rs/kWh` among the comparison groups.
- `Fri Excluding Weekday After Public Holiday` has the lowest financial CVs among the special weekday groups, which may make it the cleanest baseline comparison bucket.

## Charts

### Weekday Profile

This chart is the clearest summary of average weekday behavior and dispersion:

![Weekday profile](../data/33kv_incoming_feeder_weekday_boxplots.png)

### Weekday CV Comparison

This chart makes the difference in relative volatility easy to see:

![Weekday CV comparison](../data/33kv_incoming_feeder_weekday_cv_comparison.png)

### Daily Trend

This is the best overall view for seasonality and co-movement:

![Daily kWh and vend trend](../data/33kv_incoming_feeder_kwh_vend_trend.png)

### Special-Day Boxplots

These keep the full distributional information for the grouped comparisons:

![Special-day boxplots](../data/33kv_incoming_feeder_special_day_boxplots.png)

### Special-Day CV Comparison

This is the quickest view for comparing predictability across the special-day buckets:

![Special-day CV comparison](../data/33kv_incoming_feeder_special_day_cv_comparison.png)


## Practical Conclusions

- The feeder energy series is operationally steady; the stronger movement is in recharge behavior rather than underlying energy volume.
- Monday is the most commercially intense day in the week, while weekends are materially softer.
- Relative volatility is consistently higher for `Rs` and `Rs/kWh` than for `kWh`, so commercial behavior is noisier than physical load.
- `Weekday After Public Holiday` should be treated as a distinct operating condition because its `Rs/kWh` behavior is less stable than ordinary weekdays.
- For baseline weekday comparisons, `Fri Excluding Weekday After Public Holiday` and `Tue-Thu Excluding Weekday After Public Holiday` look cleaner than using all weekdays together.

## Caveats

- Bihar holiday dates in the notebook are suitable for analysis grouping, but some festival observances can shift slightly in practice.
- Recharge history extends earlier than the feeder energy history, so any cross-series comparison should stay inside the notebook's filtered analysis window.
- `Rs/kWh` here is an observed daily ratio of summarized vend amount to summarized feeder energy; it is useful analytically, but it should not be treated as a tariff.

## Appendix: Alternative Visuals

These figures are useful supporting views for deeper review.

### Monthly Profile

![Monthly profile](../data/33kv_incoming_feeder_monthly_profile.png)

### Indexed Trend

![Indexed trend](../data/33kv_incoming_feeder_indexed_trend.png)

### Weekday-Month Heatmap

![Weekday-month heatmap](../data/33kv_incoming_feeder_weekday_month_heatmap.png)

### kWh vs Vend Scatter

![kWh vs vend scatter](../data/33kv_incoming_feeder_kwh_vs_vend_scatter.png)