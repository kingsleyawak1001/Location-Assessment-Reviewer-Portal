# Phase 2 Assumptions and Defaults

This document fixes the default behavior used for the current Phase 2 implementation where the assessment leaves room for interpretation.

## Grouping assumptions

- Input to Phase 2 is only the **accepted** dataset from quality validation.
- Grouping is performed per `device_id`, ordered by `ts_utc`.
- A new visit starts when any condition is true:
  - first ping for the device,
  - time gap from previous ping is greater than `900s` (except configured night-gap bridge),
  - distance jump from previous ping is greater than dynamic threshold:
    `250m + max(prev_accuracy_m, curr_accuracy_m)` (haversine).
- `accuracy_m = 0` is treated as perfect accuracy, so it adds zero tolerance to the distance boundary.
- Missing accuracy uses configurable fallback (`phase2_unknown_accuracy_m`, default `50m`).
- Night-gap bridge rule (defaults):
  - if `gap > 900s` and `gap <= 14,400s`,
  - and ping is in night window (`22:00..06:00`),
  - and distance from previous ping is small (`<= 120m`),
  - then the visit is not split.
- Representative location is a robust point using `median(latitude)` and `median(longitude)` per visit.
- Visit duration is `end_ts - start_ts` in seconds.

## Stay vs pass_by assumptions

- `stay` requires both:
  - `duration_seconds >= 600`,
  - `ping_count >= 3`.
- Otherwise the visit is labeled `pass_by`.

## Stay type assumptions (baseline taxonomy)

Stay type is applied only to visits labeled `stay`.

- `home`: overnight-like window and long enough stay
  - `(start_hour >= 20 OR end_hour <= 8)` and `duration_seconds >= 7200`.
- `work`: daytime window and long enough stay
  - `(start_hour in [7..11]) AND (end_hour in [12..20])` and `duration_seconds >= 10800`.
- `other`: all remaining stays.

For `pass_by`, `stay_type` is null.

## Non-goals in this phase

- No POI enrichment/geofencing.
- No personalized multi-day behavioral modeling.
- No probabilistic or ML classification.
- No cross-file state carry-over for long visits spanning file boundaries.
