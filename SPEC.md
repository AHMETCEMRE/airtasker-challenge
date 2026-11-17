# Solution Spec — Weather Risk Pipeline

Two consumers, one definition of risk. Product reads one row per task to send CRM warnings; Operations reads one row per location per local day. Both must always report the same high-risk count for the same location and day, so the score is computed **once**, in one model, and both marts derive from it. Thresholds are dbt `vars` because Product will tune them after launch.

## Asset graph

```
  [container start]  generate_tasks.py -> raw.raw_tasks     (text columns, duplicates)
                     dbt seed          -> raw.raw_locations (20 cities)
                                               |
  [Dagster/Python]                             v
                              raw_weather_forecast ....... Open-Meteo /v1/forecast
                                               |           append-only, hourly grain
                                               v           (declared as a dbt source)
  [dbt]   stg_tasks      stg_locations    stg_weather_forecast
              |               |                   |
              +-------+-------+                   |
                      v                           v
               int_tasks_upcoming          int_weather_window
                      |                           |
                      +------------+--------------+
                                   v
                         int_task_weather_risk     <- scoring lives HERE, only here
                                   |
          +------------------------+---------------+-------------------+
          v                                        v                   v
   fct_task_weather_risk        fct_location_daily_weather     snp_task_weather_risk
       (Product)                      (Operations)                (risk history)
                                           ^
                                   int_location_days (spine)
```

The Python asset fetches for **every** location rather than only those with upcoming tasks. That keeps the graph acyclic against the scaffold's single `dbt build`, keeps the raw forecast table reproducible when task-side rules change, and puts no business logic in Python. The cost is wasted calls for quiet locations, which only matters at 40,000.

`dbt build --exclude raw_locations`: the location seed is bootstrap infrastructure owned by `entrypoint.sh`, so excluding it keeps the one table Python reads outside the dbt asset and leaves the graph a DAG.

## Data model

| Table | Layer | Grain |
| --- | --- | --- |
| `raw.raw_tasks` | source | one row per task *version* (duplicates expected) |
| `raw.raw_locations` | seed | one row per `location_id` |
| `raw.raw_weather_forecast` | Dagster | (`location_id`, `forecast_time_utc`, `fetched_at_utc`) — **append-only** |
| `category_weather_exposure` | seed | one row per category |
| `stg_tasks` | staging | one row per `task_id` |
| `stg_locations` | staging | one row per `location_id` |
| `stg_weather_forecast` | staging | (`location_id`, `forecast_time_utc`) — latest fetch per hour |
| `int_tasks_upcoming` | intermediate | one row per upcoming scheduled task |
| `int_weather_window` | intermediate | (`location_id`, `window_start_utc`) |
| `int_task_weather_risk` | intermediate | one row per upcoming scheduled task |
| `int_location_days` | intermediate | (`location_id`, `local_date`) across the horizon |
| `fct_task_weather_risk` | mart | one row per upcoming scheduled task |
| `fct_location_daily_weather` | mart | (`location_id`, `local_date`) |
| `snp_task_weather_risk` | snapshot | (`task_id`, `valid_from`) |

Staging materialises as views, marts as tables.

**Where each calculation lives.** Fetch and land only, in the Dagster asset — it validates coordinates before calling the API, stamps `fetched_at_utc` and the run id, appends, and never scores or filters. Parse, type and deduplicate in staging; no business rules. Eligibility and UTC-to-local conversion in `int_tasks_upcoming`. Window aggregation in `int_weather_window`. **Scoring and labelling in `int_task_weather_risk` and nowhere else**, with the label mapping in a `weather_risk_label(score)` macro so even the CASE expression exists once. Aggregation in `fct_location_daily_weather`. History in a dbt snapshot with `strategy: check` on the score and label, so Product gets run-over-run change without the mart becoming an event log.

`fct_task_weather_risk` is one row per task, not per task per day — a task has one scheduled time, so its risk is a scalar. `fct_location_daily_weather` spans the **whole forecast horizon**, not just days with tasks, so Ops gets a continuous calendar with `scheduled_task_count = 0` on quiet days. Hence the spine, rather than grouping the task fact.

**The reconciliation property.** For any known location and local day, `scheduled_task_count` and `high_risk_task_count` in the location mart equal the row count and the `label = 'high'` count in the task mart. This holds because both come from `int_task_weather_risk`, and it is asserted by a dbt test rather than only in prose. Orphaned-location tasks appear in the task fact as `unknown` and are absent from the location mart — there is no location to attribute them to — so the property is scoped to known locations and the orphan count is surfaced by its own test.

## Risk model

**Eligible tasks:** `lower(trim(state)) = 'assigned'` and `scheduled_at_utc is not null` and `scheduled_at_utc >= run_started_at`. `completed` already happened; `open` has no scheduled time; `cancelled` is excluded **even when scheduled in the future**, which about half are — the output drives a CRM warning, and warning someone about a job that isn't happening is worse than silence. "Now" is `{{ run_started_at }}`, not `current_timestamp`, so a run shares one clock and tests can inject a fixed one.

**Variables — hourly, not daily.** A 9am roof clean doesn't care about 3am rain, and a daily `precipitation_sum` can't tell the difference. Each task is scored over a **3-hour window from `scheduled_at`**, assuming jobs take hours not minutes: `precipitation_probability` (max), `precipitation` mm (sum), `wind_gusts_10m` (max — gusts, not mean wind, decide ladder and marquee safety), `temperature_2m` (min/max), `weather_code` (modal, for Ops' summary).

Requests pass `forecast_days=16`, `timezone=UTC` and explicit units. All three affect correctness, not just which fields return:

- **`forecast_days=16`** — the default is 7, but tasks are scheduled 1–14 days out. On the default, roughly half the eligible tasks silently get no forecast.
- **`timezone=UTC`, converting in dbt** — Open-Meteo buckets in GMT unless told otherwise, so reading its `daily=` block without `timezone` gives wrong day boundaries everywhere outside the UK. Requesting a *named* timezone fixes that but introduces a subtler bug: the response carries one `utc_offset_seconds` for the whole payload, and a single offset cannot describe a 16-day window containing a DST transition. Converting per timestamp in SQL via the IANA name and DuckDB's ICU is correct in every window, and sidesteps the daily-bucketing trap entirely since Ops' summary is aggregated from the hourly rows.
- **Explicit units** — thresholds below are in °C, mm and km/h, so relying on API defaults means a default change silently rescales every score.

Aggregating Ops' daily summary from the same hourly rows is what makes the teams consistent: "high risk at 9am" and "high risk days" cannot come from different numbers. Note that model data thins at the far end of a 16-day request — the last day or two returns nulls for some variables, earlier for UK models than US/AU. Tasks top out at 14 days so it rarely bites, but it is a real source of `unknown`.

**Score** — each hazard maps to 0–100, where 100 means the work cannot reasonably go ahead:

```
precip_score = greatest(precip_probability_max, least(100, precip_mm_sum * 25))  -- 4mm/3h -> 100
wind_score   = least(100, greatest(0, (gust_kmh_max - 20) / 0.40))               -- 20 -> 0, 60+ -> 100
temp_score   = 0 within 5C..32C, ramping to 100 at <= -5C or >= 42C

hazard_score       = greatest(precip_score, wind_score, temp_score)
weather_risk_score = round(hazard_score * category_exposure_weight)   -- 0..100
weather_risk_label = weather_risk_label(weather_risk_score)           -- macro
```

**Why `greatest`, not a weighted sum.** Risk is a disjunction — one severe hazard stops the job, and averaging would dilute a 100% rain probability into a "medium". The cost is that it ignores compounding: 8°C, wet and gusty is worse than any one alone, and this scores it as the worst single hazard. I wouldn't fix that by inventing coefficients; I'd fit them against real reschedule and cancellation rates once there is history — which is the actual reason the snapshot exists.

**Category exposure**, held in a seed so Product can tune a CSV rather than SQL:

| Weight | Categories | Why |
| --- | --- | --- |
| 1.0 | Roof Cleaning, Exterior Painting, Lawn Mowing, Gardening, Pool Cleaning | Outdoors by definition; rain or wind stops work outright — a wet roof is a fall risk, paint won't cure, wet grass tears |
| 0.6 | Moving & Removals, Event Setup, Photography | Substantially outdoors but completable at a cost — goods get wet, marquees are wind-limited, shoots move indoors |
| 0.2 | Cleaning | Indoor work, but access and carrying equipment in heavy weather drives reschedules |
| 0.0 | Tax Return, Bookkeeping, Online Tutoring, Graphic Design | Desk or remote; must never generate a warning |

**Ambiguous cases.** *Photography* is the weakest assignment — a studio portrait and a beach wedding share a category, so 0.6 is a deliberate compromise; the titles carry the signal, making keyword classification the obvious v2. *Event Setup* is wind-dominated and *Moving & Removals* rain-dominated, which a single exposure scalar can't express — a known simplification. *Indoor categories* score a deterministic `0`/`low`, never null: no weather sensitivity is a fact, not missing data. **No usable forecast** — orphaned location, invalid coordinates, or beyond the usable horizon — yields a null score and `unknown`, explicitly **not** `low`, because missing data isn't safe data and `low` would suppress a warning the pipeline can't justify. `fct_location_daily_weather` carries `unknown_risk_task_count` so counts still reconcile.

**Labels:** `low` 0–29, `medium` 30–59, `high` 60–100, `unknown` when null. Lower bounds inclusive, from `vars` (`risk_threshold_medium`, `risk_threshold_high`) consumed only through the macro — so retuning is a variable change, and because both marts call the same macro it can never desynchronise the two teams.

## Data quality and assumptions

Two issues were flagged in the README; the rest I found by reading `scripts/generate_tasks.py` and `seeds/raw_locations.csv`.

| # | Issue | Handling |
| --- | --- | --- |
| 1 | **Duplicate `task_id`s** — `T003`, `T017`, `T041`, each twice with different `posted_at` | `row_number() over (partition by task_id order by posted_at_utc desc)` with a deterministic tiebreak |
| 2 | **Dedup must follow parsing** — each pair is rendered in two *different* formats (`...T08:00:00Z` vs `2026-10-21 21:23:00+11:00`); the second is later in UTC but sorts **earlier** as text, since `' '` (0x20) precedes `'T'` (0x54) | Parse to UTC, *then* deduplicate. Ordering on raw strings picks the wrong record in all three pairs |
| 3 | **Three timestamp formats**, all `varchar` — `...Z`, space-separated UTC, and location-local offset | One `parse_utc_timestamp` macro using `try_strptime` with an ordered format list. Unparseable values become null and are counted by a test, not silently coerced |
| 4 | **Missing timestamps are sometimes `null`, sometimes `''`** (~30% of absences) | `nullif(trim(x), '')` before parsing |
| 5 | **Dirty `category`** — ~5% lowercase, ~3% uppercase, ~4% trailing space | Normalise on `lower(trim(...))` and join to the exposure seed for the canonical name. A `relationships` test on that join is what catches an unmapped category; without it a dirty category loses its warning silently |
| 6 | **Dirty `state`** — ~10% capitalised (`Assigned`) | `lower(trim(state))` plus `accepted_values`. A naive `state = 'assigned'` drops ~10% of eligible tasks |
| 7 | **Dirty `budget`** — `$284.50`, `284.50`, `285` | Strip `$` and separators, `try_cast` to `decimal(10,2)`. Unused by the risk model; cleaned for completeness |
| 8 | **Orphaned `location_id`** — `T058` → `LOC_MISSING_1` (assigned, 6 days out), `T079` → `LOC_MISSING_2` | `left join` throughout so they're never dropped silently; `T058` reaches the task fact as `unknown`. `relationships` test at `severity: warn` keeps the count visible without failing the build |
| 9 | **Perth's lat/lng are transposed** — `115.8605,-31.9505`, latitude outside ±90 | Ingestion refuses implausible coordinates, so no request is wasted on a call that can only fail; `stg_locations` derives `is_valid_coordinates` and Perth's tasks score `unknown`. **Deliberately not auto-swapped** — the transposition is obvious to a human, which is why a human should fix the seed; silently repairing inputs hides a source bug and can't distinguish a transposition from a genuinely wrong value. Test is `warn`, not `error`: failing the build over one city would deny Product its warnings for the other nineteen |
| 10 | **Inconsistent country codes** — Leeds and Glasgow are `UK`, the rest `GB` | Normalise to ISO 3166-1 alpha-2 in `stg_locations`, with `accepted_values` on the result |
| 11 | **Timezone doesn't identify a location** — Sydney/Canberra share one, as do Chicago/Houston and LA/Seattle | Forecasts keyed on `location_id`, fetched per coordinate pair. Keying on timezone would collapse distinct cities onto one forecast |
| 12 | **Half-hour offsets and no-DST zones** — Adelaide and Darwin are +09:30, Phoenix has no DST | IANA names and `at time zone` (ICU) throughout, never a fixed numeric offset |
| 13 | **UTC day ≠ local day** — `scheduled_at` lands at UTC 08–15, which at +10/+11 is 18:00–01:00 local, so some AU tasks fall on the *next* local day | `(scheduled_at_utc at time zone tz)::date`. Grouping on `date(scheduled_at_utc)` mis-buckets a material share of AU tasks and breaks Ops' midnight requirement |

**Tests.** On staging, the ones that would actually have caught a bug above: `unique`/`not_null` on `task_id` (#1, #2), `accepted_values` on `state` (#6), `relationships` on `location_id` at warn (#8), `unique`/`not_null` on `location_id`, `accepted_values` on normalised `country` (#10), and a coordinate-range test at warn (#9). One dbt unit test on `int_task_weather_risk` covering where the logic can go wrong rather than the happy path: heavy rain + Roof Cleaning → `high`; same weather + Tax Return → `0`/`low`; missing forecast → null/`unknown` rather than `0`/`low`; and exactly 60 → `high`, pinning the boundary.

**Assumptions.** Open-Meteo needs no key and 20 calls per run sits well inside its fair-use limits; forecasts are point forecasts at the nearest model grid cell to the city centroid, which is the finest geography `raw_tasks` supports, and the served grid cell is stored per row because it explains why nearby cities can share a forecast. A timestamp without an offset is UTC, and the source writes the same instant in several formats, never several instants. `scheduled_at` is the *start* of work, which occupies about three hours, and risk is evaluated against the latest forecast at run time rather than predicting what the forecast will say on the day. Product warns on `high`, possibly `medium` after tuning — which is why the label is published alongside the score. DuckDB has ICU available for named-timezone arithmetic, and dbt is 1.8+ so `unit_tests` are supported.

Scaling to 1M tasks across 40,000 locations, and the trade-offs taken here, are covered in `DECISIONS.md`.
