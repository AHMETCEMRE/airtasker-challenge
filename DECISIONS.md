# Decisions

`SPEC.md` explains what the pipeline does. This file explains the choices behind
it: what else I could have done, why I did not, what each choice costs, where the
model is weak, and what I would change before running this for real.

---

## Choices and what they cost

### I fetch the weather for every city, not just cities with jobs booked

**The other option:** only call the weather service for cities that currently
have a job coming up. Here that would be about 14 calls instead of 20.

**Why I did not:** it would put a rule about tasks inside the part of the code
that fetches weather. The weather table never deletes anything, and that history
is what lets Product compare a job's risk today against last week. If the set of
rows depended on a rule that changes, the history would stop meaning one thing.
It would also tangle the pipeline: the weather step would depend on the dbt
models, which already depend on the weather step.

**What it costs:** wasted calls for quiet cities. With 40,000 cities this flips
the other way — see *If this ran for real* below.

### I flag Perth's broken coordinates instead of fixing them

The city list has Perth as `lat=115.8605, lng=-31.9505`. Latitude only goes up to
90, so those two numbers are clearly the wrong way round.

**The other option:** swap them back automatically when the latitude is
impossible and the longitude is not.

**Why I did not:** if the pipeline quietly fixes bad input, nobody ever learns
the input is bad, so nobody fixes it at the source. And the guess is not safe:
if someone had simply mistyped the longitude as 115 rather than swapping the
pair, "fixing" it would invent a believable location in completely the wrong
place. That is worse than having no data, because it looks right.

**What it costs:** 7 jobs come out as `unknown` instead of scored. The test warns
rather than fails, so the other 19 cities still get their warnings, and it turns
into a real failure if a second city ever develops the same problem.

### I take the single worst hazard, not an average of all three

**The other option:** something like `0.55 × rain + 0.30 × wind + 0.15 × temp`.

**Why I did not:** any one bad thing is enough to stop the job. Averaging hides
that. A 100% chance of rain with calm wind and mild temperatures would come out
as "medium", which is plainly wrong. In the current data, T088 has 69 km/h gusts
and no rain at all — an average would not have flagged it.

**What it costs:** it ignores things adding up. 8°C *and* wet *and* windy is
worse than any one of those alone, but this scores it as only the worst one. I
would not fix that by making up new numbers. I would work them out from real
data on which jobs actually got rescheduled, which is why the history table
exists.

### I get hourly weather and total it up myself, instead of asking for daily

**The other option:** ask the weather service for daily figures and use them
straight.

**Why I did not:** two reasons. A job booked for 9am does not care whether it
rained at 3am, and a daily total cannot tell the difference. And building the
Operations daily view from the same hourly rows that Product's scores come from
is what makes the two teams agree — they are reading the same numbers.

It also avoids a trap. The weather service cuts its daily figures at GMT midnight
unless you tell it otherwise, so asking for daily data the obvious way gives you
day boundaries that are wrong everywhere outside the UK.

**What it costs:** 7,296 rows where about 300 would do, and a join to build the
3-hour windows. That does not matter at this size. It would at a bigger one.

### I ask for UTC times and convert to local time in SQL

**The other option:** ask the weather service for each city's own local time, so
the timestamps arrive ready to use.

**Why I did not:** the response only tells you one time offset for the whole
thing. One offset cannot describe a 16-day stretch that contains a daylight
saving change — every hour after the change would be an hour out. Converting each
timestamp separately in SQL is right in every case. No stretch in this run
happens to contain a daylight saving change, so the easy option would have
worked today and broken in April.

### History goes in a separate table, not by adding rows to the main one

**The other option:** keep adding rows to `fct_task_weather_risk` with a run id.

**Why I did not:** that changes what one row means, from "one job" to "one job on
one run". Everyone reading it would then have to remember to pick only the newest
row, and the first person who forgets double-counts their warnings. Keeping
history in its own table means the main table stays one row per job, and asking
for history is something you do on purpose.

The history table only records a new row when the **score or the label** changes.
Product wants to know if the risk changed, not if the forecast wobbled. Forecasts
move every few hours; the verdict usually does not.

### I wrote three small test helpers instead of installing dbt_utils

**The other option:** add the `dbt_utils` package for its range check and
duplicate check.

**Why I did not:** one range check is not worth a dependency that everyone has to
install before the project will even run. The three helpers are a few lines each
in `macros/generic_tests.sql`.

**What it costs:** slightly reinventing the wheel, and mine are less
battle-tested than the package versions.

### Known bad data warns instead of failing the build

Three problems are known and expected: Perth's latitude, Perth's coordinate flag,
and the two jobs pointing at cities that do not exist. These are set to warn, and
to turn into a real error if the count ever goes above one.

**Why:** failing the whole build over one bad row would mean Product gets no
warnings at all, for any city. Warning keeps the problem visible on every run
while still delivering the output. The step up to an error means the build does
break if the problem spreads.

**What it costs:** a reviewer has to read the warnings instead of trusting a
green tick. That is why the three expected warnings are written down here and in
`SPEC.md`, so nobody mistakes them for something rotting.

### Category weights and score thresholds are settings, not code

The brief says Product will adjust the thresholds after launch, so they are
settings they can change. The score-to-label rule is written in one place only,
so changing a threshold cannot make the two tables disagree. A test checks that
rather than me just claiming it.

---

## It got tested by a real outage

While I was working on the Dagster job, the weather service started failing on
and off. So the pipeline got tested by a genuine failure instead of by my own
fake data. One full run:

- got the weather for 11 of 19 cities before the service went bad
- kept retrying the other 8, gave up, and wrote each one to the log
- **saved the 11 it had** instead of throwing the whole run away
- used the new forecast for those 11 and kept the older one for the other 8
- kept both final tables in agreement the whole time
- finished successfully in 13.7 minutes

The history table then showed exactly which verdicts moved and which forecast
each one came from — T039 dropped from 100 to 49, T033 rose from 28 to 62 — so
the change could be traced to a new forecast rather than a code change.

Two choices paid for themselves here. Working out "the current forecast" per city
per hour, rather than just taking the newest run overall, is why a half-finished
run fills in gaps instead of wiping out cities it missed. And treating one city's
failure as something to skip rather than something fatal is why 11 cities still
got their warnings.

---

## Where this is weak

1. **With only some weather available, the score uses what there is.** If only
   the temperature came back and it is freezing, warning is right. If only the
   temperature came back and it is mild, the job scores `low` even though we know
   nothing about rain. That is deliberate and there is a test for it. The
   alternative — demand all three or say `unknown` — would turn the thin end of
   the forecast into a wall of unknowns.
2. **The "worst weather of the day" figure is rough.** I use the highest weather
   code as a stand-in for "how bad did it get". Those codes are only loosely
   ordered, so it is reliable within a group (light to heavy rain) and at the top
   end (storms), but comparing snow against showers is arguable.
3. **One number per category cannot say which hazard matters most.** Event Setup
   is really about wind and Moving is really about rain, but both get 0.6.
4. **Photography is the weakest category guess.** A studio portrait and a beach
   wedding are the same category. The job titles give it away, so sorting by
   keyword is the obvious next step.
5. **The scores are reasoned, not proven.** The thresholds are sensible but
   nothing here has been checked against whether jobs actually got rescheduled.
   This is the biggest gap.
6. **Every job is assumed to take 3 hours.** A lawn mow and repainting a house do
   not take the same time.
7. **One forecast per city centre.** The task data has nothing more precise than
   the city, so for somewhere the size of Sydney that is a real simplification.
8. **Risk is worked out when the pipeline runs, not for when the worker turns
   up.** A job 14 days out is scored against a 14-day forecast, which is not
   worth much. Trusting far-out forecasts less would be a sensible addition.
9. **The sample data books jobs at odd local hours** — 6 at local midnight, 8 at
   9pm — because the generator picks UTC hours 8 to 15, which in Australia is
   6pm to 2am local. So Australian jobs get scored against night-time weather.
   That is a quirk of the test data, not a bug, but it does mean the spread of
   scores here is not realistic. For real data, checking that jobs cluster in
   normal working hours would be worth a test.

---

## If this ran for real: 1M jobs, 40,000 cities

1. **Stop calling the weather service once per city.** 40,000 calls one after
   another will not fit in a run. The service accepts several locations per call,
   so cities get grouped, and the fetch gets split by region so each part can
   retry and catch up on its own instead of one failure killing everything. The
   "only fetch cities with jobs" idea I turned down above becomes worth it.
2. **One schedule per region** instead of a single 18:00 UTC run, so every region
   gets its warning before its own working day, not just Australia.
3. **Only rebuild what changed**, split by local date, instead of rebuilding
   every table every time. The 3-hour window table should also be narrowed to
   hours that actually have jobs.
4. **Start deleting old forecasts.** Keeping every hourly forecast forever grows
   faster than the job data does.
5. **Move off DuckDB to a proper warehouse.** That mostly changes some function
   names and how tables get updated, and removes the "only one thing can write at
   a time" limit that makes local work awkward. The layout of the models, the
   single place the score is worked out, and the test that the two tables agree
   all stay the same — which is the point of putting them where they are.
6. **Calibrate the score.** Once the history table has a few months in it
   alongside what actually happened to those jobs, the thresholds and the way the
   three hazards combine can be worked out from data instead of reasoned.

---

## Things I left out on purpose

Sorting categories by job title, weighting hazards differently per category, a
proper ranking of weather codes by severity, trusting far-out forecasts less,
actually sending the CRM messages, backfilling old dates, and anything to do with
deploying to the cloud. Each is mentioned above where it belongs.

The brief says unfinished P1 work scores better than finished P3 work, so P1 and
P2 were done and checked before anything optional was started.

If I had run out of time, the order I would have dropped things was: the history
table first, then the Python tests for the weather client, then the full calendar
of city-days going back to a simple grouping. None of them had to go.
