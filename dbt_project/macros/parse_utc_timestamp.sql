{#
    Parse a text timestamp from raw_tasks into a naive UTC timestamp.

    The source system writes the same instant in three different shapes:

        2026-10-21T08:00:00Z          ~70% of values
        2026-10-21 08:00:00+00:00     ~15%
        2026-10-21 19:00:00+11:00     ~15%, the location's local offset

    and writes an absent value as either NULL or an empty string.

    Two things here are deliberate.

    1. The 'Z' suffix is rewritten to '+00:00' before parsing. DuckDB's %z does
       not match a bare 'Z', so the obvious format string '%Y-%m-%dT%H:%M:%SZ'
       matches the literal character and yields a timestamp with no offset
       information, which DuckDB then resolves against the *session* timezone.
       Verified: with TimeZone='America/New_York', that path returns
       2026-10-21 12:00:00+00:00 for an input of 08:00Z - wrong by four hours.
       Normalising the suffix makes the result independent of session config.

    2. An explicit format list, rather than try_cast(... as timestamptz).
       The cast also parses all three shapes correctly today, but it accepts
       anything ISO-ish, including a string with no offset at all, which it
       would again resolve against the session timezone. The list states the
       contract: these three shapes are expected, and anything else becomes
       NULL so a not_null test surfaces it instead of the pipeline guessing.

    Returns TIMESTAMP (naive), holding the UTC wall clock, which is the same
    convention raw_weather_forecast.forecast_time_utc uses.
#}

{% macro parse_utc_timestamp(column_name) %}
    try_strptime(
        regexp_replace(nullif(trim({{ column_name }}), ''), 'Z$', '+00:00'),
        ['%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%d %H:%M:%S%z']
    ) at time zone 'UTC'
{% endmacro %}
