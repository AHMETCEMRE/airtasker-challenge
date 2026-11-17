from dagster import DefaultScheduleStatus, ScheduleDefinition

from weather_risk.jobs import weather_risk_pipeline_job

daily_weather_risk_schedule = ScheduleDefinition(
    name="daily_weather_risk_refresh",
    job=weather_risk_pipeline_job,
    cron_schedule="0 18 * * *",
    execution_timezone="UTC",
    description=(
        "Daily refresh at 18:00 UTC.\n\n"
        "Why that hour: this output drives a warning a poster has to act on, "
        "so it wants to land before their working day, and a single global run "
        "cannot be early morning everywhere. 18:00 UTC is about 05:00 in "
        "Sydney, which serves the ~60% of tasks in Australia, while still "
        "being same-day for the UK and US rather than the night before. The "
        "honest trade-off is that a Los Angeles poster gets a forecast "
        "generated at 10:00 their time, several hours staler than a Sydney "
        "poster's.\n\n"
        "In production this would be partitioned by region with a schedule "
        "per timezone band, which is also what makes 40,000 locations "
        "tractable - see SPEC.md."
    ),
    # Stopped by default so a reviewer cloning this repo does not get
    # unattended runs hitting a public API. Turn it on from the Automation tab
    # in the Dagster UI, or trigger weather_risk_pipeline manually.
    default_status=DefaultScheduleStatus.STOPPED,
)

# Picked up by weather_risk/definitions.py.
schedules = [daily_weather_risk_schedule]
