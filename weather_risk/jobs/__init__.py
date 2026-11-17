from dagster import AssetSelection, define_asset_job

weather_risk_pipeline_job = define_asset_job(
    name="weather_risk_pipeline",
    description=(
        "Fetch forecasts from Open-Meteo, then rebuild every dbt model, test "
        "and snapshot. Ordering is not configured here: it is derived from the "
        "asset graph, so the forecast asset always runs before dbt."
    ),
    selection=AssetSelection.all(),
)

# Picked up by weather_risk/definitions.py.
jobs = [weather_risk_pipeline_job]
