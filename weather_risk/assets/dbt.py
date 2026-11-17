from typing import Any, Mapping

from dagster import AssetKey
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    dbt_assets,
)

from weather_risk.resources import dbt_project


class WeatherRiskDbtTranslator(DagsterDbtTranslator):
    """Map dbt sources onto the Dagster asset keys that actually produce them.

    By default dagster-dbt keys a dbt source as [source_name, table_name], so
    `raw.raw_weather_forecast` becomes AssetKey(["raw", "raw_weather_forecast"]),
    while the Python asset that writes that table is keyed
    AssetKey(["raw_weather_forecast"]).

    Left alone, those are two unrelated nodes: the graph shows no edge between
    ingestion and dbt, and `dbt build` is free to run before any forecast has
    been fetched. Collapsing source keys to the bare table name makes the two
    line up, which is the single declaration that orders the whole pipeline.
    """

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey([dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=WeatherRiskDbtTranslator(),
    # raw_locations is bootstrap infrastructure: scripts/entrypoint.sh seeds it
    # before Dagster starts, and the ingestion asset reads it directly. Leaving
    # it inside this multi-asset would make the seed both upstream and
    # downstream of raw_weather_forecast, which is a cycle.
    exclude="raw_locations",
)
def weather_risk_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
