#!/usr/bin/env python3
"""
Fetch catchment data from DuckDB, filter by spatial overlap, 
and write per-catchment attribute tables to separate Parquet files.
"""

import os
import argparse
import logging
from pathlib import Path
from typing import Tuple, Optional, Dict

import duckdb
import geopandas as gpd
import pandas as pd
from shapely.wkt import dumps
from shapely.geometry import Polygon

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger(__name__)


def _common_cte(wkt4326: str) -> str:
    """
    Returns the shared CTE for transforming the WKT and filtering catchments.
    """
    return f"""
    WITH input_query AS (
      SELECT '{wkt4326}' AS wkt_string
    ),
    transformed_query AS (
      SELECT
        ST_Transform(
          ST_GeomFromText(wkt_string),
          'EPSG:4326', 'EPSG:5070', TRUE
        ) AS query_geom
      FROM input_query
    ),
    filtered_catchments AS (
      SELECT DISTINCT
        c.catchment_id,
        c.geometry
      FROM catchments AS c
      JOIN transformed_query AS tq
        ON ST_Intersects(c.geometry, tq.query_geom)
    )
    """


def get_catchment_data_for_geojson_poly_split(
    geojson_fp: str, con: duckdb.DuckDBPyConnection
) -> Tuple[gpd.GeoDataFrame, pd.DataFrame, Optional[Polygon]]:
    """
    Reads a GeoJSON polygon, runs two spatial queries against DuckDB
    (one for geometries, one for attributes), and returns:

      - geometries_gdf: GeoDataFrame[c   atchment_id, geometry in EPSG:5070]
      - attributes_df: DataFrame[*all attribute columns* including catchment_id]
      - query_polygon_5070: shapely Polygon for later overlap filtering
    """
    # 1) Load & reproject the query polygon
    gdf = gpd.read_file(geojson_fp)
    if gdf.empty:
        logger.error("GeoJSON is empty or invalid: %s", geojson_fp)
        return gpd.GeoDataFrame(), pd.DataFrame(), None

    # Ensure CRS is EPSG:4326 for injection into SQL
    if gdf.crs is None:
        logger.info("Assuming input GeoJSON CRS = EPSG:4326")
        gdf.set_crs(epsg=4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        logger.info("Reprojecting query polygon to EPSG:4326")
        gdf = gdf.to_crs(epsg=4326)

    src_poly_4326 = gdf.geometry.iloc[0]
    wkt4326 = dumps(src_poly_4326)

    # Also get the same polygon in EPSG:5070 for Python overlap checks
    query_poly_5070 = gdf.to_crs(epsg=5070).geometry.iloc[0]

    # 2) Build and run the geometry-only query
    cte = _common_cte(wkt4326)
    sql_geom = (
        cte
        + """
    SELECT
      fc.catchment_id,
      ST_AsWKB(fc.geometry) AS geom_wkb
    FROM filtered_catchments AS fc;
    """
    )
    geom_df = con.execute(sql_geom).fetch_df()
    if geom_df.empty:
        logger.info("No catchments intersect the query polygon.")
        empty_gdf = gpd.GeoDataFrame(
            columns=["catchment_id", "geometry"], geometry="geometry", crs="EPSG:5070"
        )
        return empty_gdf, pd.DataFrame(), query_poly_5070

    # Decode WKB → shapely geometries
    wkb_series = geom_df["geom_wkb"].apply(
        lambda x: bytes(x) if isinstance(x, bytearray) else x
    )
    geometries_gdf = gpd.GeoDataFrame(
        geom_df[["catchment_id"]],
        geometry=gpd.GeoSeries.from_wkb(wkb_series, crs="EPSG:5070"),
        crs="EPSG:5070",
    )

    # 3) Build and run the attribute query
    sql_attr = (
        cte
        + """
    SELECT
      f.catchment_id,
      ht.* EXCLUDE (catchment_id),
      hrr.raster_path   AS rem_raster_path,
      hcr.raster_path   AS catchment_raster_path
    FROM filtered_catchments AS f
    LEFT JOIN hydrotables             AS ht   ON f.catchment_id = ht.catchment_id
    LEFT JOIN hand_rem_rasters       AS hrr  ON f.catchment_id = hrr.catchment_id
    LEFT JOIN hand_catchment_rasters AS hcr  ON hrr.rem_raster_id = hcr.rem_raster_id;
    """
    )
    attributes_df = con.execute(sql_attr).fetch_df()

    return geometries_gdf, attributes_df, query_poly_5070


def filter_dataframes_by_overlap(
    geometries_gdf: gpd.GeoDataFrame,
    attributes_df: pd.DataFrame,
    query_polygon_5070: Polygon,
    overlap_threshold_percent: float = 10.0,
) -> Tuple[gpd.GeoDataFrame, pd.DataFrame, Dict[str, int]]:
    """
    Filters geometries and attributes by the % overlap with query_polygon_5070.
    Returns (filtered_geoms, filtered_attrs, summary_stats).
    """
    stats = {
        "initial_geoms": len(geometries_gdf),
        "initial_attrs": len(attributes_df),
    }
    if geometries_gdf.empty or query_polygon_5070 is None:
        stats.update(final_geoms=0, final_attrs=0, removed_geoms=0, removed_attrs=0)
        return geometries_gdf.copy(), attributes_df.copy(), stats

    geoms = geometries_gdf.copy()
    geoms["area"] = geoms.geometry.area
    geoms["inter"] = geoms.geometry.apply(
        lambda g: g.intersection(query_polygon_5070).area if not g.is_empty else 0.0
    )
    geoms["pct"] = (geoms["inter"] / geoms["area"].replace({0: pd.NA})) * 100
    geoms["pct"].fillna(0.0, inplace=True)

    keep_ids = set(geoms.loc[geoms["pct"] >= overlap_threshold_percent, "catchment_id"])
    filtered_geoms = geoms[geoms.catchment_id.isin(keep_ids)].drop(
        columns=["area", "inter", "pct"]
    )
    filtered_attrs = attributes_df[attributes_df.catchment_id.isin(keep_ids)].copy()

    stats["final_geoms"] = len(filtered_geoms)
    stats["final_attrs"] = len(filtered_attrs)
    stats["removed_geoms"] = stats["initial_geoms"] - stats["final_geoms"]
    stats["removed_attrs"] = stats["initial_attrs"] - stats["final_attrs"]

    return filtered_geoms, filtered_attrs, stats


def main():
    p = argparse.ArgumentParser(
        description="Fetch/filter catchments and write per-ID Parquet files"
    )
    p.add_argument("-g", "--geojson", required=True, help="Path to query GeoJSON")
    p.add_argument("-d", "--db", required=True, help="Path to DuckDB file")
    p.add_argument(
        "-t",
        "--threshold",
        type=float,
        default=10.0,
        help="Min overlap % to keep a catchment",
    )
    p.add_argument(
        "-o",
        "--outdir",
        required=True,
        help="Directory to write <catchment_id>.parquet files",
    )
    args = p.parse_args()

    # Connect & load extension
    con = duckdb.connect(database=args.db, read_only=True)
    try:
        con.execute("LOAD spatial;")
    except duckdb.Error:
        logger.warning("Could not load DuckDB 'spatial' extension.")

    # Fetch & filter
    geoms, attrs, query_poly_5070 = get_catchment_data_for_geojson_poly_split(
        args.geojson, con
    )
    if geoms.empty:
        logger.info("No geometries found. Exiting.")
        return

    fg, fa, stats = filter_dataframes_by_overlap(
        geoms, attrs, query_poly_5070, args.threshold
    )
    logger.info("Overlap filter summary: %s", stats)

    # Prepare output directory
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Write one parquet per catchment_id (drop the catchment_id column inside each file)
    for catch_id, group in fa.groupby("catchment_id"):
        df = group.drop(columns=["catchment_id"])
        out_path = outdir / f"{catch_id}.parquet"
        df.to_parquet(str(out_path), index=False)
        logger.info(
            "Wrote %d rows for catchment '%s' → %s", len(df), catch_id, out_path
        )

    con.close()


if __name__ == "__main__":
    main()
