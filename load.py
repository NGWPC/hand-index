#!/usr/bin/env python3
import argparse
import concurrent.futures
import contextlib
import multiprocessing
import os
import queue
import tempfile
import threading
import uuid as py_uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import fiona
import fsspec
import geopandas as gpd
import pandas as pd
from pyogrio.errors import DataSourceError
from shapely.ops import unary_union

SENTINEL = object()  # Use unique object to avoid accidental queue shutdown
UUID_NAMESPACE = py_uuid.NAMESPACE_DNS  # Cache UUID namespace for performance

# Aggregation helpers defined here for performance
to_array_agg = lambda s: list(s.dropna()) or None
to_scalar_agg = lambda s: s.dropna().iloc[0] if len(s.dropna()) > 0 else None


def load_extensions(conn, extensions: List[str]):
    """Load DuckDB extensions."""
    for ext in extensions:
        try:
            conn.execute(f"INSTALL {ext}{' FROM community' if ext == 'h3' else ''}; LOAD {ext};")
        except:
            pass


@contextlib.contextmanager
def get_database_connection(db_path: str):
    """Context manager for DuckDB connections with spatial extension."""
    conn = duckdb.connect(db_path)
    try:
        load_extensions(conn, ["spatial"])
        yield conn
    finally:
        conn.close()


def initialize_database(db_path: str, schema_path: str):
    """Initialize the DuckDB database with schema."""
    with get_database_connection(db_path) as conn:
        with open(schema_path, "r") as f:
            for stmt in [s.strip() for s in f.read().split(";") if s.strip()]:
                try:
                    conn.execute(stmt)
                except Exception as e:
                    print(f"Warning: Failed to execute statement: {stmt[:50]}... Error: {e}")
    print(f"Database initialized at: {db_path}")


@contextlib.contextmanager
def fetch_local(path: str):
    """Download S3 files to local temp directory - required because some libraries need local file access."""
    if not path.lower().startswith(("s3://", "s3a://")):
        yield path
        return

    fs, anon_path = fsspec.core.url_to_fs(path)
    fd, local_path = tempfile.mkstemp(suffix=Path(anon_path).name)
    os.close(fd)
    try:
        fs.get(anon_path, local_path)
        yield local_path
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


def make_uri(path: str, protocol: str) -> str:
    """flexible uri creation"""
    return path if protocol == "file" else f"{protocol}://{path}"


def list_branch_dirs(hand_dir: str) -> List[str]:
    """Find all branch directories in the HAND directory structure."""
    fs, root_path = fsspec.core.url_to_fs(hand_dir)
    protocol = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    branch_dirs = []

    for dir_info in fs.ls(root_path, detail=True):
        if dir_info["type"] != "directory":
            continue
        branches_path = f"{dir_info['name']}/branches"
        if fs.exists(branches_path):
            branch_dirs.extend(
                [make_uri(b["name"], protocol) for b in fs.ls(branches_path, detail=True) if b["type"] == "directory"]
            )
    return branch_dirs


def process_hydrotable_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process hydrotable dataframe by grouping by HydroID and converting select columns to arrays for stoarge and query efficiency.
    """
    if "HydroID" not in df.columns:
        raise ValueError("Missing required column: HydroID")

    df["HydroID"] = df["HydroID"].astype(str)

    numeric_cols = {"nwm_version_id", "feature_id", "stage", "discharge_cms", "default_discharge_cms"}
    array_cols = {"stage", "discharge_cms", "default_discharge_cms"}

    for col in [c for c in df.columns if c != "HydroID" and c in numeric_cols]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    agg_dict = {col: (to_array_agg if col in array_cols else to_scalar_agg) for col in df.columns if col != "HydroID"}

    return df.groupby("HydroID").agg(agg_dict).reset_index()


def read_geometries(file_uri: str) -> gpd.GeoDataFrame:
    """Read geometries from file, with fallback to fiona."""
    with fetch_local(file_uri) as local_file:
        try:
            return gpd.read_file(local_file)
        except DataSourceError:
            with fiona.open(local_file, driver="GPKG") as src:
                return gpd.GeoDataFrame.from_features(src, crs=src.crs)


def process_files(fs, dir_path: str, pattern: str, protocol: str, processor=None) -> List[Any]:
    """Generic file processor."""
    results = []
    for file_path in fs.glob(f"{dir_path}/{pattern}"):
        try:
            file_uri = make_uri(file_path, protocol)
            if processor:
                results.append(processor(file_uri))
            else:
                results.append(file_uri)
        except Exception as e:
            print(f"  ERROR processing {file_path}: {e}")
    return results


def process_branch(branch_dir: str, hand_version: str, nwm_version: str) -> Optional[Dict[str, Any]]:
    """Process one branch directory and return data for batch insertion."""
    print(f"Processing branch: {branch_dir}")

    try:
        fs, dir_path = fsspec.core.url_to_fs(branch_dir)
        protocol = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]

        # Process catchment geometries
        geometries = []
        catchment_crs = None
        for gdf in process_files(fs, dir_path, "*gw_catchments*.gpkg", protocol, read_geometries):
            if not gdf.empty:
                catchment_crs = catchment_crs or gdf.crs.to_string()
                geometries.append(unary_union(gdf.geometry))

        if not geometries:
            print(f"  No catchment geometries found in {branch_dir}")
            return None

        # Create catchment ID
        merged_geometry = unary_union(geometries)
        path_parts = branch_dir.split(f"{hand_version}/", 1)
        relative_path = f"{hand_version}/{path_parts[1]}" if len(path_parts) == 2 else branch_dir
        catchment_id = str(py_uuid.uuid5(UUID_NAMESPACE, f"{relative_path}:{merged_geometry.wkt}"))

        # Process hydrotables
        hydrotable_records = []
        csv_files = process_files(fs, dir_path, "hydroTable_*.csv", protocol)
        if csv_files:
            dfs = []
            for csv_uri in csv_files:
                with fetch_local(csv_uri) as local_file:
                    try:
                        dfs.append(pd.read_csv(local_file))
                    except Exception as e:
                        print(f"  couldn't read CSV: {csv_uri} because of {e}")

            if dfs:
                combined = pd.concat(dfs, ignore_index=True)
                processed = process_hydrotable_data(combined)
                processed["catchment_id"] = catchment_id
                processed["hand_version_id"] = hand_version
                processed["nwm_version_id"] = nwm_version
                hydrotable_records = processed.to_dict("records")

                del dfs, combined, processed
        # Process rasters
        rem_raster_records = []
        catchment_raster_records = []

        rem_files = fs.glob(f"{dir_path}/*rem_zeroed*.tif")
        if rem_files:
            if len(rem_files) > 1:
                print(f"WARNING: Multiple REM rasters found in {dir_path}")
            rem_uri = make_uri(rem_files[0], protocol)
            parts = rem_uri.split(f"{hand_version}/", 1)
            rel_uri = f"{hand_version}/{parts[1]}" if len(parts) == 2 else rem_uri
            rem_id = str(py_uuid.uuid5(UUID_NAMESPACE, f"{catchment_id}:{rel_uri}"))

            rem_raster_records.append(
                {
                    "rem_raster_id": rem_id,
                    "catchment_id": catchment_id,
                    "hand_version_id": hand_version,
                    "raster_path": rem_uri,
                    "metadata": None,
                }
            )

            catch_files = fs.glob(f"{dir_path}/*gw_catchments_reaches*.tif")
            if catch_files:
                if len(catch_files) > 1:
                    print(f"WARNING: Multiple catchment rasters found in {dir_path}")
                catch_uri = make_uri(catch_files[0], protocol)
                parts = catch_uri.split(f"{hand_version}/", 1)
                rel_uri = f"{hand_version}/{parts[1]}" if len(parts) == 2 else catch_uri
                catch_id = str(py_uuid.uuid5(UUID_NAMESPACE, f"{rem_id}:{rel_uri}"))

                catchment_raster_records.append(
                    {
                        "catchment_raster_id": catch_id,
                        "rem_raster_id": rem_id,
                        "raster_path": catch_uri,
                        "metadata": None,
                    }
                )

        print(f"  Successfully processed branch: {branch_dir}")
        return {
            "catchment": {
                "catchment_id": catchment_id,
                "hand_version_id": hand_version,
                "geometry_wkb": merged_geometry.wkb,
                "additional_attributes": None,
            },
            "hydrotables": hydrotable_records,
            "rem_rasters": rem_raster_records,
            "catchment_rasters": catchment_raster_records,
        }

    except Exception as e:
        print(f"  ERROR processing branch {branch_dir}: {e}")
        return None


def batch_insert_data(db_path: str, batch_data: List[Dict[str, Any]], valid_hydrotable_columns: set):
    """Perform batch insertions into the database."""
    if not batch_data:
        return

    with get_database_connection(db_path) as conn:
        try:
            conn.execute("BEGIN TRANSACTION;")

            # Insert catchments
            catchments = [d["catchment"] for d in batch_data if d and "catchment" in d]
            if catchments:
                print(f"Batch inserting {len(catchments)} catchments...")
                conn.executemany(
                    """INSERT INTO Catchments (catchment_id, hand_version_id, geometry, additional_attributes)
                    VALUES (?, ?, ST_GeomFromWKB(?), ?) ON CONFLICT (catchment_id) DO NOTHING""",
                    [
                        (r["catchment_id"], r["hand_version_id"], r["geometry_wkb"], r["additional_attributes"])
                        for r in catchments
                    ],
                )

            # Insert hydrotables
            hydrotables = [ht for d in batch_data if d for ht in d.get("hydrotables", [])]
            if hydrotables:
                print(f"Batch inserting {len(hydrotables)} hydrotable records...")
                # Get valid columns
                # Fast path: check if all records have same columns (common case)
                first_cols = set(hydrotables[0].keys())
                if len(hydrotables) == 1 or all(set(r.keys()) == first_cols for r in hydrotables[1:3]):
                    cols = sorted(first_cols & valid_hydrotable_columns)
                else:
                    # Slow path: records have different columns
                    cols = sorted(set().union(*(set(r.keys()) for r in hydrotables)) & valid_hydrotable_columns)

                # Escape column names with double quotes to handle special characters
                escaped_cols = [f'"{col}"' for col in cols]
                conn.executemany(
                    f"""INSERT INTO Hydrotables ({', '.join(escaped_cols)})
                    VALUES ({', '.join(['?']*len(cols))})
                    ON CONFLICT (catchment_id, hand_version_id, HydroID) DO NOTHING""",
                    [tuple(r.get(col) for col in cols) for r in hydrotables],
                )

            # Insert REM rasters
            rem_rasters = [rr for d in batch_data if d for rr in d.get("rem_rasters", [])]
            if rem_rasters:
                print(f"Batch inserting {len(rem_rasters)} REM rasters...")
                conn.executemany(
                    """INSERT INTO HAND_REM_Rasters (rem_raster_id, catchment_id, hand_version_id, raster_path, metadata)
                    VALUES (?, ?, ?, ?, ?) ON CONFLICT (rem_raster_id) DO NOTHING""",
                    [
                        (r["rem_raster_id"], r["catchment_id"], r["hand_version_id"], r["raster_path"], r["metadata"])
                        for r in rem_rasters
                    ],
                )

            # Insert catchment rasters
            catch_rasters = [cr for d in batch_data if d for cr in d.get("catchment_rasters", [])]
            if catch_rasters:
                print(f"Batch inserting {len(catch_rasters)} catchment rasters...")
                conn.executemany(
                    """INSERT INTO HAND_Catchment_Rasters (catchment_raster_id, rem_raster_id, raster_path, metadata)
                    VALUES (?, ?, ?, ?) ON CONFLICT (catchment_raster_id) DO NOTHING""",
                    [
                        (r["catchment_raster_id"], r["rem_raster_id"], r["raster_path"], r["metadata"])
                        for r in catch_rasters
                    ],
                )

            conn.execute("COMMIT;")
            print(f"Successfully batch inserted data from {len(batch_data)} branches")

            batch_data.clear()

        except Exception as e:
            conn.execute("ROLLBACK;")
            print(f"Error in batch insert: {e}")
            raise


def batch_writer(db_path: str, result_queue: queue.Queue, batch_size: int):
    """Batch writer thread that accumulates results and inserts to database."""
    valid_cols = {
        "catchment_id",
        "hand_version_id",
        "HydroID",
        "nwm_version_id",
        "feature_id",
        "stage",
        "discharge_cms",
        "default_discharge_cms",
        "HUC",
        "LakeID",
    }

    batch = []
    while True:
        item = result_queue.get()
        if item is SENTINEL:
            break

        batch.append(item)
        if len(batch) >= batch_size:
            batch_insert_data(db_path, batch, valid_cols)
            batch = []

    if batch:
        batch_insert_data(db_path, batch, valid_cols)


def load_hand_suite(db_path: str, hand_dir: str, hand_version: str, nwm_version: Decimal, batch_size: int = 200):
    """Load HAND data suite into DuckDB with batch processing."""
    branch_dirs = list_branch_dirs(hand_dir)
    if not branch_dirs:
        print("No branch directories found - exiting")
        return

    print(f"Found {len(branch_dirs)} branch directories to process")

    # we are doing consumer producer pattern here
    # result_queue is consumer waiting for data from producers
    result_queue = queue.Queue()
    writer_thread = threading.Thread(target=batch_writer, args=(db_path, result_queue, batch_size), daemon=True)
    writer_thread.start()

    successful_count = 0
    # These are producers that will process branches in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        futures = [executor.submit(process_branch, bd, hand_version, str(nwm_version)) for bd in branch_dirs]

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    result_queue.put(result)
                    successful_count += 1
            except Exception as e:
                print(f"Error processing branch: {e}")

    # stop the writer thread
    # and wait for it to finish
    result_queue.put(SENTINEL)
    writer_thread.join()

    print(f"Successfully processed {successful_count}/{len(branch_dirs)} branches")


def partition_tables_to_parquet(db_path: str, output_dir: str, h3_resolution: int = 1):
    """Partition tables from DuckDB to parquet files using H3 spatial indexing."""
    with get_database_connection(db_path) as conn:
        print("Loading DuckDB extensions...")
        load_extensions(conn, ["httpfs", "aws", "spatial", "h3"])

        if output_dir.startswith("s3://"):
            print("Configuring AWS settings for S3 access...")
            try:
                conn.execute("SET s3_region='us-east-1';")
            except Exception as e:
                print(f"Warning: Could not configure AWS settings: {e}")

        # Create indexes
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS catchments_geom_idx ON catchments USING RTREE (geometry);",
            "CREATE INDEX IF NOT EXISTS idx_hydro_catchment_id ON hydrotables (catchment_id);",
            "CREATE INDEX IF NOT EXISTS idx_hrr_catchment_id ON hand_rem_rasters (catchment_id);",
            "CREATE INDEX IF NOT EXISTS idx_hcr_rem_raster_id ON hand_catchment_rasters (rem_raster_id);",
        ]:
            try:
                conn.execute(idx_sql)
            except:
                pass

        conn.execute(f"SET VARIABLE h3_resolution = {h3_resolution};")
        output_dir = output_dir.rstrip("/") + "/"

        # Common H3 calculation
        h3_calc = """h3_latlng_to_cell(
            ST_Y(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)),
            ST_X(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)),
            getvariable('h3_resolution')
        )"""

        # Partition catchments
        print("Partitioning catchments table...")
        conn.execute(f"""
            COPY (SELECT c.*, {h3_calc} AS h3_partition_key FROM catchments c)
            TO '{output_dir}catchments/'
            WITH (FORMAT PARQUET, PARTITION_BY (h3_partition_key), OVERWRITE_OR_IGNORE 1);
        """)

        # Create temp mapping table
        print("Creating catchment H3 mapping...")
        conn.execute(f"""
            CREATE TEMP TABLE catchment_h3_map AS
            SELECT catchment_id, {h3_calc} AS h3_partition_key FROM catchments;
        """)

        # Partition hydrotables
        print("Partitioning hydrotables...")
        conn.execute(f"""
            COPY (
                SELECT ht.*, chm.h3_partition_key
                FROM hydrotables ht
                JOIN catchment_h3_map chm ON ht.catchment_id = chm.catchment_id
            ) TO '{output_dir}hydrotables/'
            WITH (FORMAT PARQUET, PARTITION_BY (h3_partition_key), OVERWRITE_OR_IGNORE 1);
        """)

        # Export non-partitioned tables
        for table, name in [
            ("hand_rem_rasters", "HAND REM rasters"),
            ("hand_catchment_rasters", "HAND catchment rasters"),
        ]:
            print(f"Exporting {name} (unpartitioned)...")
            conn.execute(f"""
                COPY {table} TO '{output_dir}{table}.parquet'
                WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
            """)

        # Create H3 lookup table
        print("Creating catchment H3 lookup table...")
        conn.execute(f"""
            COPY (
                WITH CatchmentCentroids AS (
                    SELECT c.catchment_id, {h3_calc} as h3_centroid_cell
                    FROM catchments c
                    WHERE c.geometry IS NOT NULL AND NOT ST_IsEmpty(c.geometry)
                )
                SELECT cc.catchment_id, unnest(h3_grid_disk(cc.h3_centroid_cell, 1)) AS h3_covering_cell_key
                FROM CatchmentCentroids cc
            ) TO '{output_dir}catchment_h3_lookup.parquet'
            WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
        """)

        conn.execute("DROP TABLE IF EXISTS catchment_h3_map;")

    print(f"Tables partitioned successfully to: {output_dir}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True, help="Path to DuckDB database file")
    p.add_argument("--schema-path", default="./schema/hand-index-v0.1.sql", help="Path to DuckDB schema SQL file")
    p.add_argument("--hand-dir", required=True, help="Root of your HAND HUC8 tree (local path or s3://...)")
    p.add_argument("--hand-version", required=True, help="A text id for this HAND run")
    p.add_argument("--nwm-version", required=True, help="NWM version (decimal)")
    p.add_argument("--init-db", action="store_true", help="Initialize database with schema")
    p.add_argument("--output-dir", help="Output directory for partitioned parquet files")
    p.add_argument("--skip-load", action="store_true", help="Skip loading data, only partition existing database")
    p.add_argument("--h3-resolution", type=int, default=1, help="H3 resolution for spatial partitioning (default: 1)")
    p.add_argument("--batch-size", type=int, default=50, help="Number of branches per batch (default: 50)")
    args = p.parse_args()

    db_exists = os.path.exists(args.db_path)

    if args.init_db and db_exists:
        print(f"Error: Database {args.db_path} already exists. Cannot initialize existing database.")
        exit(1)

    # Validate output directory
    if args.output_dir:
        if args.output_dir.startswith("s3://"):
            try:
                fs, out_path = fsspec.core.url_to_fs(args.output_dir)
                if fs.exists(out_path):
                    print(f"Error: S3 output directory {args.output_dir} already exists.")
                    exit(1)
                print(f"S3 output directory {args.output_dir} validated")
            except Exception as e:
                print(f"Error: Could not validate S3 output directory: {e}")
                exit(1)
        else:
            out_path = Path(args.output_dir)
            if out_path.exists():
                print(f"Error: Output directory {args.output_dir} already exists.")
                exit(1)
            try:
                out_path.mkdir(parents=True, exist_ok=False)
                print(f"Created output directory: {args.output_dir}")
            except Exception as e:
                print(f"Error: Could not create output directory: {e}")
                exit(1)

    if args.init_db:
        initialize_database(args.db_path, args.schema_path)

    if not args.skip_load or not db_exists:
        if args.skip_load and not db_exists:
            print(f"Warning: --skip-load specified but database doesn't exist. Loading data...")
        load_hand_suite(args.db_path, args.hand_dir, args.hand_version, Decimal(args.nwm_version), args.batch_size)
        print(f"\nData loaded into {args.db_path}")
    else:
        print(f"Skipping data load, using existing database: {args.db_path}")

    if args.output_dir:
        print(f"\nPartitioning tables to: {args.output_dir}")
        partition_tables_to_parquet(args.db_path, args.output_dir, args.h3_resolution)

    print(f"\nDONE.")


if __name__ == "__main__":
    main()
