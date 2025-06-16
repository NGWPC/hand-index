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

SENTINEL = object()  # Shutdown signal for batch writer thread


def process_hydrotable_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process hydrotable dataframe by grouping by HydroID and converting multi-valued columns to arrays.
    """
    required_cols = ["HydroID"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["HydroID"] = df["HydroID"].astype(str)

    for col in df.columns:
        if col != "HydroID":
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            except (ValueError, TypeError):
                pass

    def aggregate_to_scalar_or_array(series):
        """Return single value if all values are the same, otherwise return array of unique values."""
        clean_values = series.dropna()
        if clean_values.empty:
            return None

        unique_values = clean_values.unique()
        if len(unique_values) == 1:
            return unique_values[0]
        else:
            return list(unique_values)

    agg_dict = {}
    for col in df.columns:
        if col != "HydroID":
            agg_dict[col] = aggregate_to_scalar_or_array

    df = df.sort_values(["HydroID"])
    try:
        grp = df.groupby("HydroID").agg(agg_dict).reset_index()
        return grp
    except Exception as e:
        print(f"  Aggregation failed: {e}")
        print(f"  DataFrame columns: {list(df.columns)}")
        print(f"  Aggregation dict: {agg_dict}")
        raise


def initialize_database(db_path: str, schema_path: str):
    """Initialize the DuckDB database with schema."""
    conn = duckdb.connect(db_path)

    with open(schema_path, "r") as f:
        schema_sql = f.read()

    statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]
    for stmt in statements:
        if stmt:
            try:
                conn.execute(stmt)
            except Exception as e:
                print(f"Warning: Failed to execute statement: {stmt[:50]}... Error: {e}")

    conn.close()
    print(f"Database initialized at: {db_path}")


@contextlib.contextmanager
def fetch_local(path: str):
    """Download S3 files to local temp directory with context manager."""
    low = path.lower()
    if not low.startswith(("s3://", "s3a://")):
        yield path

    fs, anon_path = fsspec.core.url_to_fs(path)
    basename = Path(anon_path).name
    fd, local_path = tempfile.mkstemp(suffix=f"{basename}")
    os.close(fd)
    try:
        fs.get(anon_path, local_path)
        yield local_path
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


def list_branch_dirs(hand_dir: str) -> List[str]:
    """Find all branch directories in the HAND directory structure."""
    filesystem, root_path = fsspec.core.url_to_fs(hand_dir)
    protocol = filesystem.protocol if isinstance(filesystem.protocol, str) else filesystem.protocol[0]
    branch_directories = []
    
    for directory_info in filesystem.ls(root_path, detail=True):
        if directory_info["type"] != "directory":
            continue
            
        branches_path = f"{directory_info['name']}/branches"
        if not filesystem.exists(branches_path):
            continue
            
        for branch_info in filesystem.ls(branches_path, detail=True):
            if branch_info["type"] == "directory":
                if protocol == "file":
                    full_path = branch_info["name"]
                else:
                    full_path = f"{protocol}://{branch_info['name']}"
                branch_directories.append(full_path)
                
    return branch_directories


def read_gpkg_fallback(path: str) -> gpd.GeoDataFrame:
    """Read GPKG with fallback to Fiona."""
    try:
        return gpd.read_file(path)
    except DataSourceError:
        with fiona.open(path, driver="GPKG") as src:
            return gpd.GeoDataFrame.from_features(src, crs=src.crs)


def process_branch(branch_dir: str, hand_version: str, nwm_version_str: str) -> Optional[Dict[str, Any]]:
    """Process one branch directory and return data for batch insertion."""
    print(f"Processing branch: {branch_dir}")
    nwm_version = Decimal(nwm_version_str)

    try:
        filesystem, directory_path = fsspec.core.url_to_fs(branch_dir)
        catchment_files = filesystem.glob(f"{directory_path}/*gw_catchments*.gpkg")
        geometries = []
        catchment_crs = None

        for catchment_file_path in catchment_files:
            protocol = filesystem.protocol if isinstance(filesystem.protocol, str) else filesystem.protocol[0]
            if protocol == "file":
                file_uri = catchment_file_path
            else:
                file_uri = f"{protocol}://{catchment_file_path}"
                
            with fetch_local(file_uri) as local_file:
                try:
                    geodataframe = read_gpkg_fallback(local_file)
                    if not geodataframe.empty:
                        catchment_crs = catchment_crs or geodataframe.crs.to_string()
                        geometries.append(unary_union(geodataframe.geometry))
                except Exception as e:
                    print(f"  ERROR: could not open {local_file!r} as GPKG: {e}")

        if not geometries:
            print(f"  No catchment geometries found in {branch_dir}")
            return None

        merged_geometry = unary_union(geometries)
        path_parts = branch_dir.split(f"{hand_version}/", 1)
        relative_path = f"{hand_version}/{path_parts[1]}" if len(path_parts) == 2 else branch_dir
        catchment_id = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{Path(relative_path)}:{merged_geometry.wkt}")

        result_data = {
            "catchment": {
                "catchment_id": str(catchment_id),
                "hand_version_id": hand_version,
                "geometry_wkt": merged_geometry.wkt,
                "additional_attributes": None,
            },
            "hydrotables": [],
            "rem_rasters": [],
            "catchment_rasters": [],
        }

        hydrotable_files = filesystem.glob(f"{directory_path}/hydroTable_*.csv")
        if hydrotable_files:
            csv_dataframes = []
            for hydrotable_file_path in hydrotable_files:
                if protocol == "file":
                    file_uri = hydrotable_file_path
                else:
                    file_uri = f"{protocol}://{hydrotable_file_path}"
                    
                with fetch_local(file_uri) as local_file:
                    try:
                        csv_data = pd.read_csv(local_file)
                        csv_dataframes.append(csv_data)
                    except Exception as e:
                        print(f"  couldn't read CSV: {local_file} because of {e}")

            if csv_dataframes:
                combined_csv_data = pd.concat(csv_dataframes, ignore_index=True)
                processed_hydrotables = process_hydrotable_data(combined_csv_data)

                for _, hydrotable_row in processed_hydrotables.iterrows():
                    hydrotable_record = {
                        "catchment_id": str(catchment_id),
                        "hand_version_id": hand_version,
                        "HydroID": hydrotable_row["HydroID"],
                    }

                    for column_name in processed_hydrotables.columns:
                        if column_name != "HydroID":
                            hydrotable_record[column_name] = hydrotable_row[column_name] if column_name in hydrotable_row.index else None

                    result_data["hydrotables"].append(hydrotable_record)

        rem_tifs = filesystem.glob(f"{directory_path}/*rem_zeroed*.tif")
        rem_ids = []
        if rem_tifs:
            if len(rem_tifs) > 1:
                print(f"WARNING: Multiple REM rasters found in {directory_path}")

            rem_tif = rem_tifs[0]
            protocol = filesystem.protocol if isinstance(filesystem.protocol, str) else filesystem.protocol[0]
            if protocol == "file":
                uri = rem_tif
            else:
                uri = f"{protocol}://{rem_tif}"
            parts = uri.split(f"{hand_version}/", 1)
            rel_uri = f"{hand_version}/{parts[1]}" if len(parts) == 2 else uri
            rid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{catchment_id}:{Path(rel_uri)}")
            rem_ids.append(rid)

            result_data["rem_rasters"].append(
                {
                    "rem_raster_id": str(rid),
                    "catchment_id": str(catchment_id),
                    "hand_version_id": hand_version,
                    "raster_path": uri,
                    "metadata": None,
                }
            )

        catch_tifs = filesystem.glob(f"{directory_path}/*gw_catchments_reaches*.tif")
        if catch_tifs and rem_ids:
            if len(catch_tifs) > 1:
                print(f"WARNING: Multiple catchment rasters found in {directory_path}")

            catch_tif = catch_tifs[0]
            protocol = filesystem.protocol if isinstance(filesystem.protocol, str) else filesystem.protocol[0]
            if protocol == "file":
                uri = catch_tif
            else:
                uri = f"{protocol}://{catch_tif}"
            parts = uri.split(f"{hand_version}/", 1)
            rel_uri = f"{hand_version}/{parts[1]}" if len(parts) == 2 else uri
            crid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{rem_ids[0]}:{Path(rel_uri)}")

            result_data["catchment_rasters"].append(
                {
                    "catchment_raster_id": str(crid),
                    "rem_raster_id": str(rem_ids[0]),
                    "raster_path": uri,
                    "metadata": None,
                }
            )

        print(f"  Successfully processed branch: {branch_dir}")
        return result_data

    except Exception as e:
        print(f"  ERROR processing branch {branch_dir}: {e}")
        return None


def batch_insert_data(db_path: str, batch_data: List[Dict[str, Any]]):
    """
    Perform batch insertions into the database using efficient batch operations.
    """
    if not batch_data:
        return

    conn = duckdb.connect(db_path)

    try:
        try:
            conn.execute("INSTALL spatial; LOAD spatial;")
        except Exception:
            pass

        conn.execute("BEGIN TRANSACTION;")

        catchment_records = [data["catchment"] for data in batch_data if data and "catchment" in data]
        if catchment_records:
            print(f"Batch inserting {len(catchment_records)} catchments...")
            conn.executemany(
                """
                INSERT INTO Catchments (catchment_id, hand_version_id, geometry, additional_attributes)
                VALUES (?, ?, ST_GeomFromText(?), ?)
                ON CONFLICT (catchment_id) DO NOTHING
                """,
                [
                    (
                        r["catchment_id"],
                        r["hand_version_id"],
                        r["geometry_wkt"],
                        r["additional_attributes"],
                    )
                    for r in catchment_records
                ],
            )

        all_hydrotable_records = [
            ht for data in batch_data if data and "hydrotables" in data for ht in data["hydrotables"]
        ]
        if all_hydrotable_records:
            print(f"Batch inserting {len(all_hydrotable_records)} hydrotable records...")
            all_columns = set()
            for record in all_hydrotable_records:
                all_columns.update(record.keys())
            all_columns = sorted(list(all_columns))

            conn.executemany(
                f"""
                INSERT INTO Hydrotables ({', '.join(all_columns)})
                VALUES ({', '.join(['?']*len(all_columns))})
                ON CONFLICT (catchment_id, hand_version_id, HydroID) DO NOTHING
                """,
                [tuple(rec.get(col) for col in all_columns) for rec in all_hydrotable_records],
            )

        all_rem_rasters = [rr for data in batch_data if data and "rem_rasters" in data for rr in data["rem_rasters"]]
        if all_rem_rasters:
            print(f"Batch inserting {len(all_rem_rasters)} REM rasters...")
            conn.executemany(
                """
                INSERT INTO HAND_REM_Rasters (rem_raster_id, catchment_id, hand_version_id, raster_path, metadata)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (rem_raster_id) DO NOTHING
                """,
                [
                    (
                        r["rem_raster_id"],
                        r["catchment_id"],
                        r["hand_version_id"],
                        r["raster_path"],
                        r["metadata"],
                    )
                    for r in all_rem_rasters
                ],
            )

        all_catchment_rasters = [
            cr for data in batch_data if data and "catchment_rasters" in data for cr in data["catchment_rasters"]
        ]
        if all_catchment_rasters:
            print(f"Batch inserting {len(all_catchment_rasters)} catchment rasters...")
            conn.executemany(
                """
                INSERT INTO HAND_Catchment_Rasters (catchment_raster_id, rem_raster_id, raster_path, metadata)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (catchment_raster_id) DO NOTHING
                """,
                [
                    (
                        r["catchment_raster_id"],
                        r["rem_raster_id"],
                        r["raster_path"],
                        r["metadata"],
                    )
                    for r in all_catchment_rasters
                ],
            )

        conn.execute("COMMIT;")
        print(f"Successfully batch inserted data from {len(batch_data)} branches")

    except Exception as e:
        conn.execute("ROLLBACK;")
        print(f"Error in batch insert: {e}")
        raise
    finally:
        conn.close()


def batch_writer(db_path: str, result_queue: queue.Queue, batch_size: int):
    """
    Batch writer thread that accumulates results and inserts to database
    when batch size is reached or on shutdown.
    """
    batch = []
    while True:
        item = result_queue.get()
        if item is SENTINEL:
            break

        batch.append(item)
        if len(batch) >= batch_size:
            try:
                batch_insert_data(db_path, batch)
            except Exception as e:
                print(f"CRITICAL: Batch insert failed: {e}")
                raise
            batch = []

    if batch:
        try:
            batch_insert_data(db_path, batch)
        except Exception as e:
            print(f"CRITICAL: Final batch insert failed: {e}")
            raise


def load_hand_suite(
    db_path: str,
    hand_dir: str,
    hand_ver: str,
    nwm_ver: Decimal,
    batch_size: int = 200,
):
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
    total_count = len(branch_dirs)

    # These are producers that will process branches in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        futures = [executor.submit(process_branch, branch_dir, hand_ver, str(nwm_ver)) for branch_dir in branch_dirs]

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

    print(f"Successfully processed {successful_count}/{total_count} branches")


def partition_tables_to_parquet(db_path: str, output_dir: str, h3_resolution: int = 1):
    """Partition tables from DuckDB to parquet files using H3 spatial indexing."""
    conn = duckdb.connect(db_path)

    print("Loading DuckDB extensions...")
    try:
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("INSTALL aws;")
        conn.execute("LOAD aws;")
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        conn.execute("INSTALL h3 FROM community;")
        conn.execute("LOAD h3;")
    except Exception as e:
        print(f"Error loading extensions: {e}")
        raise

    if output_dir.startswith("s3://"):
        print("Configuring AWS settings for S3 access...")
        try:
            conn.execute("SET s3_region='us-east-1';")  # Default region
            # You may need to set these if not using default AWS credentials
            # conn.execute("SET s3_access_key_id='your-access-key';")
            # conn.execute("SET s3_secret_access_key='your-secret-key';")
        except Exception as e:
            print(f"Warning: Could not configure AWS settings: {e}")

    if output_dir.startswith("s3://"):
        print("Testing S3 connectivity...")
        try:
            conn.execute("SELECT 1;")
        except Exception as e:
            print(f"Error with S3 connectivity test: {e}")
            print(
                "Make sure your AWS credentials are configured (aws configure) and you have write access to the S3 bucket."
            )
            raise

    try:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS catchments_geom_idx
              ON catchments
              USING RTREE (geometry);
        """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hydro_catchment_id ON hydrotables (catchment_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hrr_catchment_id ON hand_rem_rasters (catchment_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_hcr_rem_raster_id ON hand_catchment_rasters (rem_raster_id);")
    except Exception as e:
        print(f"Warning: Could not create indexes: {e}")

    conn.execute(f"SET VARIABLE h3_resolution = {h3_resolution};")

    if not output_dir.endswith("/"):
        output_dir += "/"

    print("Partitioning catchments table...")
    conn.execute(
        f"""
        COPY (
            SELECT
                c.*,
                -- Get centroid, transform to EPSG:4326, then get H3 cell at resolution {h3_resolution}
                h3_latlng_to_cell(
                    ST_Y(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)), -- Latitude
                    ST_X(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)), -- Longitude
                    getvariable('h3_resolution')
                ) AS h3_partition_key
            FROM catchments c
        ) TO '{output_dir}catchments/'
        WITH (FORMAT PARQUET, PARTITION_BY (h3_partition_key), OVERWRITE_OR_IGNORE 1);
    """
    )

    print("Creating catchment H3 mapping...")
    conn.execute(
        f"""
        CREATE TEMP TABLE catchment_h3_map AS
        SELECT
            catchment_id,
            h3_latlng_to_cell(
                ST_Y(ST_Transform(ST_Centroid(geometry), 'EPSG:5070', 'EPSG:4326', true)),
                ST_X(ST_Transform(ST_Centroid(geometry), 'EPSG:5070', 'EPSG:4326', true)),
                getvariable('h3_resolution')
            ) AS h3_partition_key
        FROM catchments;
    """
    )

    print("Partitioning hydrotables...")
    conn.execute(
        f"""
        COPY (
            SELECT
                ht.*,
                chm.h3_partition_key
            FROM hydrotables ht
            JOIN catchment_h3_map chm ON ht.catchment_id = chm.catchment_id
        ) TO '{output_dir}hydrotables/'
        WITH (FORMAT PARQUET, PARTITION_BY (h3_partition_key), OVERWRITE_OR_IGNORE 1);
    """
    )

    print("Exporting HAND REM rasters (unpartitioned)...")
    conn.execute(
        f"""
        COPY hand_rem_rasters TO '{output_dir}hand_rem_rasters.parquet'
        WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
    """
    )

    print("Exporting HAND catchment rasters (unpartitioned)...")
    conn.execute(
        f"""
        COPY hand_catchment_rasters TO '{output_dir}hand_catchment_rasters.parquet'
        WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
    """
    )

    print("Creating catchment H3 lookup table...")
    conn.execute(
        f"""
        COPY (
            WITH CatchmentCentroids AS (
                -- First, get the H3 cell for the centroid of each catchment
                SELECT
                    c.catchment_id,
                    h3_latlng_to_cell(
                        ST_Y(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)), -- Latitude
                        ST_X(ST_Transform(ST_Centroid(c.geometry), 'EPSG:5070', 'EPSG:4326', true)), -- Longitude
                        getvariable('h3_resolution')
                    ) as h3_centroid_cell
                FROM catchments c
                WHERE c.geometry IS NOT NULL AND NOT ST_IsEmpty(c.geometry)
            )
            SELECT
                cc.catchment_id,
                -- For each centroid cell, get the cell itself and all neighbors in a 1-cell radius (k=1)
                unnest(h3_grid_disk(cc.h3_centroid_cell, 1)) AS h3_covering_cell_key
            FROM
                CatchmentCentroids cc
        ) TO '{output_dir}catchment_h3_lookup.parquet'
        WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
    """
    )

    conn.execute("DROP TABLE IF EXISTS catchment_h3_map;")

    conn.close()
    print(f"Tables partitioned successfully to: {output_dir}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True, help="Path to DuckDB database file")
    p.add_argument(
        "--schema-path",
        default="./schema/hand-index-v0.1.sql",
        help="Path to DuckDB schema SQL file",
    )
    p.add_argument(
        "--hand-dir",
        required=True,
        help="Root of your HAND HUC8 tree (local path or s3://...)",
    )
    p.add_argument("--hand-version", required=True, help="A text id for this HAND run")
    p.add_argument("--nwm-version", required=True, help="NWM version (decimal)")
    p.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database with schema (use for new databases)",
    )
    p.add_argument(
        "--output-dir",
        help="Output directory for partitioned parquet files (local path or s3://...). If provided, will partition tables after loading.",
    )
    p.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip loading data to .ddb file if it already exists, only partition existing .ddb",
    )
    p.add_argument(
        "--h3-resolution",
        type=int,
        default=1,
        help="H3 resolution for spatial partitioning (default: 1)",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of branches to process in each batch (default: 100)",
    )
    args = p.parse_args()

    db_exists = os.path.exists(args.db_path)

    if args.init_db and db_exists:
        print(f"Error: Database {args.db_path} already exists. Cannot initialize existing database.")
        print("Remove the existing database file or use a different path.")
        exit(1)

    # Validate output directory before starting any processing
    if args.output_dir:
        if args.output_dir.startswith("s3://"):
            try:
                fs, output_path = fsspec.core.url_to_fs(args.output_dir)
                if fs.exists(output_path):
                    print(f"Error: S3 output directory {args.output_dir} already exists.")
                    print("Remove the existing directory or use a different path.")
                    exit(1)
                print(f"S3 output directory {args.output_dir} validated - will be created during partitioning")
            except Exception as e:
                print(f"Error: Could not validate S3 output directory {args.output_dir}: {e}")
                exit(1)
        else:
            output_path = Path(args.output_dir)
            if output_path.exists():
                print(f"Error: Output directory {args.output_dir} already exists.")
                print("Remove the existing directory or use a different path.")
                exit(1)
            else:
                try:
                    output_path.mkdir(parents=True, exist_ok=False)
                    print(f"Created output directory: {args.output_dir}")
                except Exception as e:
                    print(f"Error: Could not create output directory {args.output_dir}: {e}")
                    exit(1)

    if args.init_db:
        initialize_database(args.db_path, args.schema_path)

    if not args.skip_load or not db_exists:
        hand_ver = args.hand_version
        nwm_ver = Decimal(args.nwm_version)

        if args.skip_load and not db_exists:
            print(f"Warning: --skip-load specified but database {args.db_path} does not exist. Loading data...")

        load_hand_suite(args.db_path, args.hand_dir, hand_ver, nwm_ver, args.batch_size)
        print(f"\nData loaded into {args.db_path}")
    else:
        print(f"Skipping data load, using existing database: {args.db_path}")

    if args.output_dir:
        print(f"\nPartitioning tables to: {args.output_dir}")
        partition_tables_to_parquet(args.db_path, args.output_dir, args.h3_resolution)

    print(f"\nDONE.")


if __name__ == "__main__":
    main()
