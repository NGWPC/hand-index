#!/usr/bin/env python3
import argparse
import tempfile
import shutil
import os
import uuid as py_uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import concurrent.futures
from pyogrio.errors import DataSourceError
import fiona
import threading
import queue
import time

import pandas as pd
import geopandas as gpd
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
import duckdb
import fsspec

# Connection pool for DuckDB connections
connection_pool = queue.Queue()
pool_initialized = False
pool_lock = threading.Lock()

TMP = tempfile.TemporaryDirectory()


def initialize_connection_pool(db_path: str, pool_size: int = 8):
    """Initialize the connection pool."""
    global pool_initialized
    with pool_lock:
        if not pool_initialized:
            for _ in range(pool_size):
                conn = duckdb.connect(db_path)
                conn.execute("LOAD spatial;")
                connection_pool.put(conn)
            pool_initialized = True


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get a connection from the pool."""
    return connection_pool.get()


def return_connection(conn: duckdb.DuckDBPyConnection):
    """Return a connection to the pool."""
    connection_pool.put(conn)


def initialize_database(db_path: str, schema_path: str):
    """Initialize the DuckDB database with schema."""
    conn = duckdb.connect(db_path)

    # Read and execute schema
    with open(schema_path, "r") as f:
        schema_sql = f.read()

    # Execute each statement separately
    statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]
    for stmt in statements:
        if stmt:
            try:
                conn.execute(stmt)
            except Exception as e:
                print(
                    f"Warning: Failed to execute statement: {stmt[:50]}... Error: {e}"
                )

    conn.close()
    print(f"Database initialized at: {db_path}")


def fetch_local(path: str) -> str:
    """Download S3 files to local temp directory."""
    low = path.lower()
    if not low.startswith(("s3://", "s3a://")):
        return path

    fs, anon_path = fsspec.core.url_to_fs(path)
    basename = Path(anon_path).name
    fd, local_path = tempfile.mkstemp(suffix=f"{basename}", dir=str(Path(TMP.name)))
    os.close(fd)
    fs.get(anon_path, local_path)
    return local_path


def list_branch_dirs(hand_dir: str) -> List[str]:
    """List all branch directories."""
    fs, root = fsspec.core.url_to_fs(hand_dir)
    scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    branches: List[str] = []
    for info in fs.ls(root, detail=True):
        if info["type"] != "directory":
            continue
        br_root = f"{info['name']}/branches"
        if not fs.exists(br_root):
            continue
        for sub in fs.ls(br_root, detail=True):
            if sub["type"] == "directory":
                uri = f"{scheme}://{sub['name']}" if scheme != "file" else sub["name"]
                branches.append(uri)
    return branches


def read_gpkg_fallback(path: str) -> gpd.GeoDataFrame:
    """Read GPKG with fallback to Fiona."""
    try:
        return gpd.read_file(path)
    except DataSourceError:
        with fiona.open(path, driver="GPKG") as src:
            return gpd.GeoDataFrame.from_features(src, crs=src.crs)


def process_branch(args: Tuple[str, str, str]) -> bool:
    """Process one branch directory and insert data into DuckDB."""
    d, hand_ver, nwm_ver_str = args
    print(f"Processing branch: {d}")
    nwm_ver = Decimal(nwm_ver_str)

    conn = get_connection()

    try:
        conn.execute("BEGIN TRANSACTION;")

        # Note: Hand_Versions table removed from schema

        # Process catchment geometry union
        fs, anon = fsspec.core.url_to_fs(d)
        gpkg_list = fs.glob(f"{anon}/*gw_catchments*.gpkg")
        geoms = []
        catch_crs = None

        for anon_fp in gpkg_list:
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uri = f"{scheme}://{anon_fp}" if scheme != "file" else anon_fp
            loc = fetch_local(uri)
            try:
                gdf = read_gpkg_fallback(loc)
                if not gdf.empty:
                    catch_crs = catch_crs or gdf.crs.to_string()
                    geoms.append(unary_union(gdf.geometry))
            except Exception as e:
                print(f"  ERROR: could not open {loc!r} as GPKG: {e}")
            finally:
                if os.path.exists(loc):
                    os.remove(loc)

        if not geoms:
            print(f"  No catchment geometries found in {d}")
            conn.execute("ROLLBACK;")
            return False

        # Create catchment record
        merged = unary_union(geoms)
        parts = d.split(f"{hand_ver}/", 1)
        rel_uri = f"{hand_ver}/{parts[1]}" if len(parts) == 2 else d
        cid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{Path(rel_uri)}:{merged.wkt}")

        # Insert catchment (ignore if already exists)
        conn.execute(
            """
            INSERT INTO Catchments (catchment_id, hand_version_id, geometry, additional_attributes)
            VALUES (?, ?, ST_GeomFromText(?), ?)
            ON CONFLICT (catchment_id) DO NOTHING
        """,
            [str(cid), hand_ver, merged.wkt, None],
        )

        # Process hydrotables
        csvs = fs.glob(f"{anon}/hydroTable_*.csv")
        if csvs:
            pieces = []
            for anon_fp in csvs:
                scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
                uri = f"{scheme}://{anon_fp}" if scheme != "file" else anon_fp
                loc = fetch_local(uri)
                try:
                    df_part = pd.read_csv(loc)
                    pieces.append(df_part)
                except Exception as e:
                    print(f"  couldn't read CSV: {uri} because of {e}")
                finally:
                    if os.path.exists(loc):
                        os.remove(loc)

            if pieces:
                df = pd.concat(pieces, ignore_index=True)
                df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
                df["discharge_cms"] = pd.to_numeric(
                    df["discharge_cms"], errors="coerce"
                )
                df["feature_id"] = pd.to_numeric(df["feature_id"], errors="coerce")
                df["HydroID"] = df["HydroID"].astype(str)

                def first_notnull(s):
                    return s.dropna().iloc[0] if not s.dropna().empty else None

                df = df.sort_values(["HydroID", "stage"])
                grp = (
                    df.groupby("HydroID")
                    .agg(
                        nwm_feature_id_agg=("feature_id", first_notnull),
                        huc_id_agg=("HUC", first_notnull),
                        lake_id_agg=("LakeID", first_notnull),
                        stage_list=("stage", lambda v: [float(x) for x in v.dropna()]),
                        discharge_list=(
                            "discharge_cms",
                            lambda v: [float(x) for x in v.dropna()],
                        ),
                    )
                    .reset_index()
                )

                # Insert hydrotable records
                for _, r in grp.iterrows():
                    conn.execute(
                        """
                        INSERT INTO Hydrotables (
                            catchment_id, hand_version_id, HydroID, nwm_feature_id, 
                            nwm_version_id, stage, discharge_cms, huc_id, lake_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (catchment_id, hand_version_id, HydroID) DO NOTHING
                    """,
                        [
                            str(cid),
                            hand_ver,
                            r["HydroID"],
                            (
                                int(r["nwm_feature_id_agg"])
                                if pd.notna(r["nwm_feature_id_agg"])
                                else None
                            ),
                            (
                                float(nwm_ver)
                                if pd.notna(r["nwm_feature_id_agg"])
                                else None
                            ),
                            r["stage_list"],
                            r["discharge_list"],
                            str(r["huc_id_agg"]) if pd.notna(r["huc_id_agg"]) else None,
                            (
                                str(r["lake_id_agg"])
                                if pd.notna(r["lake_id_agg"])
                                else None
                            ),
                        ],
                    )

        # Process REM rasters
        rem_tifs = fs.glob(f"{anon}/*rem_zeroed*.tif")
        rem_ids = []
        if rem_tifs:
            if len(rem_tifs) > 1:
                print(f"WARNING: Multiple REM rasters found in {anon}")

            rem_tif = rem_tifs[0]
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uri = f"{scheme}://{rem_tif}" if scheme != "file" else rem_tif
            parts = uri.split(f"{hand_ver}/", 1)
            rel_uri = f"{hand_ver}/{parts[1]}" if len(parts) == 2 else uri
            rid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{cid}:{Path(rel_uri)}")
            rem_ids.append(rid)

            conn.execute(
                """
                INSERT INTO HAND_REM_Rasters (rem_raster_id, catchment_id, hand_version_id, raster_path, metadata)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (rem_raster_id) DO NOTHING
            """,
                [str(rid), str(cid), hand_ver, uri, None],
            )

        # Process catchment rasters
        catch_tifs = fs.glob(f"{anon}/*gw_catchments_reaches*.tif")
        if catch_tifs and rem_ids:
            if len(catch_tifs) > 1:
                print(f"WARNING: Multiple catchment rasters found in {anon}")

            catch_tif = catch_tifs[0]
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uri = f"{scheme}://{catch_tif}" if scheme != "file" else catch_tif
            parts = uri.split(f"{hand_ver}/", 1)
            rel_uri = f"{hand_ver}/{parts[1]}" if len(parts) == 2 else uri
            crid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{rem_ids[0]}:{Path(rel_uri)}")

            conn.execute(
                """
                INSERT INTO HAND_Catchment_Rasters (catchment_raster_id, rem_raster_id, raster_path, metadata)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (catchment_raster_id) DO NOTHING
            """,
                [str(crid), str(rem_ids[0]), uri, None],
            )

        conn.execute("COMMIT;")
        print(f"  Successfully processed branch: {d}")
        return True

    except Exception as e:
        try:
            conn.execute("ROLLBACK;")
        except:
            pass
        print(f"  ERROR processing branch {d}: {e}")
        return False
    finally:
        return_connection(conn)


def load_hand_suite(
    db_path: str,
    hand_dir: str,
    hand_ver: str,
    nwm_ver: Decimal,
):
    """Load HAND data suite into DuckDB."""
    # Initialize connection pool
    initialize_connection_pool(db_path, pool_size=8)

    # Find all branch dirs
    branch_dirs = list_branch_dirs(hand_dir)
    if not branch_dirs:
        print("No branch directories found → exiting")
        return

    print(f"Found {len(branch_dirs)} branch directories to process")

    # Process in parallel
    args_list = [(d, hand_ver, str(nwm_ver)) for d in branch_dirs]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(process_branch, args_list))

    successful = sum(results)
    print(f"Successfully processed {successful}/{len(branch_dirs)} branches")


def main():
    try:
        p = argparse.ArgumentParser()
        p.add_argument("--db-path", required=True, help="Path to DuckDB database file")
        p.add_argument(
            "--schema-path",
            default="./schema/hand-index-duckdb.sql",
            help="Path to DuckDB schema SQL file",
        )
        p.add_argument(
            "--hand-dir",
            required=True,
            help="Root of your HAND HUC8 tree (local path or s3://…)",
        )
        p.add_argument(
            "--hand-version", required=True, help="A text id for this HAND run"
        )
        p.add_argument("--nwm-version", required=True, help="NWM version (decimal)")
        p.add_argument(
            "--init-db",
            action="store_true",
            help="Initialize database with schema (use for new databases)",
        )
        args = p.parse_args()

        # Initialize database if requested
        if args.init_db:
            initialize_database(args.db_path, args.schema_path)

        hand_ver = args.hand_version
        nwm_ver = Decimal(args.nwm_version)

        load_hand_suite(args.db_path, args.hand_dir, hand_ver, nwm_ver)

        print(f"\nDONE. Data loaded into {args.db_path}")

    finally:
        if TMP:
            print(f"Cleaning up temporary directory: {TMP.name}")
            try:
                TMP.cleanup()
            except Exception as e:
                print(f"Error cleaning up temporary directory {TMP.name}: {e}")


if __name__ == "__main__":
    main()
