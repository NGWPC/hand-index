import argparse
import gc
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path

import duckdb
import fsspec
import s3fs


def download_s3_file(s3_path: str, local_path: str, fs: s3fs.S3FileSystem) -> str:
    """Download a single file from S3 to local filesystem. This gets used to download many files in parallel in load_hand_suite when working with the catchment gpkg's. Downloading the gpkg data locally ended up being faster. Most likely because of high-latency seeks in GPKG files when trying to interact with them over S3."""
    try:
        fs.get(s3_path, local_path)
        return local_path
    except Exception as e:
        print(f"Failed to download {s3_path}: {e}")
        return None


def load_hand_suite(
    db_path: str,
    hand_dir: str,
    hand_version: str,
    nwm_version: str,
    h3_resolution: int,
    calb: bool,
    batch_size: int = 100,
):
    """
    Loads HAND data into DuckDB using parallel downloads and processing.
    """
    start_time = time.time()

    hand_dir = hand_dir.rstrip("/") + "/"
    base_glob_path = f"{hand_dir}*/branches/*"
    gpkg_glob = f"{base_glob_path}/*gw_catchments*.gpkg"
    csv_glob_path = f"{hand_dir}*/" if calb else f"{base_glob_path}/"
    csv_glob = f"{csv_glob_path}hydroTable_*.csv"
    catch_glob = f"{base_glob_path}/*gw_catchments_reaches*.tif"
    rem_glob = f"{base_glob_path}/*rem_zeroed*.tif"

    conn = duckdb.connect(db_path)
    try:
        print("\nLoading extensions (httpfs, spatial, h3, aws)...")
        conn.execute("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial;")
        conn.execute("INSTALL h3 FROM community; LOAD h3;")
        conn.execute("INSTALL aws; LOAD aws;")

        print("Processing and inserting Catchment geometries...")

        # First get the list of files, then build a dynamic query
        file_list_query = f"SELECT file FROM glob('{gpkg_glob}')"
        files = conn.execute(file_list_query).fetchall()

        if not files:
            print("WARNING: No GeoPackage files found. Skipping catchment ingestion.")
            return

        print(f"Found {len(files)} GeoPackage files to process.")

        # Group files by branch directory and pick one file per branch (excluding gw_catchments_pixels_ files since they are different than the other gw_catchment files)
        print("Grouping files by branch directory...")
        branch_to_file = {}
        for (file_path,) in files:
            if "/branches/" in file_path and "_pixels_" not in file_path:
                # Extract branch directory
                parts = file_path.split("/branches/")
                if len(parts) > 1:
                    branch_dir = parts[0] + "/branches/" + parts[1].split("/")[0] + "/"
                    if branch_dir not in branch_to_file:
                        branch_to_file[branch_dir] = file_path  # Just take the first file found

        print(f"Found {len(branch_to_file)} unique branches from {len(files)} files.")

        # Process files in batches
        branch_files = list(branch_to_file.items())
        total_inserted = 0

        # Initialize S3 filesystem for parallel downloads if needed
        fs = s3fs.S3FileSystem() if hand_dir.startswith("s3://") else None

        for i in range(0, len(branch_files), batch_size):
            batch_branches = branch_files[i : i + batch_size]
            print(
                f"Processing batch {i//batch_size + 1}/{(len(branch_files) + batch_size - 1)//batch_size} ({len(batch_branches)} branches)..."
            )

            # Download files in parallel if using S3
            local_file_paths = []
            temp_dir = None

            if fs:
                temp_dir = tempfile.mkdtemp()

                with ThreadPoolExecutor(max_workers=8) as executor:
                    # Submit download tasks
                    future_to_file = {}
                    for j, (branch_dir, file_path) in enumerate(batch_branches):
                        local_path = os.path.join(temp_dir, f"batch_{i}_{j}.gpkg")
                        future = executor.submit(download_s3_file, file_path, local_path, fs)
                        future_to_file[future] = (branch_dir, file_path, local_path)

                    # Collect results
                    for future in as_completed(future_to_file):
                        branch_dir, original_path, local_path = future_to_file[future]
                        result = future.result()
                        if result:
                            local_file_paths.append((branch_dir, original_path, local_path))
                        else:
                            print(f"Skipping failed download: {original_path}")

                print(f"Successfully downloaded {len(local_file_paths)} files.")
            else:
                # Use original paths for local files
                local_file_paths = [(branch_dir, file_path, file_path) for branch_dir, file_path in batch_branches]

            # Process each file individually - directly insert all geometries from GPKG
            print(f"Processing {len(local_file_paths)} files...")
            batch_inserted = 0

            # Batch files to reduce round trips while avoiding single giant query
            for i in range(0, len(local_file_paths), batch_size):
                batch = local_file_paths[i : i + batch_size]

                # Build single query with all file reads, then process geometries
                file_reads = []
                for branch_dir, _, local_path in batch:
                    escaped_path = local_path.replace("'", "''")
                    file_reads.append(
                        f"SELECT geom, '{branch_dir}' AS branch_path FROM ST_Read('{escaped_path}') WHERE geom IS NOT NULL"
                    )

                batch_insert_sql = f"""
                INSERT INTO Catchments (catchment_id, hand_version_id, geometry, h3_index, branch_path)
                WITH all_geoms AS (
                    {' UNION ALL '.join(file_reads)}
                ),
                merged_geoms AS (
                    SELECT 
                        branch_path,
                        COUNT(*) AS geom_count,
                        ST_Simplify(ST_Union_Agg(geom), 100) AS merged_geom
                    FROM all_geoms
                    GROUP BY branch_path
                )
                SELECT
                    uuid() AS catchment_id,
                    '{hand_version}' AS hand_version_id,
                    ST_AsText(ST_Force2D(merged_geom)) AS geometry,
                    h3_latlng_to_cell(
                        ST_Y(ST_Transform(ST_Centroid(merged_geom), 'EPSG:5070', 'EPSG:4326', true)),
                        ST_X(ST_Transform(ST_Centroid(merged_geom), 'EPSG:5070', 'EPSG:4326', true)),
                        {h3_resolution}
                    ) AS h3_index,
                    branch_path
                FROM merged_geoms
                WHERE merged_geom IS NOT NULL
                ON CONFLICT (catchment_id) DO NOTHING;
                """

                try:
                    result = conn.execute(batch_insert_sql)
                    records_inserted = result.rowcount if hasattr(result, "rowcount") else len(batch)
                    batch_inserted += records_inserted
                except Exception as e:
                    print(f"Error processing batch starting at index {i}: {e}")
                    continue

                # clean up any intermediate/cached results. Reconnecting to duckdb resets its memory usage
                conn.close()
                conn = duckdb.connect(db_path)
                conn.execute("LOAD httpfs; LOAD spatial; LOAD h3; LOAD aws;")

            total_inserted += batch_inserted
            print(f"-> Batch inserted {batch_inserted} catchment records.")

            # Clean up temporary files
            if temp_dir and os.path.exists(temp_dir):
                import shutil

                shutil.rmtree(temp_dir)

        print(f"-> Total inserted/updated {total_inserted} catchment records.")

        # Loading entire catchments table first for data integrity. Catchments must exist before Hydrotables can reference them.

        print("\nProcessing and inserting HydroTable data (with h3_index)...")
        csv_dir_extractor = f"regexp_extract(filename, '(.*/{'branches/[^/]+/' if not calb else '[^/]+/'})')"

        # Build dynamic aggregation SQL using DuckDB metadata. Doing it this way so that this script doesn't need to have any knowledge of the columns in the hydrotable.csv's. As long as the hydrotables schema has the correct numeric and numeric array types and the same column names as the csv's you are ingesting then the code is able to figure out which columns in hydrotable csv need to be aggregated to arrays and which columns to leave as scalars. Aggregation is by HydroID
        agg_sql = conn.execute("""
            WITH column_info AS (
                SELECT column_name, data_type,
                       CASE 
                           WHEN data_type LIKE '%[]%' THEN 'ARRAY_AGG(raw_csv_data."' || column_name || '") AS "' || column_name || '"'
                           ELSE 'FIRST(raw_csv_data."' || column_name || '") AS "' || column_name || '"'
                       END AS agg_clause
                FROM information_schema.columns 
                WHERE table_name = 'Hydrotables' AND table_schema = 'main'
                AND column_name NOT IN ('catchment_id', 'hand_version_id', 'HydroID', 'nwm_version_id', 'h3_index')
            )
            SELECT string_agg(agg_clause, ', ') AS dynamic_agg_clause
            FROM column_info
        """).fetchone()[0]

        # First, get list of all CSV files to process
        print("Discovering CSV files...")
        # Use glob to get file list without reading CSV content
        csv_files_query = f"SELECT file FROM glob('{csv_glob}')"
        csv_files = [row[0] for row in conn.execute(csv_files_query).fetchall()]
        print(f"Found {len(csv_files)} CSV files to process")

        # Process CSVs in batches using the provided batch_size parameter
        total_inserted = 0

        for i in range(0, len(csv_files), batch_size):
            batch_files = csv_files[i : i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(csv_files) + batch_size - 1) // batch_size
            print(f"Processing batch {batch_num}/{total_batches} ({len(batch_files)} files)...")

            # Create a union of the batch files
            file_unions = " UNION ALL ".join(
                [
                    f"SELECT {csv_dir_extractor} AS csv_dir, * FROM read_csv_auto('{file}', filename=TRUE, union_by_name=TRUE, all_varchar=TRUE)"
                    for file in batch_files
                ]
            )

            hydrotable_insert_sql = f"""
            WITH raw_csv_data AS (
                {file_unions}
            )
            INSERT INTO Hydrotables
            SELECT
                c.catchment_id,
                '{hand_version}' as hand_version_id,
                raw_csv_data.HydroID,
                '{nwm_version}' as nwm_version_id,
                FIRST(c.h3_index) as h3_index,
                {agg_sql}
            FROM raw_csv_data
            JOIN Catchments c ON c.branch_path = raw_csv_data.csv_dir
            WHERE "HydroID" IS NOT NULL AND "HydroID" != ''
            GROUP BY c.catchment_id, raw_csv_data.HydroID
            ON CONFLICT (catchment_id, hand_version_id, HydroID) DO NOTHING;
            """

            try:
                result = conn.execute(hydrotable_insert_sql)
                rowcount = result.rowcount if hasattr(result, "rowcount") else 0
                total_inserted += rowcount
                print(f"  -> Batch {batch_num} inserted.")
            except Exception as e:
                print(f"  -> Error in batch {batch_num}: {e}")
                # Continue with next batch instead of failing completely
                continue

            # clean up any intermediate/cached results. Reconnecting to duckdb resets its memory usage
            conn.close()
            conn = duckdb.connect(db_path)
            conn.execute("LOAD httpfs; LOAD spatial; LOAD h3; LOAD aws;")

        print(f"-> Total inserted/updated {total_inserted} hydrotable records.")

        # Process REM rasters
        print("\nProcessing and inserting REM rasters...")

        rem_insert_sql = f"""
        INSERT INTO HAND_REM_Rasters (rem_raster_id, catchment_id, hand_version_id, raster_path, metadata)
        SELECT
            uuid() AS rem_raster_id,
            c.catchment_id,
            '{hand_version}' AS hand_version_id,
            rem_files.file AS raster_path,
            json_object('branch_dir', regexp_extract(rem_files.file, '(.*/branches/[^/]+/)')) AS metadata
        FROM (SELECT file FROM glob('{rem_glob}')) AS rem_files
        JOIN Catchments c ON c.branch_path = regexp_extract(rem_files.file, '(.*/branches/[^/]+/)')
        ON CONFLICT (rem_raster_id) DO NOTHING;
        """

        result = conn.execute(rem_insert_sql)
        rem_count = result.rowcount if hasattr(result, "rowcount") else 0
        print(f"-> Inserted {rem_count} REM raster records.")

        # Process catchment rasters
        print("\nProcessing and inserting catchment rasters...")

        catch_insert_sql = f"""
        INSERT INTO HAND_Catchment_Rasters (catchment_raster_id, rem_raster_id, raster_path, metadata)
        SELECT
            uuid() AS catchment_raster_id,
            r.rem_raster_id,
            catch_files.file AS raster_path,
            json_object('branch_dir', regexp_extract(catch_files.file, '(.*/branches/[^/]+/)')) AS metadata
        FROM (SELECT file FROM glob('{catch_glob}')) AS catch_files
        JOIN HAND_REM_Rasters r ON json_extract_string(r.metadata, '$.branch_dir') = regexp_extract(catch_files.file, '(.*/branches/[^/]+/)')
        ON CONFLICT (catchment_raster_id) DO NOTHING;
        """

        result = conn.execute(catch_insert_sql)
        catch_count = result.rowcount if hasattr(result, "rowcount") else 0
        print(f"-> Inserted {catch_count} catchment raster records.")

    except Exception as e:
        print(f"\nFATAL ERROR during data ingestion: {e}")
        raise
    finally:
        conn.close()
        print(f"--- Ingestion Finished in {time.time() - start_time:.2f} seconds ---")


def partition_tables_to_parquet(db_path: str, output_dir: str):
    """Exports tables from DuckDB to a partitioned Parquet dataset."""
    print(f"\n--- Starting Parquet Export to {output_dir} ---")
    with duckdb.connect(db_path, read_only=True) as conn:
        print("Loading extensions for S3 export (httpfs, aws)...")
        conn.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")
        output_dir = output_dir.rstrip("/") + "/"

        # Export partitioned tables
        partitioned_tables = ["Catchments", "Hydrotables"]
        for table in partitioned_tables:
            start_time = time.time()
            partition_path = f"{output_dir}{table.lower()}/"
            print(f"  Exporting '{table}' to '{partition_path}' (partitioned by h3_index)...")
            conn.execute(f"""
                COPY (SELECT * FROM {table} WHERE h3_index IS NOT NULL)
                TO '{partition_path}'
                WITH (FORMAT PARQUET, PARTITION_BY (h3_index), OVERWRITE_OR_IGNORE 1);
            """)
            print(f"  -> Finished '{table}' in {time.time() - start_time:.2f} seconds.")

        # Export non-partitioned raster tables as single files
        single_file_tables = ["HAND_REM_Rasters", "HAND_Catchment_Rasters"]
        for table in single_file_tables:
            start_time = time.time()
            file_path = f"{output_dir}{table.lower()}.parquet"
            print(f"  Exporting '{table}' to '{file_path}' (single file)...")
            conn.execute(f"""
                COPY {table}
                TO '{file_path}'
                WITH (FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);
            """)
            print(f"  -> Finished '{table}' in {time.time() - start_time:.2f} seconds.")

    print(f"\n--- Data successfully exported to: {output_dir} ---")


def main():
    p = argparse.ArgumentParser(description="HAND data loader and exporter for DuckDB.")
    p.add_argument("--db-path", required=True)
    p.add_argument(
        "--schema-path",
        default="./schema/hand-index-ver-fim100-uncalb.sql",
        help="Path to SQL schema file (default: ./schema/hand-index-ver-fim100-uncalb.sql)",
    )
    p.add_argument("--hand-dir", required=True)
    p.add_argument("--hand-version", required=True)
    p.add_argument("--nwm-version", required=True)
    p.add_argument("--h3-resolution", type=int, default=1)
    p.add_argument("--calb", action="store_true")
    p.add_argument("--skip-load", action="store_true")
    p.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for processing files. The larger the tables that will be created the smaller this needs to be to avoid running out of memory while running the queries.",
    )
    p.add_argument("--output-dir", help="If provided, export tables to this S3 or local directory.")
    args = p.parse_args()

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

    # Check if database already exists unless we're skipping load
    if not args.skip_load:
        if os.path.exists(args.db_path):
            print(
                f"Error: Database file already exists at {args.db_path}. Remove it or use --skip-load to work with existing database."
            )
            return
        # Initialize new database with schema
        with duckdb.connect(args.db_path) as conn:
            with open(args.schema_path, "r") as f:
                schema_sql = f.read()
                conn.execute(schema_sql)
        print(f"Database initialized at '{args.db_path}' using schema '{args.schema_path}'.")

    if not args.skip_load:
        load_hand_suite(
            db_path=args.db_path,
            hand_dir=args.hand_dir,
            hand_version=args.hand_version,
            nwm_version=str(Decimal(args.nwm_version)),
            h3_resolution=args.h3_resolution,
            calb=args.calb,
            batch_size=args.batch_size,
        )

    if args.output_dir:
        partition_tables_to_parquet(args.db_path, args.output_dir)

    print("\nWorkflow Complete.")


if __name__ == "__main__":
    main()
