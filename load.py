#!/usr/bin/env python3
import argparse
import tempfile
import shutil
import os
import yaml
import uuid as py_uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Type
import concurrent.futures
from pyogrio.errors import DataSourceError
import fiona

import pandas as pd
import geopandas as gpd
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from pydantic import BaseModel, create_model, ValidationError, Field, ConfigDict
import pyarrow as pa
import pyarrow.parquet as pq
import fsspec

# Load your YAML schema
SCHEMA_PATH = "./schema/hand-index-v0.1.yml"
with open(SCHEMA_PATH) as f:
    SCHEMA = yaml.safe_load(f)

PARENT_KEYS: Dict[str, Set[Any]] = {}
TMP = tempfile.TemporaryDirectory()


# Type‐conversion helpers
def to_decimal(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except InvalidOperation:
        return None


def to_uuid(v: Any) -> Optional[py_uuid.UUID]:
    if v is None:
        return None
    if isinstance(v, py_uuid.UUID):
        return v
    try:
        return py_uuid.UUID(str(v))
    except ValueError:
        return None


# YAML to Python type map
TYPE_MAPPING = {
    "TEXT": (str, ...),
    "INTEGER": (int, ...),
    "BIGINT": (int, ...),
    "DECIMAL": (Optional[Decimal], None),
    "REAL": (Optional[float], None),
    "BOOLEAN": (Optional[bool], None),
    "UUID": (Optional[py_uuid.UUID], None),
    "JSONB": (Optional[dict], None),
    "GEOMETRY": (Optional[BaseGeometry], None),
    "LIST_DECIMAL": (Optional[List[Decimal]], None),
}


# Build Pydantic models dynamically
def build_models(schema: Dict) -> Dict[str, Type[BaseModel]]:
    models: Dict[str, Type[BaseModel]] = {}
    for tbl, tbl_def in schema["tables"].items():
        fields: Dict[str, Tuple[Any, Any]] = {}
        for col, props in tbl_def["columns"].items():
            col_type = props["type"]
            if col_type not in TYPE_MAPPING:
                raise ValueError(f"Unknown type '{col_type}' for {tbl}.{col}")
            py_type, default = TYPE_MAPPING[col_type]
            # force required if not nullable and no default
            if not props.get("nullable", True) and default is None:
                default = ...
            extras: Dict[str, Any] = {}
            if props.get("primary_key"):
                extras["primary_key"] = True
            if props.get("foreign_key"):
                extras["foreign_key"] = props["foreign_key"]
            if extras:
                fields[col] = (py_type, Field(default, **extras))
            else:
                fields[col] = (py_type, default)

        model = create_model(
            tbl,
            __base__=BaseModel,
            __config__=ConfigDict(arbitrary_types_allowed=True),
            **fields,
        )
        models[tbl] = model
    return models


MODELS = build_models(SCHEMA)


# Register PK values for FK checks
def register_parent_keys(df: pd.DataFrame, tbl: str):
    tbl_def = SCHEMA["tables"][tbl]
    pk_cols = tbl_def.get("primary_key_columns") or [
        c for c, p in tbl_def["columns"].items() if p.get("primary_key")
    ]
    if not pk_cols:
        return

    key: str
    if len(pk_cols) == 1:
        c = pk_cols[0]
        vals = set(df[c].dropna().tolist())
        if tbl_def["columns"][c]["type"] == "UUID":
            vals = {to_uuid(v) for v in vals}
        key = f"{tbl}.{c}"
        PARENT_KEYS.setdefault(key, set()).update(vals)
    else:
        sub = df[pk_cols].dropna(how="any")
        tup = {tuple(r) for r in sub.to_records(index=False)}
        key = f"{tbl}.({','.join(pk_cols)})"
        PARENT_KEYS.setdefault(key, set()).update(tup)


# validator + writer
def generic_validate_and_write(
    records: List[Dict[str, Any]],
    tbl: str,
    outdir: str,
    is_geo: bool = False,
    crs: Optional[str] = None,
):
    Model = MODELS[tbl]
    valid, invalid = [], []
    for idx, rec in enumerate(records):
        # coerce DECIMAL/UUID/LIST_DECIMAL
        for k, v in list(rec.items()):
            col_def = SCHEMA["tables"][tbl]["columns"].get(k)
            if not col_def:
                continue
            t = col_def["type"]
            if t == "DECIMAL":
                rec[k] = to_decimal(v)
            elif t == "UUID":
                rec[k] = to_uuid(v)
            elif t == "LIST_DECIMAL" and v is not None:
                rec[k] = [to_decimal(x) for x in v]
        try:
            inst = Model(**rec)
            row = inst.model_dump(exclude_none=True)
            # retain shapely geometry if present
            if "geometry" in rec:
                row["geometry"] = rec["geometry"]
            valid.append(row)
        except ValidationError as e:
            invalid.append((idx, e.errors()))

    if invalid:
        print(f" {len(invalid)}/{len(records)} invalid rows in {tbl}")
    if not valid:
        print(f"→ skipping {tbl} (no valid rows)")
        return

    df = pd.DataFrame(valid)

    # convert all UUID columns to strings
    for col, props in SCHEMA["tables"][tbl]["columns"].items():
        if props["type"] == "UUID" and col in df:
            df[col] = df[col].astype(str)

    fn = SCHEMA["tables"][tbl]["file_path"]
    target_uri = f"{outdir.rstrip('/')}/{fn}"

    fs, anon = fsspec.core.url_to_fs(target_uri)
    with fs.open(anon, "wb") as out_f:
        if is_geo and "geometry" in df.columns:
            # Build GeoDataFrame
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=crs or "EPSG:5070")

            # True GeoParquet write (injects GeoParquet metadata)
            # Pass the opened file‐handle directly
            gdf.to_parquet(
                out_f,
                engine="pyarrow",
                compression="snappy",
                index=False,
            )
            register_parent_keys(gdf, tbl)
            print(f"wrote {len(valid)} rows → {tbl} (GeoParquet)")

        else:
            # Plain Parquet: build Arrow table, attach custom metadata, then write
            table = pa.Table.from_pandas(df, preserve_index=False)
            existing = table.schema.metadata or {}
            meta = {
                b"custom_schema_name": SCHEMA["schema_name"].encode(),
                b"custom_schema_version": SCHEMA["schema_version"].encode(),
                b"custom_yaml_schema_src": Path(SCHEMA_PATH).name.encode(),
            }
            new_schema = table.schema.with_metadata({**existing, **meta})

            pq.write_table(table.cast(new_schema), out_f, compression="snappy")
            register_parent_keys(df, tbl)
            print(f"wrote {len(valid)} rows → {tbl} (Parquet)")


def fetch_local(path: str) -> str:
    """If `path` is an S3 URL, download it atomically to a unique
    temp file and return that local filename; otherwise return
    `path` itself."""
    low = path.lower()
    if not low.startswith(("s3://", "s3a://")):
        return path

    # Turn URL into a filesystem + "anonymous" path
    fs, anon_path = fsspec.core.url_to_fs(path)

    # Grab just the basename
    basename = Path(anon_path).name

    fd, local_path = tempfile.mkstemp(suffix=f"{basename}", dir=str(Path(TMP.name)))
    os.close(fd)  # close the fd so fs.get can open it

    fs.get(anon_path, local_path)
    return local_path


def list_branch_dirs(hand_dir: str) -> List[str]:
    fs, root = fsspec.core.url_to_fs(hand_dir)
    scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    branches: List[str] = []
    for info in fs.ls(root, detail=True):
        if info["type"] != "directory":
            continue
        huc8 = info["name"]
        br_root = f"{info['name']}/branches"
        if not fs.exists(br_root):
            continue
        for sub in fs.ls(br_root, detail=True):
            if sub["type"] == "directory":
                uri = f"{scheme}://{sub['name']}" if scheme != "file" else sub["name"]
                branches.append(uri)
    return branches


# Helper function to handle possible errors when using pyogrio to read gpkgs
def read_gpkg_fallback(path: str) -> gpd.GeoDataFrame:
    try:
        # first try Pyogrio (the default in GeoPandas >=0.12)
        return gpd.read_file(path)
    except DataSourceError:
        # fallback to Fiona with explicit driver
        with fiona.open(path, driver="GPKG") as src:
            return gpd.GeoDataFrame.from_features(src, crs=src.crs)


def process_branch(args: Tuple[str, str, str]) -> Tuple[
    Optional[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    Optional[str],
]:
    """
    Process one branch directory. Returns:
      - a single catchment record (or None if no catchments)
      - list of hydro records
      - list of rem raster records
      - list of catchment raster records
      - the CRS string for this branch's GPKG (or None)
    """
    d, hand_ver, nwm_ver_str = args
    print(f"Processing branch: {d}")
    nwm_ver = Decimal(nwm_ver_str)

    catch_rec: Optional[Dict[str, Any]] = None
    hydro_recs: List[Dict[str, Any]] = []
    rem_recs: List[Dict[str, Any]] = []
    cr_recs: List[Dict[str, Any]] = []
    catch_crs: Optional[str] = None

    # Catchment geometry union
    fs, anon = fsspec.core.url_to_fs(d)
    gpkg_list = fs.glob(f"{anon}/*gw_catchments*.gpkg")
    geoms = []
    for anon_fp in gpkg_list:
        scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
        uri = f"{scheme}://{anon_fp}" if scheme != "file" else anon_fp
        loc = fetch_local(uri)
        try:
            gdf = read_gpkg_fallback(loc)
        except Exception:
            print(f"  ERROR: could not open {loc!r} as GPKG")
            continue
        if gdf.empty:
            continue
        catch_crs = catch_crs or gdf.crs.to_string()
        geoms.append(unary_union(gdf.geometry))

    if geoms:
        merged = unary_union(geoms)
        parts = uri.split(f"{hand_ver}/", 1)
        # relative uri for everything after hand version makes it so that uuid's for a hand run should be the same whether on local or s3
        rel_uri = f"{hand_ver}/{parts[1]}" if len(parts) == 2 else uri
        cid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{Path(rel_uri)}:{merged.wkt}")
        catch_rec = {
            "catchment_id": cid,
            "hand_version_id": hand_ver,
            "geometry": merged,
            "additional_attributes": None,
        }

    # If no catchment, skip the rest
    if catch_rec is None:
        return None, [], [], [], None

    # Hydrotable
    csvs = fs.glob(f"{anon}/hydroTable_*.csv")
    if csvs:
        pieces = []
        for anon_fp in csvs:
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uri = f"{scheme}://{anon_fp}" if scheme != "file" else anon_fp
            loc = fetch_local(uri)
            info = fs.info(anon_fp)
            try:
                df_part = pd.read_csv(loc)
            except Exception as e:
                print(f"  couldn't read CSV: {uri} because of {e}")
                continue
            pieces.append(pd.read_csv(loc))
        if pieces:
            df = pd.concat(pieces, ignore_index=True)
            df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
            df["discharge_cms"] = pd.to_numeric(df["discharge_cms"], errors="coerce")
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
                    stage_list=("stage", lambda v: [to_decimal(x) for x in v.dropna()]),
                    discharge_list=(
                        "discharge_cms",
                        lambda v: [to_decimal(x) for x in v.dropna()],
                    ),
                )
                .reset_index()
            )
            for _, r in grp.iterrows():
                hydro_recs.append(
                    {
                        "catchment_id": cid,
                        "hand_version_id": hand_ver,
                        "HydroID": r["HydroID"],
                        "nwm_feature_id": (
                            int(r["nwm_feature_id_agg"])
                            if pd.notna(r["nwm_feature_id_agg"])
                            else None
                        ),
                        "nwm_version_id": (
                            nwm_ver if pd.notna(r["nwm_feature_id_agg"]) else None
                        ),
                        "stage": r["stage_list"],
                        "discharge_cms": r["discharge_list"],
                        "huc_id": (
                            str(r["huc_id_agg"]) if pd.notna(r["huc_id_agg"]) else None
                        ),
                        "lake_id": (
                            str(r["lake_id_agg"])
                            if pd.notna(r["lake_id_agg"])
                            else None
                        ),
                    }
                )

    # REM Raster
    rem_ids: List[py_uuid.UUID] = []
    rem_tifs = fs.glob(f"{anon}/*rem_zeroed*.tif")
    if not rem_tifs:
        print(f"WARNING: No REM rasters found in {anon}")
    else:
        if len(rem_tifs) > 1:
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uris = [f"{scheme}://{t}" if scheme != "file" else t for t in rem_tifs]
            print(
                f"WARNING: Multiple REM rasters found in {anon}:\n  "
                + "\n  ".join(uris)
            )
        # Always use the first
        rem_tif = rem_tifs[0]
        scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
        uri = f"{scheme}://{rem_tif}" if scheme != "file" else rem_tif
        parts = uri.split(f"{hand_ver}/", 1)
        rel_uri = f"{hand_ver}/{parts[1]}" if len(parts) == 2 else uri
        rid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{cid}:{Path(rel_uri)}")
        rem_ids.append(rid)
        rem_recs.append(
            {
                "rem_raster_id": rid,
                "catchment_id": cid,
                "hand_version_id": hand_ver,
                "raster_path": uri,
                "metadata": None,
            }
        )

    # Catchment Rasters
    catch_tifs = fs.glob(f"{anon}/*gw_catchments_reaches*.tif")
    if not catch_tifs:
        print(f"WARNING: No catchment rasters found in {anon}")
    elif rem_ids:
        if len(catch_tifs) > 1:
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uris = [f"{scheme}://{t}" if scheme != "file" else t for t in catch_tifs]
            # print(
            #     f"WARNING: Multiple catchment rasters found in {anon}:\n  "
            #     + "\n  ".join(uris)
            # )
        # Always use the first
        catch_tif = catch_tifs[0]
        scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
        uri = f"{scheme}://{catch_tif}" if scheme != "file" else catch_tif
        parts = uri.split(f"{hand_ver}/", 1)
        rel_uri = f"{hand_ver}/{parts[1]}" if len(parts) == 2 else uri
        crid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{rem_ids[0]}:{Path(rel_uri)}")
        cr_recs.append(
            {
                "catchment_raster_id": crid,
                "rem_raster_id": rem_ids[0],
                "raster_path": uri,
                "metadata": None,
            }
        )

    return catch_rec, hydro_recs, rem_recs, cr_recs, catch_crs


def load_hand_suite(
    outdir: str,
    hand_dir: str,
    hand_ver: str,
    nwm_ver: Decimal,
):
    # 1) find all branch dirs once
    branch_dirs = list_branch_dirs(hand_dir)
    if not branch_dirs:
        print("No branch directories found → exiting")
        return

    # 2) dispatch in parallel
    args_list = [(d, hand_ver, str(nwm_ver)) for d in branch_dirs]
    all_catch: List[Dict[str, Any]] = []
    all_hydro: List[Dict[str, Any]] = []
    all_rem: List[Dict[str, Any]] = []
    all_cr: List[Dict[str, Any]] = []
    catch_crs_list: List[str] = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for catch_rec, hydro, rem, cr, crs in executor.map(process_branch, args_list):
            if catch_rec:
                all_catch.append(catch_rec)
                if crs:
                    catch_crs_list.append(crs)
            all_hydro.extend(hydro)
            all_rem.extend(rem)
            all_cr.extend(cr)

    # pick first CRS or default
    catch_crs = catch_crs_list[0] if catch_crs_list else "EPSG:5070"

    # 3) validate + write each table exactly once
    generic_validate_and_write(
        all_catch, "Catchments", outdir, is_geo=True, crs=catch_crs
    )
    generic_validate_and_write(all_hydro, "Hydrotables", outdir)
    generic_validate_and_write(all_rem, "HAND_REM_Rasters", outdir)
    generic_validate_and_write(all_cr, "HAND_Catchment_Rasters", outdir)


def main():
    try:
        p = argparse.ArgumentParser()
        p.add_argument(
            "--output-dir",
            required=True,
            help="where to write your Parquet/Geoparquet files",
        )
        p.add_argument(
            "--hand-dir",
            required=True,
            help="root of your HAND HUC8 tree (local path or s3://…)",
        )
        p.add_argument(
            "--hand-version", required=True, help="a text id for this HAND run"
        )
        p.add_argument("--nwm-version", required=True, help="NWM version (decimal)")
        args = p.parse_args()

        outdir_uri = args.output_dir.rstrip("/")
        if outdir_uri.startswith(("s3://", "s3a://")):
            outdir = outdir_uri
        else:
            Path(outdir_uri).mkdir(parents=True, exist_ok=True)
            outdir = outdir_uri

        hand_ver = args.hand_version
        nwm_ver = Decimal(args.nwm_version)

        if hand_ver not in str(outdir):
            p.error(
                f"--hand-dir ('{args.hand_dir}') must contain the hand-version '{hand_ver}'"
            )
        if hand_ver not in str(outdir):
            p.error(
                f"--output-dir ('{outdir}') must contain the hand-version '{hand_ver}'"
            )

        load_hand_suite(outdir, args.hand_dir, hand_ver, nwm_ver)

        print("\nDONE. Parent Key registry summary:")
        for k, v in PARENT_KEYS.items():
            print(f"  {k}: {len(v)} keys")
    finally:
        if TMP:  # Check if TMP was successfully created
            print(f"Cleaning up temporary directory: {TMP.name}")
            try:
                TMP.cleanup()
            except Exception as e:
                print(f"Error cleaning up temporary directory {TMP.name}: {e}")


if __name__ == "__main__":
    main()
