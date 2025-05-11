#!/usr/bin/env python3
import argparse
import tempfile
import shutil
import yaml
import uuid as py_uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Type

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
    models = {}
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
            extras = {}
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
    outdir: Path,
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
    if is_geo and "geometry" in df.columns:
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=crs or "EPSG:5070")
        table = pa.Table.from_pandas(gdf, preserve_index=False)
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)

    existing = table.schema.metadata or {}
    meta = {
        b"custom_schema_name": SCHEMA["schema_name"].encode(),
        b"custom_schema_version": SCHEMA["schema_version"].encode(),
        b"custom_yaml_schema_src": Path(SCHEMA_PATH).name.encode(),
    }
    new_schema = table.schema.with_metadata({**existing, **meta})
    # build the target URI (either "file:///…" or "s3://…")
    fn = SCHEMA["tables"][tbl]["file_path"]
    target = f"{outdir.rstrip('/')}/{fn}"
    fs, path = fsspec.core.url_to_fs(target)
    with fs.open(path, "wb") as f:
        pq.write_table(table.cast(new_schema), f, compression="snappy")

    fs, path = fsspec.core.url_to_fs(target)
    # open an S3 (or local) file and write the parquet bytes
    with fs.open(path, "wb") as f:
        pq.write_table(table.cast(new_schema), f, compression="snappy")
    print(f"wrote {len(valid)} rows → {tbl}")

    register_parent_keys(gdf if is_geo and "geometry" in df.columns else df, tbl)


# if path is S3, download to a temp dir
TMP = tempfile.TemporaryDirectory()


def fetch_local(path: str) -> str:
    """If `path` starts with s3://, pull it down once into TMP and return local path."""
    low = path.lower()
    if low.startswith("s3://") or low.startswith("s3a://"):
        fs, anon_path = fsspec.core.url_to_fs(path)
        # internal path e.g. "bucket/key/to/file.gpkg"
        name = Path(anon_path).name
        local = Path(TMP.name) / name
        if not local.exists():
            with fs.open(anon_path, "rb") as src, open(local, "wb") as dst:
                shutil.copyfileobj(src, dst)
        return str(local)
    else:
        return path


# Walk branch dirs in local or S3
def list_branch_dirs(hand_dir: str) -> List[str]:
    fs, root = fsspec.core.url_to_fs(hand_dir)
    scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
    branches: List[str] = []

    # list all HUC8 directories under root
    for info in fs.ls(root, detail=True):
        if info["type"] != "directory":
            continue
        huc8 = info["name"]
        br_root = f"{huc8}/branches"
        # does branches/ exist?
        if not fs.exists(br_root):
            continue
        # list each sub‐branch
        for sub in fs.ls(br_root, detail=True):
            if sub["type"] == "directory":
                uri = f"{scheme}://{sub['name']}" if scheme != "file" else sub["name"]
                branches.append(uri)
    return branches


# Load HAND suite from local or S3
def load_hand_suite(
    outdir: Path,
    hand_dir: str,
    hand_ver: str,
    nwm_ver: Decimal,
):
    # find all branch folders
    branch_dirs = list_branch_dirs(hand_dir)

    # 1) Catchments
    catch_recs: List[Dict[str, Any]] = []
    dir_to_id: Dict[str, py_uuid.UUID] = {}
    catch_crs = None

    for d in branch_dirs:
        print(f"processing branch: {d}")
        # glob remote or local GPKG
        fs, anon = fsspec.core.url_to_fs(d)
        pl = fs.glob(f"{anon}/*gw_catchments*.gpkg")
        if not pl:
            continue

        geoms = []
        for anon_fp in pl:
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uri = f"{scheme}://{anon_fp}" if scheme != "file" else anon_fp
            loc = fetch_local(uri)
            gdf = gpd.read_file(loc)
            if gdf.empty:
                continue
            catch_crs = catch_crs or gdf.crs
            geoms.append(unary_union(gdf.geometry))

        if not geoms:
            continue
        merged = unary_union(geoms)
        cid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{hand_ver}:{merged.wkt}")
        dir_to_id[d] = cid
        catch_recs.append(
            {
                "catchment_id": cid,
                "hand_version_id": hand_ver,
                "geometry": merged,
                "additional_attributes": None,
            }
        )

    generic_validate_and_write(
        catch_recs, "Catchments", outdir, is_geo=True, crs=catch_crs or "EPSG:5070"
    )

    valid_cids = PARENT_KEYS.get("Catchments.catchment_id", set())
    branch_dirs = [d for d in branch_dirs if dir_to_id.get(d) in valid_cids]

    # 2) Hydrotables
    hydro_recs: List[Dict[str, Any]] = []
    for d in branch_dirs:
        fs, anon = fsspec.core.url_to_fs(d)
        csvs = fs.glob(f"{anon}/hydroTable_*.csv")
        if not csvs:
            continue

        # load each CSV into a single DF
        pieces = []
        for anon_fp in csvs:
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uri = f"{scheme}://{anon_fp}" if scheme != "file" else anon_fp
            loc = fetch_local(uri)
            pieces.append(pd.read_csv(loc))
        df = pd.concat(pieces, ignore_index=True)

        # coerce
        df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
        df["discharge_cms"] = pd.to_numeric(df["discharge_cms"], errors="coerce")
        df["feature_id"] = pd.to_numeric(df["feature_id"], errors="coerce")
        df["HydroID"] = df["HydroID"].astype(str)

        def first_notnull(s):
            return s.dropna().iloc[0] if not s.dropna().empty else None

        grp = (
            df.groupby("HydroID")
            .agg(
                nwm_feature_id_agg=("feature_id", first_notnull),
                huc_id_agg=("HUC", first_notnull),
                lake_id_agg=("LakeID", first_notnull),
                stage_list=(
                    "stage",
                    lambda v: [to_decimal(x) for x in sorted(v.dropna())],
                ),
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
                    "catchment_id": dir_to_id[d],
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
                        str(r["lake_id_agg"]) if pd.notna(r["lake_id_agg"]) else None
                    ),
                }
            )

    generic_validate_and_write(hydro_recs, "Hydrotables", outdir)

    # 3) HAND_REM_Rasters
    rem_recs: List[Dict[str, Any]] = []
    rem_map: Dict[str, List[py_uuid.UUID]] = {}

    for d in branch_dirs:
        fs, anon = fsspec.core.url_to_fs(d)
        tifs = fs.glob(f"{anon}/*rem_zeroed*.tif")
        if not tifs:
            continue

        ids: List[py_uuid.UUID] = []
        for anon_fp in tifs:
            scheme = fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0]
            uri = f"{scheme}://{anon_fp}" if scheme != "file" else anon_fp
            rid = py_uuid.uuid5(
                py_uuid.NAMESPACE_DNS, f"{dir_to_id[d]}:{Path(uri).name}"
            )
            ids.append(rid)
            rem_recs.append(
                {
                    "rem_raster_id": rid,
                    "catchment_id": dir_to_id[d],
                    "hand_version_id": hand_ver,
                    "raster_path": uri,
                    "metadata": None,
                }
            )
        rem_map[d] = ids

    generic_validate_and_write(rem_recs, "HAND_REM_Rasters", outdir)

    # 4) HAND_Catchment_Rasters
    cr_recs: List[Dict[str, Any]] = []
    for d, ids in rem_map.items():
        first_rem = ids[0]
        for anon_fp in fsspec.core.url_to_fs(d)[0].glob(
            f"{fsspec.core.url_to_fs(d)[1]}/*gw_catchments*.tif"
        ):
            uri = f"{fsspec.core.url_to_fs(d)[0].protocol}://{anon_fp}"
            cid = py_uuid.uuid5(py_uuid.NAMESPACE_DNS, f"{first_rem}:{Path(uri).name}")
            cr_recs.append(
                {
                    "catchment_raster_id": cid,
                    "rem_raster_id": first_rem,
                    "raster_path": uri,
                    "metadata": None,
                }
            )

    generic_validate_and_write(cr_recs, "HAND_Catchment_Rasters", outdir)


def main():
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
    p.add_argument("--hand-version", required=True, help="a text id for this HAND run")
    p.add_argument("--nwm-version", required=True, help="NWM version (decimal)")
    args = p.parse_args()

    outdir_uri = args.output_dir.rstrip("/")
    # if this is a true URI (s3:// or s3a://) we do NOT mkdir() locally
    if outdir_uri.startswith(("s3://", "s3a://")):
        outdir = outdir_uri
    else:
        outdir = Path(outdir_uri)
        outdir.mkdir(parents=True, exist_ok=True)
        outdir = str(outdir)

    hand_ver = args.hand_version
    nwm_ver = Decimal(args.nwm_version)

    # enforce that the hand-version string appears somewhere in the output path
    if hand_ver not in str(outdir):
        p.error(f"--output-dir ('{outdir}') must contain the hand-version '{hand_ver}'")

    load_hand_suite(outdir, args.hand_dir, hand_ver, nwm_ver)

    print("\nDONE. Parent Key registry summary:")
    for k, v in PARENT_KEYS.items():
        print(f"  {k}: {len(v)} keys")


if __name__ == "__main__":
    main()
