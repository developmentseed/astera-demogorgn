"""
ingest.py — Consolidated ingestion + source.coop upload pipeline.

Both initial ingest and updates read from NetCDF or Zarr files on source.coop.
The mode (initial vs update) and input type (realizations vs mask) are both
auto-detected:

  • Input type   — presence of an ``i`` dimension → realization cube (3-D);
                   absence → 2-D mask file (e.g. antarctica_fast_flow_mask_50m.nc)
  • Ingest mode  — if no store exists at the target S3 path → initial ingest;
                   otherwise → update

For realization inputs, update NetCDFs are georeferenced spatial subsets and
may contain fewer than N_REALIZATIONS realizations; they are matched into the
store on the ``i`` axis.

For mask inputs, the entire store is always rewritten from the source file —
all non-coordinate variables are stored as separate 2-D Zarr arrays with
(512, 512) chunks and zstd compression.

To force a re-ingest when a store already exists, delete the store directory
first and re-run.

Usage
-----
  python ingest.py --type realizations --mode initial --data ./data/realizations.nc
  python ingest.py --type realizations --mode update  --data s3://.../
  python ingest.py --type mask         --mode initial --data ./data/mask.nc
  python ingest.py --type realizations --mode update  --data ./local_update.nc --dry-run

Environment variables
---------------------
  DATA_PATH          Local path or s3:// URI of the input NetCDF (env: DATA_PATH).
                     For initial ingest: full-grid NetCDF.
                     For updates: georeferenced spatial subset with i, y, x coords;
                     y/x must be a contiguous subset of the full grid; i values are
                     matched by value, not position.
  COMMIT_MESSAGE     Optional Icechunk commit message (env: COMMIT_MESSAGE).
  DRY_RUN            "true" to validate an update without writing (default: false).

  NC_VAR             Name of the data variable in the NetCDF (env: NC_VAR).
                     Auto-detected if omitted: tries "realizations", then
                     "__xarray_dataarray_variable__", then the first non-coord var.

  # ── AWS credentials (picked up automatically by boto3) ─────────────────────
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
    Standard boto3 credential chain — set these (or use an instance/task role)
    to authenticate writes to source.coop's S3 bucket.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import tempfile
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Generator

import boto3
import icechunk
import netCDF4 as nc
import numpy as np
import tqdm
import zarr
from boto3.s3.transfer import TransferConfig
from zarr.codecs import BloscCodec

# ── Array constants ───────────────────────────────────────────────────────────
N_REALIZATIONS = 100
SHARD_SHAPE = (N_REALIZATIONS, 512, 512)  # outer write unit (~100 MB/shard)
INNER_CHUNK = (N_REALIZATIONS, 128, 128)  # inner read unit (~6.5 MB)

# ── Transfer tuning ───────────────────────────────────────────────────────────
UPLOAD_WORKERS = 16
UPLOAD_CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB multipart parts

TRANSFER_CFG = TransferConfig(
    multipart_threshold=UPLOAD_CHUNK_SIZE,
    multipart_chunksize=UPLOAD_CHUNK_SIZE,
    max_concurrency=50,
    use_threads=True,
)

# Coordinate variable names expected in every NetCDF
COORD_VARS = {"seed_id", "y", "x"}

# Coordinate variable names for 2-D mask files (no seed_id dimension)
MASK_COORD_VARS = {"y", "x"}

# Chunk shape for 2-D mask arrays
MASK_CHUNK = (512, 512)


# ── Utilities ─────────────────────────────────────────────────────────────────


def log(msg: str) -> None:
    print(msg, flush=True)


def parse_s3(path: str) -> tuple[str, str]:
    stripped = path.replace("s3://", "")
    bucket, key = stripped.split("/", 1)
    return bucket, key


def is_s3(path: str) -> bool:
    return str(path).startswith("s3://")


def download_s3_to_temp(s3_client, s3_path: str, suffix: str) -> str:
    """
    Download an S3 object to a temp file with a tqdm progress bar.

    Uses boto3's managed transfer (multipart, concurrent) under the hood.
    Caller is responsible for unlinking the returned temp path.
    """
    bucket, key = parse_s3(s3_path)
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp_path = tmp.name

    total = s3_client.head_object(Bucket=bucket, Key=key)["ContentLength"]
    log(f"  Downloading s3://{bucket}/{key}")
    log(f"  Size: {total / 1e9:.2f} GB")

    with tqdm.tqdm(
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc="  download",
        ncols=80,
    ) as bar:
        s3_client.download_file(
            bucket,
            key,
            tmp_path,
            Callback=lambda n: bar.update(n),
            Config=TRANSFER_CFG,
        )

    return tmp_path


@contextlib.contextmanager
def open_nc(s3_client, path: str) -> Generator[nc.Dataset, None, None]:
    """
    Open a NetCDF file from a local path or s3:// URI.

    Downloads to a temp file first if on S3 (netCDF4 requires a local path),
    then cleans up on exit.
    """
    tmp_path = None
    if is_s3(path):
        tmp_path = download_s3_to_temp(s3_client, path, ".nc")
        local_path = tmp_path
    else:
        local_path = path

    dataset = nc.Dataset(local_path, "r")
    try:
        yield dataset
    finally:
        dataset.close()
        if tmp_path:
            os.unlink(tmp_path)


def detect_data_var(dataset: nc.Dataset, nc_var: str | None) -> str:
    """
    Return the name of the data variable to read from a NetCDF dataset.

    Priority:
      1. Explicitly supplied nc_var argument
      2. "realizations"
      3. "__xarray_dataarray_variable__"  (xarray default export name)
      4. First variable that is not a coordinate (i / y / x)
    """
    if nc_var:
        if nc_var not in dataset.variables:
            raise ValueError(
                f"Variable {nc_var!r} not found in NetCDF. "
                f"Available: {list(dataset.variables)}"
            )
        return nc_var

    for candidate in ("realizations", "__xarray_dataarray_variable__"):
        if candidate in dataset.variables:
            return candidate

    non_coord = [v for v in dataset.variables if v not in COORD_VARS]
    if len(non_coord) == 1:
        return non_coord[0]

    raise ValueError(
        f"Cannot auto-detect data variable. Found: {list(dataset.variables)}. "
        "Use --nc-var to specify."
    )


def read_nc_data(
    dataset: nc.Dataset,
    nc_var: str | None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Read data and coordinate arrays from an open NetCDF dataset.

    Returns
    -------
    data   : float32 ndarray, shape (n_i, n_y, n_x)
    i_vals : int64 ndarray,   shape (n_i,)
    y_vals : float32 ndarray, shape (n_y,)
    x_vals : float32 ndarray, shape (n_x,)
    """
    var_name = detect_data_var(dataset, nc_var)
    data = np.ma.filled(dataset.variables[var_name][:].astype("float32"), np.nan)
    seed_id_vals = dataset.variables["seed_id"][:].astype("int64")
    y_vals = dataset.variables["y"][:].astype("float32")
    x_vals = dataset.variables["x"][:].astype("float32")
    log(
        f"  Variable : {var_name!r}  shape {data.shape}  "
        f"seed_id ∈ [{seed_id_vals[0]}, {seed_id_vals[-1]}]  "
        f"y ∈ [{y_vals[0]:.0f}, {y_vals[-1]:.0f}]  "
        f"x ∈ [{x_vals[0]:.0f}, {x_vals[-1]:.0f}]"
    )
    return data, seed_id_vals, y_vals, x_vals


def read_zarr_data(
    path: str,
    nc_var: str | None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Read data and coordinate arrays from a Zarr v3 group (local or s3://).

    Expects the group to contain i, y, x coordinate arrays alongside the data
    array.  Variable detection follows the same priority as read_nc_data.

    Returns
    -------
    data   : float32 ndarray, shape (n_i, n_y, n_x)
    i_vals : int64 ndarray,   shape (n_i,)
    y_vals : float32 ndarray, shape (n_y,)
    x_vals : float32 ndarray, shape (n_x,)
    """
    if is_s3(path):
        try:
            import s3fs
        except ImportError as exc:
            raise ImportError(
                "s3fs is required to read Zarr from S3: pip install s3fs"
            ) from exc
        store = s3fs.S3Map(root=path.replace("s3://", ""), s3=s3fs.S3FileSystem())
    else:
        store = path

    group = zarr.open(store, mode="r")
    if isinstance(group, zarr.Array):
        raise ValueError(
            f"Zarr input at {path!r} is a bare array; expected a group with "
            "'realizations' (or equivalent) plus i, y, x coordinate arrays."
        )

    available = list(group.keys())
    if nc_var:
        if nc_var not in available:
            raise ValueError(
                f"Variable {nc_var!r} not found in Zarr group. Available: {available}"
            )
        var_name = nc_var
    else:
        for candidate in ("realizations", "__xarray_dataarray_variable__"):
            if candidate in available:
                var_name = candidate
                break
        else:
            non_coord = [v for v in available if v not in COORD_VARS]
            if len(non_coord) == 1:
                var_name = non_coord[0]
            else:
                raise ValueError(
                    f"Cannot auto-detect data variable in Zarr group. "
                    f"Found: {available}. Use --nc-var to specify."
                )

    data = group[var_name][...].astype("float32")
    seed_id_vals = group["seed_id"][...].astype("int64")
    y_vals = group["y"][...].astype("float32")
    x_vals = group["x"][...].astype("float32")
    log(
        f"  Variable : {var_name!r}  shape {data.shape}  "
        f"seed_id ∈ [{seed_id_vals[0]}, {seed_id_vals[-1]}]  "
        f"y ∈ [{y_vals[0]:.0f}, {y_vals[-1]:.0f}]  "
        f"x ∈ [{x_vals[0]:.0f}, {x_vals[-1]:.0f}]"
    )
    return data, seed_id_vals, y_vals, x_vals


def read_input_data(
    s3_client,
    path: str,
    nc_var: str | None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Dispatch to the correct reader based on file extension."""
    if path.rstrip("/").endswith(".zarr"):
        log(f"  Format : Zarr")
        return read_zarr_data(path, nc_var)
    log(f"  Format : NetCDF")
    with open_nc(s3_client, path) as dataset:
        return read_nc_data(dataset, nc_var)


def peek_has_seed_id_dimension(s3_client, path: str) -> bool:
    """
    Return True if the input file contains a ``seed_id`` dimension/coordinate.

    Used to distinguish realization cubes (3-D, has ``seed_id``) from 2-D mask files.
    For NetCDF inputs a temporary download is performed; the temp file is
    deleted immediately after the check.
    """
    if path.rstrip("/").endswith(".zarr"):
        if is_s3(path):
            try:
                import s3fs
            except ImportError as exc:
                raise ImportError(
                    "s3fs is required to read Zarr from S3: pip install s3fs"
                ) from exc
            store = s3fs.S3Map(root=path.replace("s3://", ""), s3=s3fs.S3FileSystem())
        else:
            store = path
        group = zarr.open(store, mode="r")
        return "seed_id" in group
    else:
        with open_nc(s3_client, path) as dataset:
            return "seed_id" in dataset.variables or "seed_id" in dataset.dimensions


def read_mask_nc_data(
    dataset: nc.Dataset,
) -> tuple[dict[str, np.ndarray], np.ndarray, np.ndarray]:
    """
    Read all non-coordinate variables and spatial coordinates from a 2-D mask
    NetCDF dataset.

    Returns
    -------
    arrays : dict mapping variable name → ndarray (preserves original dtype)
    y_vals : float32 ndarray, shape (n_y,)
    x_vals : float32 ndarray, shape (n_x,)
    """
    y_vals = dataset.variables["y"][:].astype("float32")
    x_vals = dataset.variables["x"][:].astype("float32")
    data_vars = [v for v in dataset.variables if v not in MASK_COORD_VARS]
    arrays: dict[str, np.ndarray] = {}
    for name in data_vars:
        raw = dataset.variables[name][:]
        if hasattr(raw, "filled"):
            raw = raw.filled(fill_value=0)
        arrays[name] = np.asarray(raw)
    log(
        f"  Mask variables : {list(arrays.keys())}  "
        f"y ∈ [{y_vals[0]:.0f}, {y_vals[-1]:.0f}]  "
        f"x ∈ [{x_vals[0]:.0f}, {x_vals[-1]:.0f}]"
    )
    return arrays, y_vals, x_vals


def read_mask_zarr_data(
    path: str,
) -> tuple[dict[str, np.ndarray], np.ndarray, np.ndarray]:
    """
    Read all non-coordinate arrays and spatial coordinates from a 2-D mask
    Zarr group (local or s3://).

    Returns
    -------
    arrays : dict mapping variable name → ndarray (preserves original dtype)
    y_vals : float32 ndarray, shape (n_y,)
    x_vals : float32 ndarray, shape (n_x,)
    """
    if is_s3(path):
        try:
            import s3fs
        except ImportError as exc:
            raise ImportError(
                "s3fs is required to read Zarr from S3: pip install s3fs"
            ) from exc
        store = s3fs.S3Map(root=path.replace("s3://", ""), s3=s3fs.S3FileSystem())
    else:
        store = path
    group = zarr.open(store, mode="r")
    y_vals = group["y"][...].astype("float32")
    x_vals = group["x"][...].astype("float32")
    data_vars = [k for k in group.keys() if k not in MASK_COORD_VARS]
    arrays: dict[str, np.ndarray] = {name: group[name][...] for name in data_vars}
    log(
        f"  Mask variables : {list(arrays.keys())}  "
        f"y ∈ [{y_vals[0]:.0f}, {y_vals[-1]:.0f}]  "
        f"x ∈ [{x_vals[0]:.0f}, {x_vals[-1]:.0f}]"
    )
    return arrays, y_vals, x_vals


def read_mask_input_data(
    s3_client,
    path: str,
) -> tuple[dict[str, np.ndarray], np.ndarray, np.ndarray]:
    """Dispatch to the correct mask reader based on file extension."""
    if path.rstrip("/").endswith(".zarr"):
        log("  Format : Zarr (mask)")
        return read_mask_zarr_data(path)
    log("  Format : NetCDF (mask)")
    with open_nc(s3_client, path) as dataset:
        return read_mask_nc_data(dataset)


def match_coords(
    store_coords: np.ndarray,
    update_coords: np.ndarray,
    axis_name: str,
    tol: float = 1.0,
) -> slice:
    """
    Find the contiguous slice in store_coords that corresponds to update_coords.

    Matches by nearest value with a tolerance check, so floating-point
    coordinate round-trips don't cause spurious mismatches.

    Raises ValueError if the update coordinates are not a contiguous subset
    of the store coordinates within tolerance.
    """
    start_idx = int(np.argmin(np.abs(store_coords - update_coords[0])))
    end_idx = start_idx + len(update_coords)

    if end_idx > len(store_coords):
        raise ValueError(
            f"Update {axis_name} range [{update_coords[0]}, {update_coords[-1]}] "
            f"extends beyond the store ({axis_name} has {len(store_coords)} values)."
        )

    matched = store_coords[start_idx:end_idx]
    max_err = float(np.max(np.abs(matched - update_coords)))
    if max_err > tol:
        raise ValueError(
            f"Update {axis_name} coordinates do not align with the store "
            f"(max error {max_err:.4f} > tolerance {tol}). "
            "Check that the update NetCDF uses the same grid as the store."
        )

    return slice(start_idx, end_idx)


def match_seed_id_values(
    store_seed_id: np.ndarray,
    update_seed_id: np.ndarray,
) -> np.ndarray:
    """
    Return the store positions corresponding to each value in update_seed_id.

    store_seed_id  — the seed_id coordinate array from the Icechunk store, e.g. [0..99]
    update_seed_id — the seed_id values in the update NetCDF, e.g. [0, 5, 42]

    Raises ValueError if any update_seed_id value is not present in store_seed_id.
    """
    positions = np.searchsorted(store_seed_id, update_seed_id)
    # searchsorted can return out-of-bounds indices; validate all matches
    out_of_bounds = positions >= len(store_seed_id)
    mismatch = ~out_of_bounds & (
        store_seed_id[np.minimum(positions, len(store_seed_id) - 1)] != update_seed_id
    )
    bad = update_seed_id[out_of_bounds | mismatch]
    if len(bad):
        raise ValueError(
            f"Update contains seed_id values not found in the store: {bad.tolist()}. "
            f"Store seed_id range: [{store_seed_id[0]}, {store_seed_id[-1]}]."
        )
    return positions


# ── S3 sync (delta upload) ───────────────────────────────────────────────────


def sync_store_to_s3(local_path: str | Path, dest_s3_path: str) -> None:
    """
    Upload all new or changed files from local_path to dest_s3_path.

    Compares local file sizes against existing S3 object sizes; only uploads
    objects that are missing or have a different size.  This makes update runs
    fast — typically only the handful of newly-written shard files need to be
    transferred.
    """
    bucket, prefix = parse_s3(dest_s3_path)
    local = Path(local_path)
    s3 = boto3.client("s3")

    existing: dict[str, int] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            rel = obj["Key"][len(prefix) + 1 :]
            existing[rel] = obj["Size"]

    local_files = sorted(f for f in local.rglob("*") if f.is_file())
    to_upload = [
        f
        for f in local_files
        if (rel := f.relative_to(local).as_posix()) not in existing
        or existing[rel] != f.stat().st_size
    ]

    if not to_upload:
        log("  [sync] Store already up-to-date on S3 — nothing to upload.")
        return

    total_bytes = sum(f.stat().st_size for f in to_upload)
    log(
        f"  [sync] Uploading {len(to_upload)}/{len(local_files)} objects "
        f"({total_bytes / 1e9:.2f} GB) to s3://{bucket}/{prefix} …"
    )
    t0 = time.perf_counter()

    def _upload(f: Path) -> None:
        key = f"{prefix}/{f.relative_to(local).as_posix()}"
        s3.upload_file(
            str(f),
            bucket,
            key,
            ExtraArgs={"ACL": "bucket-owner-full-control"},
            Config=TRANSFER_CFG,
        )

    with tqdm.tqdm(total=len(to_upload), desc="  upload", unit="file", ncols=80) as bar:
        with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as pool:
            futures = {pool.submit(_upload, f): f for f in to_upload}
            for fut in as_completed(futures):
                fut.result()
                bar.update(1)

    log(f"  [sync] Done in {time.perf_counter() - t0:.1f}s")


# ── Initial ingest ────────────────────────────────────────────────────────────


def run_initial(
    source_path: str,
    store_s3_path: str,
    nc_var: str | None,
) -> str:
    """
    Build a fresh Icechunk store from a full-grid NetCDF or Zarr, sync it to
    source.coop, then delete the local temp directory.

    Streams one spatial shard (~100 MB) at a time — peak RAM is one shard
    regardless of the total file size.

    Returns the Icechunk snapshot ID of the initial commit.
    """
    log("=== Initial Ingest ===")
    log(f"  Source : {source_path}")
    log(f"  Store  : {store_s3_path}")

    s3 = boto3.client("s3")
    t_start = time.perf_counter()

    # ── Open source (keep it open for shard-by-shard reads) ───────────────────
    # For NetCDF: download to temp file first (netCDF4 needs a local path).
    # For Zarr: open directly (supports lazy slicing).
    is_zarr = source_path.rstrip("/").endswith(".zarr")

    if is_zarr:
        log("  Format : Zarr")
        if is_s3(source_path):
            try:
                import s3fs
            except ImportError as exc:
                raise ImportError(
                    "s3fs is required for S3 Zarr: pip install s3fs"
                ) from exc
            src_store = s3fs.S3Map(
                root=source_path.replace("s3://", ""), s3=s3fs.S3FileSystem()
            )
        else:
            src_store = source_path
        src_group = zarr.open(src_store, mode="r")
        available = list(src_group.keys())
        var_name = nc_var or next(
            (
                c
                for c in ("realizations", "__xarray_dataarray_variable__")
                if c in available
            ),
            next((v for v in available if v not in COORD_VARS), None),
        )
        if not var_name:
            raise ValueError(f"Cannot detect data variable in Zarr. Found: {available}")
        src_var = src_group[var_name]
        n_i, ny, nx = src_var.shape
        seed_id_vals = src_group["seed_id"][...].astype("int64")
        y_vals = src_group["y"][...].astype("float32")
        x_vals = src_group["x"][...].astype("float32")
        nc_tmp = None
        nc_ds = None
    else:
        log("  Format : NetCDF")
        nc_tmp = (
            download_s3_to_temp(s3, source_path, ".nc") if is_s3(source_path) else None
        )
        nc_ds = nc.Dataset(nc_tmp or source_path, "r")
        var_name = detect_data_var(nc_ds, nc_var)
        src_var = nc_ds.variables[var_name]
        n_i, ny, nx = src_var.shape
        seed_id_vals = nc_ds.variables["seed_id"][:].astype("int64")
        y_vals = nc_ds.variables["y"][:].astype("float32")
        x_vals = nc_ds.variables["x"][:].astype("float32")

    log(f"  [download]  {time.perf_counter() - t_start:.1f}s")
    log(f"  Variable : {var_name!r}  shape ({n_i}, {ny}, {nx})")

    if n_i != N_REALIZATIONS:
        raise ValueError(
            f"Source has {n_i} realizations but N_REALIZATIONS={N_REALIZATIONS}. "
            "Update N_REALIZATIONS at the top of this script if intentional."
        )

    n_shards_y = -(-ny // SHARD_SHAPE[1])
    n_shards_x = -(-nx // SHARD_SHAPE[2])
    n_shards = n_shards_y * n_shards_x
    log(f"  Grid   : {ny} × {nx}  |  shards: {n_shards_y} × {n_shards_x} = {n_shards}")

    # Write directly to S3 — no local disk space needed.
    bucket, prefix = parse_s3(store_s3_path)
    storage = icechunk.s3_storage(
        bucket=bucket, prefix=prefix, region="us-west-2", from_env=True
    )
    repo = icechunk.Repository.open_or_create(storage=storage)
    session = repo.writable_session("main")

    try:
        root = zarr.open_group(session.store, mode="w")
        arr = root.create_array(
            "realizations",
            shape=(N_REALIZATIONS, ny, nx),
            shards=SHARD_SHAPE,
            chunks=INNER_CHUNK,
            dtype="float32",
            compressors=BloscCodec(cname="zstd", clevel=5),
            dimension_names=["seed_id", "y", "x"],
            overwrite=True,
        )

        for coord_name, coord_vals in [
            ("seed_id", seed_id_vals),
            ("y", y_vals),
            ("x", x_vals),
        ]:
            root.create_array(
                coord_name,
                shape=coord_vals.shape,
                dtype=coord_vals.dtype,
                chunks=coord_vals.shape,
                dimension_names=[coord_name],
                overwrite=True,
            )
            root[coord_name][:] = coord_vals

        # Row-buffer shard loop.
        #
        # Reads one realization at a time per shard-row, which aligns with
        # (1, *, *) NetCDF chunking and avoids forcing HDF5 to decompress
        # 100 full 2-D slabs per spatial tile.
        #
        # For each row of shards:
        #   1. Allocate row_buf (N, row_height, nx) — ~2.7 GB peak
        #   2. Fill it realization-by-realization: src_var[i, y_sl, :]
        #   3. Write each spatial shard directly to S3 via Icechunk
        #
        # Peak RAM = one row buffer; no local store copy needed.
        tracemalloc.start()
        t_ingest = time.perf_counter()
        shard_times: list[float] = []

        y_starts = list(range(0, ny, SHARD_SHAPE[1]))
        x_starts = list(range(0, nx, SHARD_SHAPE[2]))

        with tqdm.tqdm(
            total=n_shards, desc="  write shards", unit="shard", ncols=80
        ) as bar:
            for sy in y_starts:
                y_sl = slice(sy, min(sy + SHARD_SHAPE[1], ny))
                row_ny = y_sl.stop - y_sl.start

                row_buf = np.empty((n_i, row_ny, nx), dtype="float32")
                for i in range(n_i):
                    row_buf[i] = np.ma.filled(
                        src_var[i, y_sl, :].astype("float32"), np.nan
                    )

                for sx in x_starts:
                    x_sl = slice(sx, min(sx + SHARD_SHAPE[2], nx))
                    t_s = time.perf_counter()

                    arr[:, y_sl, x_sl] = row_buf[:, :, x_sl]

                    shard_times.append(time.perf_counter() - t_s)
                    _, peak_bytes = tracemalloc.get_traced_memory()
                    bar.set_postfix(
                        last=f"{shard_times[-1]:.1f}s",
                        peak_ram=f"{peak_bytes / 1e9:.2f}GB",
                    )
                    bar.update(1)

                del row_buf

        snapshot_id = session.commit(
            f"Initial write: {N_REALIZATIONS} realizations, float32, sharded {SHARD_SHAPE}"
        )

        t_write = time.perf_counter() - t_ingest
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        log(
            f"  [write]     {t_write:.1f}s ({t_write / 60:.1f} min)  "
            f"mean shard: {np.mean(shard_times):.2f}s  "
            f"peak RAM: {peak_bytes / 1e6:.0f} MB  "
            f"snapshot: {snapshot_id}"
        )
        log(f"  [total]     {time.perf_counter() - t_start:.1f}s")

    finally:
        if nc_ds:
            nc_ds.close()
        if nc_tmp:
            os.unlink(nc_tmp)

    return snapshot_id


# ── Update ────────────────────────────────────────────────────────────────────


def run_update(
    store_s3_path: str,
    data_path: str,
    dry_run: bool,
    nc_var: str | None,
    message: str = "",
) -> str:
    """
    Apply a partial update from a georeferenced NetCDF subset directly to the
    Icechunk store on source.coop — no local store copy needed.

    The update NetCDF must contain i, y, x coordinate variables.  y/x values
    identify the spatial region to overwrite; i values are matched by value so
    updates with fewer than N_REALIZATIONS realizations apply only to the
    matching positions.

    Returns a result summary string.
    """
    log(f"=== Update{'  [DRY RUN]' if dry_run else ''} ===")
    log(f"  Data  : {data_path}")
    log(f"  Store : {store_s3_path}")

    s3 = boto3.client("s3")

    # ── Load source data ──────────────────────────────────────────────────────
    log("Loading source data …")
    t_download = time.perf_counter()
    new_data, upd_seed_id, upd_y, upd_x = read_input_data(s3, data_path, nc_var)
    log(
        f"  [download]   {time.perf_counter() - t_download:.1f}s  "
        f"shape {new_data.shape}  "
        f"({len(upd_seed_id)} realizations, {len(upd_y)} rows, {len(upd_x)} cols)"
    )

    # ── Open store directly on S3 ─────────────────────────────────────────────
    bucket, prefix = parse_s3(store_s3_path)
    storage = icechunk.s3_storage(
        bucket=bucket, prefix=prefix, region="us-west-2", from_env=True
    )
    repo = icechunk.Repository.open(storage)
    session = repo.writable_session("main")
    root = zarr.open_group(session.store, mode="r+")
    arr = root["realizations"]

    store_seed_id = root["seed_id"][:]
    store_y = root["y"][:]
    store_x = root["x"][:]

    # ── Coordinate matching ───────────────────────────────────────────────────
    row_sl = match_coords(store_y, upd_y, "y")
    col_sl = match_coords(store_x, upd_x, "x")
    seed_id_positions = match_seed_id_values(store_seed_id, upd_seed_id)

    log(
        f"  Store slice  : rows {row_sl.start}:{row_sl.stop}  "
        f"cols {col_sl.start}:{col_sl.stop}  "
        f"seed_id positions {seed_id_positions.tolist()}"
    )

    if dry_run:
        log("[DRY RUN] Coordinates validated — skipping write.")
        return "DRY RUN — validated OK"

    # ── Apply update ──────────────────────────────────────────────────────────
    t_write = time.perf_counter()

    for upd_idx, store_pos in enumerate(seed_id_positions):
        arr[int(store_pos), row_sl, col_sl] = new_data[upd_idx]

    commit_msg = message or "Update"
    snapshot_id = session.commit(commit_msg)
    log(f"  [write]      {time.perf_counter() - t_write:.1f}s  snapshot: {snapshot_id}")
    log(f"  [total]      {time.perf_counter() - t_download:.1f}s")

    result = (
        f"OK — "
        f"rows {row_sl.start}:{row_sl.stop}  "
        f"cols {col_sl.start}:{col_sl.stop}  "
        f"{len(seed_id_positions)} realization(s) updated  "
        f"snapshot={snapshot_id}"
    )
    log(result)
    return result


# ── Mask ingest ───────────────────────────────────────────────────────────────


def run_mask(
    source_path: str,
    store_s3_path: str,
    message: str = "",
) -> str:
    """
    Write (or overwrite) a 2-D mask Icechunk store on source.coop.

    The entire mask is always rewritten from the source file — there is no
    partial-update path.  All non-coordinate variables are stored as separate
    2-D arrays with (512, 512) zstd-compressed chunks; ``y`` and ``x`` are
    stored as 1-D coordinate arrays.

    Returns the Icechunk snapshot ID of the commit.
    """
    log("=== Mask Ingest ===")
    log(f"  Source : {source_path}")
    log(f"  Store  : {store_s3_path}")

    s3 = boto3.client("s3")

    t_download = time.perf_counter()
    arrays, y_vals, x_vals = read_mask_input_data(s3, source_path)
    log(f"  [download]  {time.perf_counter() - t_download:.1f}s")

    ny, nx = y_vals.shape[0], x_vals.shape[0]

    with tempfile.TemporaryDirectory(prefix="icechunk_mask_") as tmp:
        local_store = Path(tmp) / "mask.icechunk"
        storage = icechunk.local_filesystem_storage(str(local_store))
        repo = icechunk.Repository.open_or_create(storage=storage)
        session = repo.writable_session("main")

        root = zarr.open_group(session.store, mode="w")

        for coord_name, coord_vals in [("y", y_vals), ("x", x_vals)]:
            root.create_array(
                coord_name,
                shape=coord_vals.shape,
                dtype=coord_vals.dtype,
                chunks=coord_vals.shape,
                dimension_names=[coord_name],
                overwrite=True,
            )
            root[coord_name][:] = coord_vals

        t_write = time.perf_counter()
        for var_name, var_data in arrays.items():
            log(
                f"  [write] {var_name}  shape {var_data.shape}  dtype {var_data.dtype} …"
            )
            root.create_array(
                var_name,
                shape=var_data.shape,
                chunks=MASK_CHUNK,
                dtype=var_data.dtype,
                compressors=BloscCodec(cname="zstd", clevel=5),
                dimension_names=["y", "x"],
                overwrite=True,
            )
            root[var_name][:] = var_data

        commit_msg = message or f"Mask write: {list(arrays.keys())}, grid {ny}×{nx}"
        snapshot_id = session.commit(commit_msg)
        log(
            f"  [write]     {time.perf_counter() - t_write:.1f}s  "
            f"snapshot: {snapshot_id}"
        )

        t_upload = time.perf_counter()
        sync_store_to_s3(local_store, store_s3_path)
        log(f"  [upload]    {time.perf_counter() - t_upload:.1f}s")
        log(f"  [total]     {time.perf_counter() - t_download:.1f}s")

    return snapshot_id


# ── CLI ───────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--data",
        default=os.environ.get("DATA_PATH"),
        metavar="PATH",
        help="Local path or s3:// URI of the input NetCDF (env: DATA_PATH)",
    )
    p.add_argument(
        "--store",
        default=os.environ.get("STORE_S3_PATH"),
        metavar="S3_URI",
        help="s3:// URI of the Icechunk store on source.coop (env: STORE_S3_PATH)",
    )
    p.add_argument(
        "--nc-var",
        default=os.environ.get("NC_VAR"),
        metavar="VAR",
        help="Name of the data variable in the NetCDF (env: NC_VAR); "
        'auto-detected if omitted (tries "realizations", then '
        '"__xarray_dataarray_variable__", then the first non-coord var)',
    )
    p.add_argument(
        "--type",
        choices=["realizations", "mask"],
        required=True,
        metavar="TYPE",
        help="Input data type: 'realizations' (3-D cube with seed_id/y/x) or "
        "'mask' (2-D spatial file)",
    )
    p.add_argument(
        "--mode",
        choices=["initial", "update"],
        required=True,
        metavar="MODE",
        help="Ingest mode: 'initial' (create a new store) or "
        "'update' (apply a partial update to an existing store)",
    )
    p.add_argument(
        "--message",
        default=os.environ.get("COMMIT_MESSAGE", ""),
        metavar="MSG",
        help="Icechunk commit message (env: COMMIT_MESSAGE); defaults to 'Update'",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "false").lower() == "true",
        help="[update] Validate without writing (env: DRY_RUN)",
    )
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if not args.data:
        parser.error("--data is required (or set DATA_PATH)")
    if not args.store:
        parser.error("--store is required (or set STORE_S3_PATH)")

    log(f"Input type : {args.type}")
    log(f"Mode       : {args.mode}")

    # For S3 NetCDF inputs, download once here so the run functions don't each
    # trigger a separate download.
    s3 = boto3.client("s3")
    nc_tmp = None
    data_path = args.data
    if is_s3(args.data) and not args.data.rstrip("/").endswith(".zarr"):
        nc_tmp = download_s3_to_temp(s3, args.data, ".nc")
        data_path = nc_tmp

    try:
        if args.type == "mask":
            snapshot_id = run_mask(
                source_path=data_path,
                store_s3_path=args.store,
                message=args.message,
            )
            Path("/tmp/ingest_result.txt").write_text(
                f"snapshot_id={snapshot_id}\nstore={args.store}\n"
            )

        else:  # realizations
            if args.mode == "initial":
                snapshot_id = run_initial(
                    source_path=data_path,
                    store_s3_path=args.store,
                    nc_var=args.nc_var,
                )
                Path("/tmp/ingest_result.txt").write_text(
                    f"mode=initial\nsnapshot_id={snapshot_id}\nstore={args.store}\n"
                )
            else:
                result = run_update(
                    store_s3_path=args.store,
                    data_path=data_path,
                    dry_run=args.dry_run,
                    nc_var=args.nc_var,
                    message=args.message,
                )
                Path("/tmp/ingest_result.txt").write_text(result + "\n")
    finally:
        if nc_tmp:
            os.unlink(nc_tmp)


if __name__ == "__main__":
    main()
