"""
ingest.py — Consolidated ingestion + source.coop upload pipeline.

Modes
-----
  initial   Build a fresh Icechunk store from a source NetCDF (local or S3),
            then sync it to source.coop.
  update    Apply a single basin-level partial update to an existing local
            Icechunk store, then sync only the new/changed objects to source.coop.

Usage
-----
  python ingest.py --mode initial
  python ingest.py --mode update --basin jakobshavn --date 2025-04 \\
      --mask s3://us-west-2.opendata.source.coop/<org>/masks/jakobshavn.npy \\
      --data ./jakobshavn_2025-04.npy
  python ingest.py --mode update --dry-run ...

Environment variables
---------------------
  MODE               "initial" or "update"  (overridden by --mode)

  # ── initial ────────────────────────────────────────────────────────────────
  SOURCE_PATH        Local path or s3:// URI to the source NetCDF (.nc)
                     containing variable "__xarray_dataarray_variable__"
                     with shape (N_REALIZATIONS, y, x).

  # ── update ─────────────────────────────────────────────────────────────────
  DATE               Update date string, e.g. "2025-04" (env: DATE).
  MASK_PATH          s3:// URI of the basin mask .npy in the source.coop bucket
                     (env: MASK_PATH).  Shape: (y, x), dtype bool/int.
  DATA_PATH          Local path or s3:// URI of the new data .npy
                     (env: DATA_PATH).  Shape: (n_realizations, y_bbox, x_bbox).
  COMMIT_MESSAGE     Optional Icechunk commit message (env: COMMIT_MESSAGE);
                     defaults to empty string.
  DRY_RUN            "true" to validate shapes without writing (default: false).
  GIT_SHA            Triggering commit SHA — embedded in Icechunk commit messages.

  # ── both modes ─────────────────────────────────────────────────────────────
  LOCAL_STORE_PATH   Local path for the Icechunk store.
                     (default: /tmp/realizations.icechunk)
  DEST_S3_PATH       Destination on source.coop, e.g.:
                     s3://us-west-2.opendata.source.coop/<org>/astera/realizations.icechunk
                     Omit (or set SKIP_UPLOAD=true) to skip the upload step.
  SKIP_UPLOAD        "true" to skip the S3 sync step (default: false).

  # ── AWS credentials (picked up automatically by boto3) ─────────────────────
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
    Standard boto3 credential chain — set these (or use an instance/task role)
    to authenticate writes to source.coop's S3 bucket.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import icechunk
import numpy as np
import zarr
from boto3.s3.transfer import TransferConfig
from zarr.codecs import BloscCodec

# ── Array constants ───────────────────────────────────────────────────────────
N_REALIZATIONS = 100
SHARD_SHAPE    = (N_REALIZATIONS, 512, 512)   # outer write unit (~100 MB/shard)
INNER_CHUNK    = (N_REALIZATIONS, 128, 128)   # inner read unit (~6.5 MB)

# ── Upload tuning ─────────────────────────────────────────────────────────────
UPLOAD_WORKERS       = 16
MULTIPART_CHUNK_SIZE = 64 * 1024 * 1024       # 64 MB per multipart part

TRANSFER_CFG = TransferConfig(
    multipart_threshold=MULTIPART_CHUNK_SIZE,
    multipart_chunksize=MULTIPART_CHUNK_SIZE,
    max_concurrency=10,
    use_threads=True,
)


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
    """Download an S3 object to a temp file; caller is responsible for unlinking."""
    bucket, key = parse_s3(s3_path)
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp_path = tmp.name
    log(f"  Downloading s3://{bucket}/{key} → {tmp_path} ...")
    t0 = time.perf_counter()
    s3_client.download_file(bucket, key, tmp_path)
    log(f"  Download complete in {time.perf_counter() - t0:.1f}s")
    return tmp_path


def load_array(s3_client, path: str) -> np.ndarray:
    """
    Load an array from a local path or an s3:// URI.

    Supported formats
    -----------------
    .npy          — NumPy binary; downloaded to a temp file if on S3.
    Zarr array    — local directory or s3:// prefix (requires s3fs for S3).
                    If the path points to a Zarr group with exactly one array,
                    that array is returned.  If there are multiple arrays,
                    point directly to the array path inside the store.
    """
    if path.endswith(".npy"):
        if is_s3(path):
            tmp = download_s3_to_temp(s3_client, path, ".npy")
            try:
                return np.load(tmp)
            finally:
                os.unlink(tmp)
        return np.load(path)

    # ── Zarr ──────────────────────────────────────────────────────────────────
    if is_s3(path):
        try:
            import s3fs
        except ImportError as exc:
            raise ImportError(
                "s3fs is required to read Zarr arrays from S3: "
                "pip install s3fs"
            ) from exc
        store = s3fs.S3Map(root=path.replace("s3://", ""), s3=s3fs.S3FileSystem())
    else:
        store = path

    z = zarr.open(store, mode="r")
    if isinstance(z, zarr.Array):
        return z[...]

    # Group — return the single contained array or raise a helpful error
    arrays = list(z.arrays())
    if len(arrays) == 1:
        return arrays[0][1][...]
    names = [name for name, _ in arrays]
    raise ValueError(
        f"Zarr group at {path!r} contains {len(names)} arrays: {names}. "
        "Point --mask / --data directly to the array path inside the store, "
        f"e.g. {path}/{names[0]}"
    )


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
    local  = Path(local_path)
    s3     = boto3.client("s3")

    # ── List existing S3 objects ──────────────────────────────────────────────
    existing: dict[str, int] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
        for obj in page.get("Contents", []):
            rel = obj["Key"][len(prefix) + 1:]   # strip "prefix/"
            existing[rel] = obj["Size"]

    # ── Determine which local files need uploading ────────────────────────────
    local_files = sorted(f for f in local.rglob("*") if f.is_file())
    to_upload   = [
        f for f in local_files
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
    t0   = time.perf_counter()
    done = 0

    def _upload(f: Path) -> None:
        key = f"{prefix}/{f.relative_to(local).as_posix()}"
        s3.upload_file(str(f), bucket, key, Config=TRANSFER_CFG)

    with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as pool:
        futures = {pool.submit(_upload, f): f for f in to_upload}
        for fut in as_completed(futures):
            fut.result()   # re-raise any upload exception immediately
            done += 1
            if done % 200 == 0 or done == len(to_upload):
                elapsed = time.perf_counter() - t0
                log(f"    {done}/{len(to_upload)} uploaded  ({elapsed:.0f}s)")

    log(f"  [sync] Done in {time.perf_counter() - t0:.1f}s")


# ── Initial ingest ────────────────────────────────────────────────────────────

def run_initial(
    source_path: str,
    local_store_path: str | Path,
    dest_s3_path: str | None,
    skip_upload: bool,
) -> str:
    """
    Build a fresh Icechunk store from a source NetCDF, then sync to source.coop.

    Returns the Icechunk snapshot ID of the initial commit.
    """
    import netCDF4 as nc

    log("=== Initial Ingest ===")
    log(f"  Source     : {source_path}")
    log(f"  Local store: {local_store_path}")
    log(f"  Dest S3    : {dest_s3_path or '(none — upload skipped)'}")

    s3       = boto3.client("s3")
    nc_path  = source_path
    tmp_path = None

    if is_s3(source_path):
        tmp_path = download_s3_to_temp(s3, source_path, ".nc")
        nc_path  = tmp_path

    try:
        src     = nc.Dataset(nc_path, "r")
        src_var = src.variables["__xarray_dataarray_variable__"]
        n_src, ny, nx = src_var.shape
        log(f"NetCDF shape: ({n_src}, {ny}, {nx})  dtype: {src_var.dtype}")

        if n_src != N_REALIZATIONS:
            raise ValueError(
                f"Source has {n_src} realizations but N_REALIZATIONS={N_REALIZATIONS}. "
                "Update N_REALIZATIONS at the top of this script if intentional."
            )

        n_shards_y = -(-ny // SHARD_SHAPE[1])
        n_shards_x = -(-nx // SHARD_SHAPE[2])
        n_shards   = n_shards_y * n_shards_x
        log(f"Spatial shards: {n_shards_y} × {n_shards_x} = {n_shards}")

        # ── Create (or overwrite) the local Icechunk store ────────────────────
        store_path = Path(local_store_path)
        storage    = icechunk.local_filesystem_storage(str(store_path))
        repo       = icechunk.Repository.open_or_create(storage=storage)
        session    = repo.writable_session("main")
        store      = session.store

        root = zarr.open_group(store, mode="w")
        arr  = root.create_array(
            "realizations",
            shape=(N_REALIZATIONS, ny, nx),
            shards=SHARD_SHAPE,
            chunks=INNER_CHUNK,
            dtype="float32",
            compressors=BloscCodec(cname="zstd", clevel=5),
            dimension_names=["i", "y", "x"],
            overwrite=True,
        )

        for coord_name, coord_vals in [
            ("i", np.arange(N_REALIZATIONS, dtype="int64")),
            ("y", src.variables["y"][:].astype("float32")),
            ("x", src.variables["x"][:].astype("float32")),
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

        # ── Streaming write: one spatial shard at a time ──────────────────────
        # Each shard spans ALL realizations for a 512 × 512 spatial tile.
        # Writing complete shards avoids read-modify-write amplification.
        tracemalloc.start()
        t_ingest    = time.perf_counter()
        shard_times: list[float] = []
        n_done      = 0

        for sy in range(0, ny, SHARD_SHAPE[1]):
            for sx in range(0, nx, SHARD_SHAPE[2]):
                y_sl = slice(sy, min(sy + SHARD_SHAPE[1], ny))
                x_sl = slice(sx, min(sx + SHARD_SHAPE[2], nx))
                t_s  = time.perf_counter()

                tile = src_var[:, y_sl, x_sl].astype("float32")
                arr[:, y_sl, x_sl] = tile
                del tile

                n_done += 1
                shard_times.append(time.perf_counter() - t_s)
                _, peak_bytes = tracemalloc.get_traced_memory()

                if n_done % 100 == 0 or n_done == n_shards:
                    elapsed = time.perf_counter() - t_ingest
                    log(
                        f"  Shard {n_done:4d}/{n_shards}  "
                        f"last: {shard_times[-1]:.1f}s  "
                        f"elapsed: {elapsed:.0f}s  "
                        f"peak traced RAM: {peak_bytes / 1e9:.2f} GB"
                    )

        snapshot_id = session.commit(
            f"Initial write: {N_REALIZATIONS} realizations, float32, "
            f"sharded {SHARD_SHAPE}"
        )

    finally:
        src.close()
        if tmp_path:
            os.unlink(tmp_path)

    t_total = time.perf_counter() - t_ingest
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    log(
        f"\nIngest complete:\n"
        f"  Wall time   : {t_total:.1f}s ({t_total / 60:.1f} min)\n"
        f"  Mean shard  : {np.mean(shard_times):.2f}s\n"
        f"  Peak RAM    : {peak_bytes / 1e6:.0f} MB\n"
        f"  Snapshot ID : {snapshot_id}"
    )

    if dest_s3_path and not skip_upload:
        sync_store_to_s3(local_store_path, dest_s3_path)

    return snapshot_id


# ── Basin update ──────────────────────────────────────────────────────────────

def run_update(
    local_store_path: str | Path,
    date: str,
    mask_path: str,
    data_path: str,
    dest_s3_path: str | None,
    dry_run: bool,
    git_sha: str,
    skip_upload: bool,
    message: str = "",
) -> str:
    """
    Apply a single basin-level partial update to the local store, then sync
    only the new/changed shard objects to source.coop.

    mask_path  — s3:// URI (fetched from source.coop) or local path to a .npy
                 boolean mask with shape (y, x).
    data_path  — local path or s3:// URI to a .npy array with shape
                 (n_realizations, y_bbox, x_bbox).

    Returns the result string for this update.
    """
    log(f"=== Basin Update{'  [DRY RUN]' if dry_run else ''} ===")
    log(f"  Date       : {date}")
    log(f"  Mask       : {mask_path}")
    log(f"  Data       : {data_path}")
    log(f"  Local store: {local_store_path}")
    log(f"  Dest S3    : {dest_s3_path or '(none — upload skipped)'}")
    log(f"  Git SHA    : {git_sha}")

    s3 = boto3.client("s3")

    # ── Load mask and data ────────────────────────────────────────────────────
    log("Loading mask …")
    basin_mask = load_array(s3, mask_path).astype(bool)

    log("Loading data …")
    new_data = load_array(s3, data_path).astype("float32")

    rows_with_data = np.where(basin_mask.any(axis=1))[0]
    cols_with_data = np.where(basin_mask.any(axis=0))[0]
    bbox_h = int(rows_with_data[-1]) - int(rows_with_data[0]) + 1
    bbox_w = int(cols_with_data[-1]) - int(cols_with_data[0]) + 1
    log(f"Mask bbox : {bbox_h} rows × {bbox_w} cols")
    log(f"Data shape: {new_data.shape}")

    # ── Validate shapes ───────────────────────────────────────────────────────
    expected_shape = (new_data.shape[0], bbox_h, bbox_w)
    if new_data.shape != expected_shape:
        raise ValueError(
            f"Data shape {new_data.shape} does not match "
            f"mask bounding box {expected_shape}"
        )

    if dry_run:
        log("[DRY RUN] Shapes validated — skipping write.")
        return f"({date}): DRY RUN — validated OK"

    # ── Apply update ──────────────────────────────────────────────────────────
    t0 = time.perf_counter()

    storage = icechunk.local_filesystem_storage(str(local_store_path))
    repo    = icechunk.Repository.open(storage)
    session = repo.writable_session("main")
    arr     = zarr.open_group(session.store, mode="r+")["realizations"]

    rows   = np.where(basin_mask.any(axis=1))[0]
    cols   = np.where(basin_mask.any(axis=0))[0]
    row_sl = slice(int(rows[0]), int(rows[-1]) + 1)
    col_sl = slice(int(cols[0]), int(cols[-1]) + 1)

    arr[:, row_sl, col_sl] = new_data

    commit_msg  = message or ""
    snapshot_id = session.commit(commit_msg)
    elapsed     = time.perf_counter() - t0

    result = (
        f"({date}): OK — "
        f"rows {row_sl.start}:{row_sl.stop}  "
        f"cols {col_sl.start}:{col_sl.stop}  "
        f"snapshot={snapshot_id}  {elapsed:.1f}s"
    )
    log(result)

    if dest_s3_path and not skip_upload:
        sync_store_to_s3(local_store_path, dest_s3_path)

    return result


# ── CLI ───────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--mode",
        choices=["initial", "update"],
        default=os.environ.get("MODE"),
        help="Operation mode (env: MODE)",
    )
    p.add_argument(
        "--source-path",
        default=os.environ.get("SOURCE_PATH"),
        metavar="PATH",
        help="[initial] Local path or s3:// URI of the source NetCDF (env: SOURCE_PATH)",
    )
    p.add_argument(
        "--local-store-path",
        default=os.environ.get("LOCAL_STORE_PATH", "/tmp/realizations.icechunk"),
        metavar="PATH",
        help="Local path for the Icechunk store (env: LOCAL_STORE_PATH, "
             "default: /tmp/realizations.icechunk)",
    )
    p.add_argument(
        "--dest-s3-path",
        default=os.environ.get("DEST_S3_PATH"),
        metavar="S3_URI",
        help="s3:// destination on source.coop (env: DEST_S3_PATH); "
             "omit to skip upload",
    )
    p.add_argument(
        "--date",
        default=os.environ.get("DATE"),
        metavar="YYYY-MM",
        help="[update] Update date, e.g. 2025-04 (env: DATE)",
    )
    p.add_argument(
        "--mask",
        default=os.environ.get("MASK_PATH"),
        metavar="PATH",
        help="[update] s3:// URI of the basin mask .npy in source.coop "
             "(env: MASK_PATH)",
    )
    p.add_argument(
        "--data",
        default=os.environ.get("DATA_PATH"),
        metavar="PATH",
        help="[update] Local path or s3:// URI of the new data .npy "
             "(env: DATA_PATH)",
    )
    p.add_argument(
        "--message",
        default=os.environ.get("COMMIT_MESSAGE", ""),
        metavar="MSG",
        help="[update] Optional Icechunk commit message (env: COMMIT_MESSAGE); "
             "defaults to empty string",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "false").lower() == "true",
        help="[update] Validate without writing (env: DRY_RUN)",
    )
    p.add_argument(
        "--git-sha",
        default=os.environ.get("GIT_SHA", "unknown")[:8],
        help="[update] Git SHA for commit messages (env: GIT_SHA)",
    )
    p.add_argument(
        "--skip-upload",
        action="store_true",
        default=os.environ.get("SKIP_UPLOAD", "false").lower() == "true",
        help="Skip the S3 sync step — useful for local testing (env: SKIP_UPLOAD)",
    )
    return p


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    if not args.mode:
        parser.error("--mode is required (or set the MODE env var)")

    if args.mode == "initial":
        if not args.source_path:
            parser.error(
                "--source-path is required for initial mode (or set SOURCE_PATH)"
            )
        snapshot_id = run_initial(
            source_path=args.source_path,
            local_store_path=args.local_store_path,
            dest_s3_path=args.dest_s3_path,
            skip_upload=args.skip_upload,
        )
        Path("/tmp/ingest_result.txt").write_text(
            f"mode=initial\nsnapshot_id={snapshot_id}\n"
            f"store={args.local_store_path}\n"
            f"dest={args.dest_s3_path}\n"
        )

    else:  # update
        missing = [name for name, val in [
            ("--date", args.date),
            ("--mask", args.mask),
            ("--data", args.data),
        ] if not val]
        if missing:
            parser.error(
                f"update mode requires: {', '.join(missing)}"
            )
        result = run_update(
            local_store_path=args.local_store_path,
            date=args.date,
            mask_path=args.mask,
            data_path=args.data,
            dest_s3_path=args.dest_s3_path,
            dry_run=args.dry_run,
            git_sha=args.git_sha,
            skip_upload=args.skip_upload,
            message=args.message,
        )
        Path("/tmp/ingest_result.txt").write_text(result + "\n")


if __name__ == "__main__":
    main()
