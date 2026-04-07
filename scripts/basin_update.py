"""
basin_update.py — apply one or more basin updates to an existing Icechunk store.

Reads a space-separated list of YAML manifest files from the MANIFEST_FILES
environment variable. Each manifest describes one basin update:

  basin: jakobshavn
  date: 2025-04
  mask_s3_path: s3://.../<org>/astera/masks/jakobshavn.npy
  data_s3_path: s3://.../<org>/astera/model-output/jakobshavn/2025-04/data.npy

Environment variables (set by GitHub Actions):
  MANIFEST_FILES   space-separated list of manifest paths
  STORE_S3_PATH    s3://us-west-2.opendata.source.coop/<org>/astera/realizations.icechunk
  DRY_RUN          "true" to validate without writing
  GIT_SHA          the triggering commit SHA (used in commit message)
"""
import os, sys, tempfile, time
import numpy as np
import yaml, boto3
import icechunk, zarr
from pathlib import Path

MANIFEST_FILES = os.environ["MANIFEST_FILES"].split()
STORE_S3_PATH  = os.environ["STORE_S3_PATH"]
DRY_RUN        = os.environ.get("DRY_RUN", "false").lower() == "true"
GIT_SHA        = os.environ.get("GIT_SHA", "unknown")[:8]

def parse_s3(path):
    path = path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    return bucket, key

def download_npy(s3, s3_path):
    bucket, key = parse_s3(s3_path)
    with tempfile.NamedTemporaryFile(suffix=".npy", delete=False) as tmp:
        tmp_path = tmp.name
    s3.download_file(bucket, key, tmp_path)
    arr = np.load(tmp_path)
    os.unlink(tmp_path)
    return arr

def open_icechunk_s3(store_s3_path, writable=False):
    bucket, prefix = parse_s3(store_s3_path)
    storage = icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region="us-west-2",
    )
    repo = icechunk.Repository.open(storage)
    if writable:
        return repo, repo.writable_session("main")
    return repo, repo.readonly_session("main")

def update_basin(arr, basin_mask, new_data):
    rows = np.where(basin_mask.any(axis=1))[0]
    cols = np.where(basin_mask.any(axis=0))[0]
    row_sl = slice(int(rows[0]), int(rows[-1]) + 1)
    col_sl = slice(int(cols[0]), int(cols[-1]) + 1)
    arr[:, row_sl, col_sl] = new_data.astype("float32")
    return row_sl, col_sl

def log(msg):
    print(msg, flush=True)

def main():
    s3 = boto3.client("s3")
    results = []

    log(f"=== Basin Update{'  [DRY RUN]' if DRY_RUN else ''} ===")
    log(f"  Store     : {STORE_S3_PATH}")
    log(f"  Manifests : {MANIFEST_FILES}")
    log(f"  Git SHA   : {GIT_SHA}")

    for manifest_path in MANIFEST_FILES:
        log(f"\n--- Processing {manifest_path} ---")
        manifest = yaml.safe_load(Path(manifest_path).read_text())

        basin      = manifest["basin"]
        date       = manifest["date"]
        mask_path  = manifest["mask_s3_path"]
        data_path  = manifest["data_s3_path"]

        log(f"  Basin : {basin}  date: {date}")
        log(f"  Mask  : {mask_path}")
        log(f"  Data  : {data_path}")

        # ── Download mask and data ──────────────────────────────────────────
        log("  Downloading mask ...")
        basin_mask = download_npy(s3, mask_path).astype(bool)

        log("  Downloading data ...")
        new_data = download_npy(s3, data_path).astype("float32")

        rows_with_data = np.where(basin_mask.any(axis=1))[0]
        cols_with_data = np.where(basin_mask.any(axis=0))[0]
        bbox_h = int(rows_with_data[-1]) - int(rows_with_data[0]) + 1
        bbox_w = int(cols_with_data[-1]) - int(cols_with_data[0]) + 1
        log(f"  Mask bbox : {bbox_h} rows × {bbox_w} cols")
        log(f"  Data shape: {new_data.shape}")

        # ── Validate shapes ─────────────────────────────────────────────────
        expected = (new_data.shape[0], bbox_h, bbox_w)
        if new_data.shape != expected:
            raise ValueError(
                f"Data shape {new_data.shape} doesn't match mask bbox {expected}"
            )

        if DRY_RUN:
            log("  [DRY RUN] Skipping write.")
            results.append(f"{basin} ({date}): DRY RUN — validated OK")
            continue

        # ── Apply update ────────────────────────────────────────────────────
        t0 = time.perf_counter()
        repo, session = open_icechunk_s3(STORE_S3_PATH, writable=True)
        arr = zarr.open_group(session.store, mode="r+")["realizations"]

        row_sl, col_sl = update_basin(arr, basin_mask, new_data)

        snapshot_id = session.commit(
            f"Update {basin} {date} — git:{GIT_SHA}"
        )
        elapsed = time.perf_counter() - t0

        msg = (f"{basin} ({date}): OK — "
               f"rows {row_sl.start}:{row_sl.stop} cols {col_sl.start}:{col_sl.stop}  "
               f"snapshot={snapshot_id}  {elapsed:.1f}s")
        log(f"  {msg}")
        results.append(msg)

    # ── Write summary ────────────────────────────────────────────────────────
    summary = "\n".join(results)
    log(f"\n=== Summary ===\n{summary}")
    Path("/tmp/update_result.txt").write_text(summary)

if __name__ == "__main__":
    main()
