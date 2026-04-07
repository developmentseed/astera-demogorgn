"""
initial_ingest.py — stream a NetCDF from S3 into a new Icechunk store on S3.

Environment variables (set by the GitHub Actions workflow):
  NETCDF_S3_PATH   s3://bucket/key/to/realizations_100.nc
  STORE_S3_PATH    s3://us-west-2.opendata.source.coop/<org>/astera/realizations.icechunk
  BATCH_SIZE       realizations per read/write pass (default 1)
"""
import os, sys, time, tracemalloc, tempfile
import numpy as np
import netCDF4 as nc
import icechunk, zarr, boto3
from zarr.codecs import BloscCodec
from pathlib import Path

NETCDF_S3_PATH = os.environ["NETCDF_S3_PATH"]
STORE_S3_PATH  = os.environ["STORE_S3_PATH"]
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE", "1"))
SHARD_SHAPE    = None   # set after reading array dimensions
INNER_CHUNK    = None

N_REALIZATIONS = 100

def parse_s3(path):
    path = path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    return bucket, key

def open_icechunk_s3(store_s3_path):
    bucket, prefix = parse_s3(store_s3_path)
    storage = icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region="us-west-2",
        # Credentials come from the ambient IAM role (OIDC); no keys needed.
    )
    return icechunk.Repository.open_or_create(storage=storage)

def log(msg):
    print(msg, flush=True)

def main():
    log(f"=== Initial Ingest ===")
    log(f"  Source : {NETCDF_S3_PATH}")
    log(f"  Store  : {STORE_S3_PATH}")
    log(f"  Batch  : {BATCH_SIZE}")

    # ── Download NetCDF to a temp file (S3 → local; netCDF4 needs a file path) ──
    s3 = boto3.client("s3")
    src_bucket, src_key = parse_s3(NETCDF_S3_PATH)

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp_path = tmp.name

    log(f"Downloading {NETCDF_S3_PATH} → {tmp_path} ...")
    t0 = time.perf_counter()
    s3.download_file(src_bucket, src_key, tmp_path,
                     Callback=lambda n: None)
    log(f"Download complete in {time.perf_counter()-t0:.1f}s")

    src = nc.Dataset(tmp_path, "r")
    src_var = src.variables["__xarray_dataarray_variable__"]
    ny, nx = src_var.shape[1], src_var.shape[2]
    log(f"NetCDF shape: ({src_var.shape[0]}, {ny}, {nx})  dtype: {src_var.dtype}")

    SHARD_SHAPE = (N_REALIZATIONS, 512, 512)
    INNER_CHUNK = (N_REALIZATIONS, 128, 128)

    # ── Open Icechunk store ───────────────────────────────────────────────────
    repo    = open_icechunk_s3(STORE_S3_PATH)
    session = repo.writable_session("main")
    store   = session.store

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
    for name, vals in [
        ("i", np.arange(N_REALIZATIONS, dtype="int64")),
        ("y", src.variables["y"][:].astype("float32")),
        ("x", src.variables["x"][:].astype("float32")),
    ]:
        root.create_array(name, shape=vals.shape, dtype=vals.dtype,
                          chunks=vals.shape, dimension_names=[name], overwrite=True)
        root[name][:] = vals

    # ── Streaming write ───────────────────────────────────────────────────────
    tracemalloc.start()
    t_ingest = time.perf_counter()
    batch_times = []

    for batch_start in range(0, N_REALIZATIONS, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, N_REALIZATIONS)
        t_b = time.perf_counter()

        batch = src_var[batch_start:batch_end, :, :].astype("float32")
        arr[batch_start:batch_end, :, :] = batch
        del batch

        elapsed = time.perf_counter() - t_b
        batch_times.append(elapsed)
        _, peak_mb = tracemalloc.get_traced_memory()
        if batch_end % 10 == 0 or batch_end == N_REALIZATIONS:
            log(f"  Written {batch_end:3d}/{N_REALIZATIONS}  "
                f"batch: {elapsed:.1f}s  peak traced: {peak_mb/1e6:.0f} MB")

    snapshot_id = session.commit(
        f"Initial write: {N_REALIZATIONS} realizations, float32, "
        f"sharded {SHARD_SHAPE}, batch_sz={BATCH_SIZE}"
    )
    src.close()
    os.unlink(tmp_path)

    t_total = time.perf_counter() - t_ingest
    _, peak_mb = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    result = (
        f"Total wall time  : {t_total:.1f}s ({t_total/60:.1f} min)\n"
        f"Mean batch time  : {np.mean(batch_times):.1f}s\n"
        f"Peak traced RAM  : {peak_mb/1e6:.0f} MB\n"
        f"Snapshot ID      : {snapshot_id}\n"
        f"Store            : {STORE_S3_PATH}\n"
    )
    log(result)
    Path("/tmp/ingest_result.txt").write_text(result)

if __name__ == "__main__":
    main()
