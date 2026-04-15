"""
initial_ingest.py — stream a NetCDF from S3 into a new Icechunk store on S3.

Environment variables (set by the GitHub Actions workflow):
  NETCDF_S3_PATH   s3://bucket/key/to/realizations_100.nc
  STORE_S3_PATH    s3://us-west-2.opendata.source.coop/<org>/astera/realizations.icechunk
"""
import os, time, tracemalloc, tempfile
import numpy as np
import netCDF4 as nc
import icechunk, zarr, boto3
from zarr.codecs import BloscCodec
from pathlib import Path

NETCDF_S3_PATH = os.environ["NETCDF_S3_PATH"]
STORE_S3_PATH  = os.environ["STORE_S3_PATH"]

N_REALIZATIONS = 100
SHARD_SHAPE    = (N_REALIZATIONS, 512, 512)
INNER_CHUNK    = (N_REALIZATIONS, 128, 128)

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
    )
    return icechunk.Repository.open_or_create(storage=storage)

def log(msg):
    print(msg, flush=True)

def main():
    log("=== Initial Ingest ===")
    log(f"  Source : {NETCDF_S3_PATH}")
    log(f"  Store  : {STORE_S3_PATH}")

    # ── Download NetCDF to a temp file (netCDF4 needs a local file path) ─────
    s3 = boto3.client("s3")
    src_bucket, src_key = parse_s3(NETCDF_S3_PATH)

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
        tmp_path = tmp.name

    log(f"Downloading {NETCDF_S3_PATH} → {tmp_path} ...")
    t0 = time.perf_counter()
    s3.download_file(src_bucket, src_key, tmp_path)
    log(f"Download complete in {time.perf_counter()-t0:.1f}s")

    src     = nc.Dataset(tmp_path, "r")
    src_var = src.variables["__xarray_dataarray_variable__"]
    ny, nx  = src_var.shape[1], src_var.shape[2]
    log(f"NetCDF shape: {src_var.shape}  dtype: {src_var.dtype}")

    n_shards_y = -(-ny // SHARD_SHAPE[1])
    n_shards_x = -(-nx // SHARD_SHAPE[2])
    n_shards   = n_shards_y * n_shards_x
    log(f"Spatial shards: {n_shards_y} × {n_shards_x} = {n_shards}")

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

    # ── Streaming write: one complete shard at a time ─────────────────────────
    # SHARD_SHAPE[0] == N_REALIZATIONS, so each shard spans all realizations for
    # a 512×512 spatial tile. Writing one complete shard at a time means every
    # shard is written exactly once — no read-modify-write amplification.
    tracemalloc.start()
    t_ingest    = time.perf_counter()
    shard_times = []
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
                log(f"  Shard {n_done:4d}/{n_shards}  "
                    f"last: {shard_times[-1]:.1f}s  "
                    f"elapsed: {elapsed:.0f}s  "
                    f"peak traced RAM: {peak_bytes/1e9:.2f} GB")

    snapshot_id = session.commit(
        f"Initial write: {N_REALIZATIONS} realizations, float32, sharded {SHARD_SHAPE}"
    )
    src.close()
    os.unlink(tmp_path)

    t_total = time.perf_counter() - t_ingest
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    result = (
        f"Total wall time  : {t_total:.1f}s ({t_total/60:.1f} min)\n"
        f"Mean shard time  : {np.mean(shard_times):.2f}s\n"
        f"Peak traced RAM  : {peak_bytes/1e6:.0f} MB\n"
        f"Snapshot ID      : {snapshot_id}\n"
        f"Store            : {STORE_S3_PATH}\n"
    )
    log(result)
    Path("/tmp/ingest_result.txt").write_text(result)

if __name__ == "__main__":
    main()
