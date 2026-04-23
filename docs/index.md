# astera-demogorgn

Ingest and update pipeline for the DEMOGORGN ice-sheet topology data.

## Overview

This project manages a 100-realization Antarctic ice-sheet datacube stored as a sharded Zarr V3 array in an [Icechunk](https://icechunk.io) repository on [Source.Coop](https://source.coop/englacial/demogorgn).


See the [Pipeline](pipeline.md) page for detailed explanation + usage instructions for ingesting and updating the data archive.

## Data usage/reading

The Icechunk archives on source.coop are readable without credentials. The icechunk archives enable a few key features:

- Range requests (HTTP byte-range fetches) to enable spatial/seed subsetting without having to fetch the entire file
- Data versioning and reading from a specific point in time
- Efficient data compression (65Gb NetCDF becomes a 16Gb icechunk archive)
- Interoperability with Xarray library for data access (data can be fetched directly from the source.coop S3 bucket, without needing to download file to disk first)

The data fetching/usage examples below require:

```
pip install icechunk zarr xarray netCDF4 numpy
```

### Opening the stores using [Xarray](https://docs.xarray.dev/en/stable/)

Data proxy API note: the data is publicly readable without any account or credentials. source.coop exposes the data through a proxy API at `https://data.source.coop` that accepts S3-style requests. The `bucket` parameter below is set to the organisation name (`englacial`) rather than the underlying AWS bucket name — the proxy resolves this automatically. The `endpoint_url` parameter points requests at the proxy instead of AWS directly.

```python
import icechunk
import xarray as xr

# source.coop data proxy — serves public data without credentials.
# The org name ("englacial") is used as the S3 bucket; the repo path follows.
SOURCE_COOP_ENDPOINT = "https://data.source.coop"
ORG_NAME = "englacial"

def open_icechunk_repo(path: str): 
    storage = icechunk.s3_storage(
        bucket=ORG_NAME,
        prefix=path,
        endpoint_url=SOURCE_COOP_ENDPOINT,
        anonymous=True,
        force_path_style=True,
    )
    return icechunk.Repository.open(storage)

def open_store(path: str) ->  xr.Dataset:
    
    repo = open_icechunk_repo(path)

    # branch="main" fetches the most recent version of the data
    session = repo.readonly_session(branch="main")

    # zarr_format=3 enables Zarr v3 sharding (outer shards + inner chunks)
    return xr.open_zarr(session.store, consolidated=False, zarr_format=3)
 
 # realizations array + seed_id/y/x coords
realizations_dataset = open_store("demogorgn/icechunk/realizations.icechunk")  

# bed + y/x coords
mask_dataset = open_store("demogorgn/icechunk/antarctica_fast_flow_mask_50m.icechunk")       
```

??? example 
    Output:

    ```bash
    In [11]: realizations_dataset
    Out[11]:
    <xarray.Dataset> Size: 71GB
    Dimensions:       (seed_id: 100, y: 13334, x: 13334)
    Coordinates:
    * seed_id       (seed_id) int64 800B 0 1 2 3 4 5 6 7 ... 93 94 95 96 97 98 99
    * y             (y) float32 53kB 3.333e+06 3.333e+06 ... -3.333e+06 -3.333e+06
    * x             (x) float32 53kB -3.333e+06 -3.333e+06 ... 3.333e+06 3.333e+06
    Data variables:
        realizations  (seed_id, y, x) float32 71GB ...


    In [12]: mask_dataset
    Out[12]:
    <xarray.Dataset> Size: 4GB
    Dimensions:         (y: 12445, x: 12445)
    Coordinates:
    * y               (y) float32 50kB 2.8e+06 2.8e+06 ... -2.799e+06 -2.8e+06
    * x               (x) float32 50kB -2.8e+06 -2.8e+06 ... 2.799e+06 2.8e+06
    Data variables:
        fast_flow_mask  (y, x) int64 1GB ...
        lat             (y, x) float64 1GB ...
        lon             (y, x) float64 1GB ...
    ```

### 1. Windowed read and ensemble statistics

Fetch a spatial window and compute ensemble mean and spread across all 100 realizations. Only the Zarr chunks overlapping the bounding box are transferred — no full-array download.

```python
# Bounding box in EPSG:3031 metres
X_MIN, X_MAX = -500_000.0, -300_000.0
Y_MAX, Y_MIN =  500_000.0,  300_000.0   # descending — pass max first

realizations = realizations_dataset["realizations"]

subset = realizations.sel(
    x=slice(X_MIN, X_MAX),
    y=slice(Y_MAX, Y_MIN),   # y descending
)
print(f"Subset shape: {subset.shape}")   # (100, n_y, n_x)

# Ensemble mean and std over all 100 realizations
mean_elev = subset.mean(dim="seed_id").compute()
std_elev  = subset.std(dim="seed_id").compute()
```

??? example

    Output:
    ```
    In [13]: mean_elev
    Out[13]:
    <xarray.DataArray 'realizations' (y: 400, x: 400)> Size: 640kB
    array([[ -97.683464,  -73.27276 ,  -59.647495, ...,  439.58926 ,
            441.46985 ,  437.29483 ],
        [-101.85089 ,  -73.70325 ,  -71.27562 , ...,  439.52274 ,
            452.7914  ,  432.16333 ],
        [-148.86836 ,  -73.463455,  -40.387363, ...,  448.59723 ,
            447.99176 ,  425.14514 ],
        ...,
        [-548.1683  , -585.7937  , -615.783   , ..., -538.9274  ,
            -560.07916 , -565.3934  ],
        [-519.39343 , -517.37866 , -490.3994  , ..., -538.2435  ,
            -545.97833 , -546.2873  ],
        [-494.50247 , -465.4407  , -474.61633 , ..., -531.3872  ,
            -543.855   , -554.0597  ]], shape=(400, 400), dtype=float32)
    Coordinates:
    * y        (y) float32 2kB 4.998e+05 4.992e+05 ... 3.008e+05 3.002e+05
    * x        (x) float32 2kB -4.998e+05 -4.992e+05 ... -3.008e+05 -3.002e+05

    In [14]: std_elev
    Out[14]:
    <xarray.DataArray 'realizations' (y: 400, x: 400)> Size: 640kB
    array([[110.19651   , 109.45956   , 104.12201   , ..., 151.21176   ,
            151.14178   , 153.03677   ],
        [ 98.31736   ,  95.88217   ,  96.83175   , ..., 155.6633    ,
            156.82104   , 156.46309   ],
        [ 91.93957   ,  87.40099   ,  85.697334  , ..., 166.27524   ,
            164.57101   , 165.68294   ],
        ...,
        [ 34.066845  ,  33.748405  ,  29.48755   , ...,   0.5202135 ,
            18.734865  ,  26.90109   ],
        [ 21.018694  ,  26.476627  ,  26.426537  , ...,   0.48180303,
            0.54081994,  18.593473  ],
        [  0.769584  ,   0.8082617 ,   0.8655422 , ...,  19.196617  ,
            0.5384659 ,   0.5499978 ]], shape=(400, 400), dtype=float32)
    Coordinates:
    * y        (y) float32 2kB 4.998e+05 4.992e+05 ... 3.008e+05 3.002e+05
    * x        (x) float32 2kB -4.998e+05 -4.992e+05 ... -3.008e+05 -3.002e+05
    ```

#### Subset to a specific set of realizations

Use `.sel(seed_id=...)` to load only a subset of the ensemble rather than all 100:

```python
# Select realizations 0, 10, and 42 by label
subset_3 = realizations.sel(
    x=slice(X_MIN, X_MAX),
    y=slice(Y_MAX, Y_MIN),
    seed_id=[0, 10, 42],
)

# Or select a contiguous range with slice
subset_first20 = realizations.sel(
    x=slice(X_MIN, X_MAX),
    y=slice(Y_MAX, Y_MIN),
    seed_id=slice(0, 19),
)
```

??? example 
    Output: 
    ```
    In [19]: subset_3
    Out[19]:
    <xarray.DataArray 'realizations' (seed_id: 3, y: 400, x: 400)> Size: 2MB
    [480000 values with dtype=float32]
    Coordinates:
    * seed_id  (seed_id) int64 24B 0 10 42
    * y        (y) float32 2kB 4.998e+05 4.992e+05 ... 3.008e+05 3.002e+05
    * x        (x) float32 2kB -4.998e+05 -4.992e+05 ... -3.008e+05 -3.002e+05

    In [20]: subset_first20
    Out[20]:
    <xarray.DataArray 'realizations' (seed_id: 20, y: 400, x: 400)> Size: 13MB
    [3200000 values with dtype=float32]
    Coordinates:
    * seed_id  (seed_id) int64 160B 0 1 2 3 4 5 6 7 8 ... 12 13 14 15 16 17 18 19
    * y        (y) float32 2kB 4.998e+05 4.992e+05 ... 3.008e+05 3.002e+05
    * x        (x) float32 2kB -4.998e+05 -4.992e+05 ... -3.008e+05 -3.002e+05
    ```

#### Apply the mask: statistics for ice/rock pixels only

Load the corresponding spatial window from the mask store and use `xr.where` to restrict statistics to pixels where the fast-flow mask equals 1:

```python
import numpy as np

# Load the same spatial window from the mask store
fast_flow_mask_window = mask_dataset["fast_flow_mask"].sel(
    x=slice(X_MIN, X_MAX),
    y=slice(Y_MAX, Y_MIN),
)

# Align mask to the realizations grid (handles any minor float coord differences)
fast_flow_mask_aligned = fast_flow_mask_window.interp_like(subset, method="nearest")

# Mask: keep only pixels where fast_flow_mask == 1 (fast-flow pixels)
masked_subset = subset.where(fast_flow_mask_aligned == 1)

mean_masked = masked_subset.mean(dim="seed_id").compute()
std_masked  = masked_subset.std(dim="seed_id").compute()

# Count of valid pixels per location
n_valid = (~np.isnan(masked_subset)).sum(dim="seed_id").compute()
```

??? example
    Output: 
    ```
    In [23]: mean_masked
    Out[23]:
    <xarray.DataArray 'realizations' (y: 400, x: 400)> Size: 640kB
    array([[       nan,        nan,        nan, ...,        nan,        nan,
                nan],
        [       nan,        nan,        nan, ...,        nan,        nan,
                nan],
        [       nan,        nan,        nan, ...,        nan,        nan,
                nan],
        ...,
        [       nan, -585.7937 , -615.783  , ...,        nan,        nan,
                nan],
        [       nan, -517.37866, -490.3994 , ...,        nan,        nan,
                nan],
        [       nan, -465.4407 , -474.61633, ...,        nan,        nan,
                nan]], shape=(400, 400), dtype=float32)
    Coordinates:
    * y        (y) float32 2kB 4.998e+05 4.992e+05 ... 3.008e+05 3.002e+05
    * x        (x) float32 2kB -4.998e+05 -4.992e+05 ... -3.008e+05 -3.002e+05

    In [24]: std_masked
    Out[24]:
    <xarray.DataArray 'realizations' (y: 400, x: 400)> Size: 640kB
    array([[       nan,        nan,        nan, ...,        nan,        nan,
                nan],
        [       nan,        nan,        nan, ...,        nan,        nan,
                nan],
        [       nan,        nan,        nan, ...,        nan,        nan,
                nan],
        ...,
        [       nan, 33.748405 , 29.48755  , ...,        nan,        nan,
                nan],
        [       nan, 26.476627 , 26.426537 , ...,        nan,        nan,
                nan],
        [       nan,  0.8082617,  0.8655422, ...,        nan,        nan,
                nan]], shape=(400, 400), dtype=float32)
    Coordinates:
    * y        (y) float32 2kB 4.998e+05 4.992e+05 ... 3.008e+05 3.002e+05
    * x        (x) float32 2kB -4.998e+05 -4.992e+05 ... -3.008e+05 -3.002e+05

    In [25]: n_valid
    Out[25]:
    <xarray.DataArray 'realizations' (y: 400, x: 400)> Size: 1MB
    array([[  0,   0,   0, ...,   0,   0,   0],
        [  0,   0,   0, ...,   0,   0,   0],
        [  0,   0,   0, ...,   0,   0,   0],
        ...,
        [  0, 100, 100, ...,   0,   0,   0],
        [  0, 100, 100, ...,   0,   0,   0],
        [  0, 100, 100, ...,   0,   0,   0]], shape=(400, 400))
    Coordinates:
    * y        (y) float32 2kB 4.998e+05 4.992e+05 ... 3.008e+05 3.002e+05
    * x        (x) float32 2kB -4.998e+05 -4.992e+05 ... -3.008e+05 -3.002e+05
    ```

#### Interpolate all realizations along a transect

Query all realizations at a set of (x, y) points — e.g. a flow line or flight track:

```python
n_pts   = 40
track_x = xr.DataArray(np.linspace(X_MIN, X_MAX, n_pts), dims="points")
track_y = xr.DataArray(np.full(n_pts, (Y_MIN + Y_MAX) / 2.0), dims="points")

profile = realizations.interp(x=track_x, y=track_y, method="linear").compute()
# shape: (100, 40) — all realizations at each sample point

ens_mean = profile.mean("seed_id")
ens_std  = profile.std("seed_id")
```

??? example
    Output: 
    ```
    In [28]: profile
    Out[28]:
    <xarray.DataArray 'realizations' (seed_id: 100, points: 40)> Size: 32kB
    array([[ -961.25247192, -1054.83587881,  -967.94859001, ...,
            -109.93409856,   103.08650599,    39.43743896],
        [ -974.63633728, -1004.20980483,  -982.45917061, ...,
            -154.16314629,    19.31907243,  -136.76080322],
        [-1025.77485657,  -975.99843069,  -966.41567015, ...,
            -95.94231591,   159.46286676,   133.05879211],
        ...,
        [ -822.87649536,  -979.47450804,  -968.67190943, ...,
            -300.77180256,  -242.96932543,   -51.56808472],
        [-1123.45834351,  -940.37931589, -1000.63571128, ...,
            -356.56628829,  -153.5598658 ,   -93.60653687],
        [ -921.12138367, -1023.71966161,  -978.53516721, ...,
            -284.23929655,  -163.36387624,   167.20947266]], shape=(100, 40))
    Coordinates:
    * seed_id  (seed_id) int64 800B 0 1 2 3 4 5 6 7 8 ... 92 93 94 95 96 97 98 99
        x        (points) float64 320B -5e+05 -4.949e+05 ... -3.051e+05 -3e+05
        y        (points) float64 320B 4e+05 4e+05 4e+05 4e+05 ... 4e+05 4e+05 4e+05
    Dimensions without coordinates: points

    In [29]: ens_mean
    Out[29]:
    <xarray.DataArray 'realizations' (points: 40)> Size: 320B
    array([-1021.8202037 , -1019.41738975,  -978.44554061,  -796.33171683,
            -583.99786688,  -349.13191487,  -229.17934653,  -311.23866512,
            -307.66454722,  -280.18081343,  -230.02058457,  -255.05921741,
            -275.63103857,  -266.74918137,  -253.81910123,  -287.4166736 ,
            -321.45604195,  -342.78256732,  -306.89033921,  -282.28122682,
            -191.96679255,  -113.15776882,  -149.47157208,  -207.26063681,
            -251.49413415,  -280.01643827,  -264.26559939,  -272.89527798,
            -254.60997943,  -251.56994672,  -268.01083494,  -332.21373794,
            -292.10172678,  -217.36142002,  -239.45695652,  -272.87808178,
            -249.84522846,  -165.78308653,   -26.41491708,   152.19946373])
    Coordinates:
        x        (points) float64 320B -5e+05 -4.949e+05 ... -3.051e+05 -3e+05
        y        (points) float64 320B 4e+05 4e+05 4e+05 4e+05 ... 4e+05 4e+05 4e+05
    Dimensions without coordinates: points

    In [30]: ens_std
    Out[30]:
    <xarray.DataArray 'realizations' (points: 40)> Size: 320B
    array([ 75.92927152,  39.23309003,  12.8033041 ,  18.10677637,
            66.00417325,  35.30941325,  57.80556286,  92.54603277,
        107.78362999, 115.58991203,  93.97249718,  47.42806045,
            14.16991193,   2.22160587,  60.78443185,  65.52130951,
            77.94345752,  53.79098962,  16.01752299,  20.71410336,
            11.15452578,   6.47763946,  52.14626643,  86.16769155,
        103.58498805, 140.41046207, 143.46020089, 144.44870255,
        138.00274912, 110.43935439,  67.79939245,  28.48681738,
            53.79894798,  79.71755009, 107.68589255, 129.17190639,
        140.05805711, 182.58060903, 211.03855155, 267.74695776])
    Coordinates:
        x        (points) float64 320B -5e+05 -4.949e+05 ... -3.051e+05 -3e+05
        y        (points) float64 320B 4e+05 4e+05 4e+05 4e+05 ... 4e+05 4e+05 4e+05
    Dimensions without coordinates: points
    ```

### 2. Reading a previous version of the data

Every ingest and update creates a new Icechunk snapshot. You can list all snapshots and pin a read session to any of them — useful for comparing before/after an update or reproducing a historical result.

```python

repo = open_icechunk_repo("demogorgn/icechunk/realizations.icechunk")

for snap in repo.ancestry(branch="main"):
    print(snap.id, snap.written_at.strftime("%Y-%m-%d %H:%M"), snap.message)
```

??? example 
    Output:

    ```
    3KQWZ9ABCDEF  2026-04-17 14:32  Bed 990k update 2026-04-17
    EZHA4B867A1QGNANNHWG 2026-04-17 20:00 Initial write: 100 realizations, float32, sharded (100, 512, 512)
    1CECHNKREP0F1RSTCMT0 2026-04-17 19:35 Repository initialized
    ```

Pin to a specific snapshot by ID and perform a windowed read exactly as with the latest version:

```python
SNAPSHOT_ID = "EZHA4B867A1QGNANNHWG"   # paste any ID from the ancestry list above

session_v1 = repo.readonly_session(snapshot_id=SNAPSHOT_ID)
ds_v1 = xr.open_zarr(session_v1.store, consolidated=False, zarr_format=3)

# Windowed read from the historical snapshot — identical API to the latest version
subset_v1 = ds_v1["realizations"].sel(
    x=slice(X_MIN, X_MAX),
    y=slice(Y_MAX, Y_MIN),
).compute()
```

??? example
    Output: 
    ```
    In [33]: subset_v1
    Out[33]:
    <xarray.DataArray 'realizations' (seed_id: 100, y: 400, x: 400)> Size: 64MB
    array([[[-200.67606  , -174.63708  ,  -85.02551  , ...,  341.7792   ,
            345.73734  ,  337.80518  ],
            [-106.97699  ,  -56.186707 ,  -37.67279  , ...,  346.31342  ,
            376.39032  ,  378.33807  ],
            [-106.676025 ,    1.942688 ,  -29.944275 , ...,  401.03537  ,
            407.4972   ,  406.6589   ],
            ...,
            [-495.20203  , -567.2076   , -628.6028   , ..., -538.5767   ,
            -528.387    , -538.44824  ],
            [-468.63644  , -501.48737  , -507.57205  , ..., -538.2457   ,
            -546.1842   , -538.4732   ],
            [-494.6848   , -463.87448  , -475.35132  , ..., -494.19965  ,
            -544.09375  , -555.2104   ]],

        [[  84.25458  ,  103.15234  ,  130.83118  , ...,  334.74634  ,
            385.07642  ,  420.97067  ],
            [  55.274353 ,  109.89746  ,   95.171265 , ...,  330.11127  ,
            393.92426  ,  446.16312  ],
            [ -56.641846 ,   42.43811  ,   89.48743  , ...,  419.33066  ,
            409.65173  ,  481.05902  ],
    ...
            -541.2251   , -576.3712   ],
            [-515.65106  , -492.02542  , -460.78717  , ..., -537.32153  ,
            -545.4486   , -557.9584   ],
            [-493.5639   , -465.39612  , -472.8019   , ..., -491.39206  ,
            -544.0647   , -554.7223   ]],

        [[-152.52176  , -165.12155  , -167.79361  , ...,  457.6871   ,
            477.14352  ,  462.36053  ],
            [-171.27637  , -176.81793  , -159.10959  , ...,  523.0783   ,
            565.5596   ,  504.10703  ],
            [-221.35071  , -161.07932  , -134.29977  , ...,  579.3513   ,
            612.62225  ,  591.658    ],
            ...,
            [-554.4741   , -596.36755  , -610.6242   , ..., -538.46094  ,
            -569.50775  , -564.245    ],
            [-532.1763   , -509.6422   , -504.7761   , ..., -537.38776  ,
            -546.7633   , -550.37463  ],
            [-493.93304  , -465.5547   , -474.09924  , ..., -549.8597   ,
            -543.3654   , -553.6005   ]]],
        shape=(100, 400, 400), dtype=float32)
    Coordinates:
    * seed_id  (seed_id) int64 800B 0 1 2 3 4 5 6 7 8 ... 92 93 94 95 96 97 98 99
    * y        (y) float32 2kB 4.998e+05 4.992e+05 ... 3.008e+05 3.002e+05
    * x        (x) float32 2kB -4.998e+05 -4.992e+05 ... -3.008e+05 -3.002e+05
    ```

Compare a specific spatial window between two versions:

```python
session_latest = repo.readonly_session(branch="main")
ds_latest = xr.open_zarr(session_latest.store, consolidated=False, zarr_format=3)

subset_latest = ds_latest["realizations"].sel(
    x=slice(X_MIN, X_MAX),
    y=slice(Y_MAX, Y_MIN),
).compute()

diff = subset_latest - subset_v1
print(f"Max change in window: {float(diff.max()):.4f} m")
print(f"Pixels changed:       {int((diff != 0).sum())}")
```

(TODO: rerun example after partial update)
??? example 
    Output: 
    ```
        In [34]: session_latest = repo.readonly_session(branch="main")
        ...: ds_latest = xr.open_zarr(session_latest.store, consolidated=False, zarr_format=3)
        ...:
        ...: subset_latest = ds_latest["realizations"].sel(
        ...:     x=slice(X_MIN, X_MAX),
        ...:     y=slice(Y_MAX, Y_MIN),
        ...: ).compute()
        ...:
        ...: diff = subset_latest - subset_v1
        ...: print(f"Max change in window: {float(diff.max()):.4f} m")
        ...: print(f"Pixels changed:       {int((diff != 0).sum())}")
    Max change in window: 0.0000 m
    Pixels changed:       0
    ```

---

### 3. Export a windowed subset to a local NetCDF

Download a spatial window (and optionally a subset of realizations) and write it to a self-contained NetCDF4 file for offline use.

```python
import netCDF4 as nc_lib
import numpy as np

OUT_PATH = "./realizations_subset.nc"

# ── Define the subset ────────────────────────────────────────────────────────
X_MIN, X_MAX = -200_000.0, 100_000.0
Y_MAX, Y_MIN =  -900_000.0, -1_300_000.0
SEED_IDS     = list(range(100))   # all 100; narrow to e.g. [0, 1, 2] for a quick test

realizations = realizations_dataset["realizations"]
subset = realizations.sel(
    x=slice(X_MIN, X_MAX),
    y=slice(Y_MAX, Y_MIN),
    seed_id=SEED_IDS,
).compute()

x_vals      = subset.x.values.astype("float32")
y_vals      = subset.y.values.astype("float32")
seed_id_vals = subset.seed_id.values.astype("int64")
n_seeds, ny, nx = subset.shape

# ── Write NetCDF ─────────────────────────────────────────────────────────────
with nc_lib.Dataset(OUT_PATH, "w", format="NETCDF4") as dst:
    dst.createDimension("seed_id", n_seeds)
    dst.createDimension("y", ny)
    dst.createDimension("x", nx)

    # Coordinate variables
    v = dst.createVariable("seed_id", "i8", ("seed_id",)); v[:] = seed_id_vals
    v = dst.createVariable("y",  "f4", ("y",));  v[:] = y_vals;  v.units = "metres"
    v = dst.createVariable("x",  "f4", ("x",));  v[:] = x_vals;  v.units = "metres"

    # Main data variable — chunked for efficient row-oriented reads
    data_var = dst.createVariable(
        "realizations", "f4", ("seed_id", "y", "x"),
        chunksizes=(1, 128, 128),
        zlib=True, complevel=4,
        fill_value=np.nan,
    )
    data_var[:] = subset.values
    data_var.grid_mapping = "crs"
    data_var.long_name    = "bed elevation ensemble"
    data_var.units        = "m"

    dst.Conventions = "CF-1.8"
    dst.source      = "s3://us-west-2.opendata.source.coop/englacial/demogorgn/icechunk/realizations.icechunk"

print(f"Written: {OUT_PATH}  ({ny} × {nx}, {n_seeds} realizations)")
```

#### Include the mask in the same file

```python
bed_window = mask_dataset["fast_flow_mask"].sel(
    x=slice(X_MIN, X_MAX),
    y=slice(Y_MAX, Y_MIN),
).interp_like(subset.isel(seed_id=0), method="nearest").compute()

with nc_lib.Dataset(OUT_PATH, "a") as dst:
    mvar = dst.createVariable(
        "fast_flow_mask", "i1", ("y", "x"),
        zlib=True, complevel=4,
    )
    mvar[:] = bed_window.values.astype("int8")
    mvar.long_name = "fast-flow methodology mask (1 = fast flow)"
```

## Grid reference

| Property | Value |
|----------|-------|
| Projection | EPSG:3031 (Antarctic Polar Stereographic) |
| Resolution | 500 m |
| Extent | ~6,667 km x 6,667 km |
| Grid size | 13,334 x 13,334 cells |
| Realizations | 100 stochastic model runs |
| Dtype | float32 |
| Compression | zstd (level 5) |
| Shards | (100, 512, 512) |
| Chunks | (100, 128, 128) |
