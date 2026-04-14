# astera-demogorgn

Ingest and update pipeline for the DEMOGORGN ice-sheet topology data.

## Overview

This project manages a 100-realization Antarctic ice-sheet datacube stored as a sharded Zarr V3 array in an [Icechunk](https://icechunk.io) repository on [Source.Coop](https://source.coop/englacial/demogorgn).

Two GitHub Actions workflows automate the pipeline:

- **Initial Ingest**: one-time conversion of a NetCDF source file into the Icechunk store
- **Basin Update**: incremental updates to individual drainage basins, triggered by pushing a YAML manifest

See the [Pipeline](pipeline.md) page for detailed diagrams.

## Reading the data

```python
import icechunk
import xarray as xr

storage = icechunk.s3_storage(
    bucket="us-west-2.opendata.source.coop",
    prefix="englacial/demogorgn/realizations.icechunk",
    region="us-west-2",
)
repo = icechunk.Repository.open(storage)
session = repo.readonly_session("main")

ds = xr.open_zarr(session.store, consolidated=False, zarr_format=3)
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
