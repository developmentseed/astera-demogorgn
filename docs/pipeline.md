# Data Pipeline

## Datacube

The pipeline produces a sharded Zarr V3 datacube in an Icechunk store on [Source.Coop](https://source.coop/englacial/demogorgn):

<p align="center">
  <img src="../images/cube-full.svg" alt="Full Antarctic datacube" width="520"/>
</p>

Basin updates write only the bounding box of a drainage basin, replacing that spatial region across all 100 realizations:

<p align="center">
  <img src="../images/cube-basin-update.svg" alt="Basin update: partial write" width="520"/>
</p>

---

## Initial Ingest

One-time ingestion of the full Antarctic ice-sheet datacube from a NetCDF source file. Scientists upload the NetCDF to S3, then trigger the workflow manually.

<p align="center">
  <img src="../images/pipeline-ingest.svg" alt="Initial ingest: HPC to S3 to GitHub Actions to Icechunk"/>
</p>

Each batch is a single realization slab streamed from the NetCDF:

<p align="center">
  <img src="../images/cube-batch.svg" alt="Single ingest batch" width="480"/>
</p>

---

## Basin Update

Incremental update replacing a single drainage basin's spatial region. Triggered automatically when a YAML manifest is pushed to `updates/`, or manually via workflow_dispatch.

<p align="center">
  <img src="../images/pipeline-update.svg" alt="Basin update: HPC to S3 to GitHub Actions to Icechunk"/>
</p>

### YAML manifest format

```yaml
basin: jakobshavn
date: "2025-04"
mask_s3_path: "s3://us-west-2.opendata.source.coop/englacial/demogorgn/masks/jakobshavn.npy"
data_s3_path: "s3://us-west-2.opendata.source.coop/englacial/demogorgn/model-output/jakobshavn/2025-04/data.npy"
```

Commit the manifest to `updates/` on the `main` branch. The workflow detects changed YAML files and passes them to `basin_update.py`.

---

## Summary

| | Initial Ingest | Basin Update |
|---|---|---|
| **Trigger** | Manual (workflow_dispatch) | Push YAML to `updates/` or manual |
| **Runner** | 16-core, 16 GB RAM | Standard |
| **Source data** | NetCDF on S3 | mask + data .npy on S3 |
| **Write scope** | Full cube | Bounding box of basin mask |

---

## Alternative design: direct from HPC

The current pipeline routes data through GitHub Actions, which means source files (NetCDF, .npy) must be uploaded to S3 before processing. An alternative design would have scientists run ingest and update scripts directly from HPC, writing to the Icechunk store on Source.Coop without an intermediate upload step.

<p align="center">
  <img src="../images/pipeline-option2.svg" alt="Alternative: HPC scripts write directly to Source.Coop"/>
</p>

This approach could also use **read-modify-write** for basin updates: read the current bounding box from the store, overwrite only the pixels inside the basin mask in memory, then write the bounding box back. Zarr operates on rectangular chunks, so the full bounding box is still transferred over the network in both directions, but non-masked pixels within the bbox are preserved. This is safer when basin bounding boxes overlap.

### Data sources for updates

With the direct-from-HPC approach, scientists can supply basin update data from either source:

- **Local files**: point the update script at `.npy` or `.nc` files on the HPC filesystem (no upload step needed)
- **S3**: if the data has already been uploaded to Source.Coop, the script could read it directly from S3 using the same `icechunk.s3_storage` credentials

This means scientists who have already uploaded their realizations to S3 for the GitHub Actions pipeline can reuse that data without re-uploading, while scientists working locally can skip the upload entirely.

### Handling NaN values in overlapping bounding boxes

Model output contains valid data only within the basin mask. Pixels outside the mask but inside the bounding box are NaN. When neighbouring basins have overlapping bounding boxes, writing the full bbox can overwrite valid data from a previously updated basin with NaN.

<p align="center">
  <img src="../images/overlap-problem.svg" alt="Overlapping bounding boxes: the NaN problem"/>
</p>

Icechunk's versioning means the data is never truly lost (the previous snapshot is preserved and accessible via `readonly_session(snapshot_id=...)`), but the latest version would be incorrect. A **read-modify-write** with the basin mask avoids this: only pixels where the mask is `True` are updated, so NaN values outside the basin are never written to the store.

!!! note
    This applies to the current GitHub Actions pipeline, which writes the full bounding box. The alternative design's read-modify-write approach would handle this correctly by skipping NaN pixels outside the mask.

### Comparison

| | Current (GitHub Actions) | Alternative (Direct from HPC) |
|---|---|---|
| **Automation** | Fully automated via CI/CD | Manual script invocation |
| **Source data** | Must be uploaded to S3 first | Local files or S3 |
| **Audit trail** | Git commits + workflow logs | Icechunk snapshots only |
| **Basin precision** | Writes full bounding box (including NaN) | Read-modify-write skips NaN outside mask |
| **Overlapping basins** | NaN can overwrite valid data from neighbours | Safe, only masked pixels changed |
| **Network** | S3 &rarr; runner &rarr; S3 (double transfer) | HPC &rarr; S3 (single transfer, but bbox read + write) |
