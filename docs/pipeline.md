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

One-time ingestion of the full Antarctic ice-sheet datacube. Raw model output (residual fields) is postprocessed on HPC before ingestion: the Bedmap3 trend is added back to each residual, and a mask is applied to keep simulated values where ice or rock is present and use the original Bedmap3 bed topography elsewhere.

Each batch is a single realization slab streamed into the store:

<p align="center">
  <img src="../images/cube-batch.svg" alt="Single ingest batch" width="480"/>
</p>

### Current design: GitHub Actions

The postprocessed NetCDF is uploaded to S3, then the workflow is triggered manually.

<p align="center">
  <img src="../images/pipeline-ingest.svg" alt="Ingest via GitHub Actions: HPC to S3 to GitHub Actions to Icechunk"/>
</p>

### Alternative design: direct from HPC

Scientists run the ingest script directly on HPC. The NetCDF stays on the local filesystem and is streamed to the Icechunk store on Source.Coop without an intermediate S3 upload.

<p align="center">
  <img src="../images/pipeline-option2-ingest.svg" alt="Ingest direct from HPC to Source.Coop"/>
</p>

---

## Basin Update

Incremental update replacing a single drainage basin's spatial region across all realizations. Raw basin model output is postprocessed on HPC (add trend, apply Bedmap3 mask) before the update is applied.

### Current design: GitHub Actions

The postprocessed mask and data are uploaded to S3. A YAML manifest is committed to `updates/`, which triggers the workflow automatically, or it can be triggered manually via workflow_dispatch.

<p align="center">
  <img src="../images/pipeline-update.svg" alt="Basin update via GitHub Actions: HPC to S3 to GitHub Actions to Icechunk"/>
</p>

#### YAML manifest format

```yaml
basin: jakobshavn
date: "2025-04"
mask_s3_path: "s3://us-west-2.opendata.source.coop/englacial/demogorgn/masks/jakobshavn.npy"
data_s3_path: "s3://us-west-2.opendata.source.coop/englacial/demogorgn/model-output/jakobshavn/2025-04/data.npy"
```

Commit the manifest to `updates/` on the `main` branch. The workflow detects changed YAML files and passes them to `basin_update.py`.

### Alternative design: direct from HPC

Scientists run the update script directly on HPC, pointing it at local mask and data files. The script uses **read-modify-write**: it reads the current bounding box from the store, overwrites only the pixels inside the basin mask in memory, then writes the bounding box back. This means NaN values outside the basin are never written to the store.

<p align="center">
  <img src="../images/pipeline-option2-update.svg" alt="Basin update direct from HPC to Source.Coop"/>
</p>

---

## Handling NaN values in overlapping bounding boxes

Model output contains valid data only within the basin mask. Pixels outside the mask but inside the bounding box are NaN. When neighbouring basins have overlapping bounding boxes, writing the full bbox can overwrite valid data from a previously updated basin with NaN.

<p align="center">
  <img src="../images/overlap-problem.svg" alt="Overlapping bounding boxes: the NaN problem"/>
</p>

Icechunk's versioning means the data is never truly lost (the previous snapshot is preserved and accessible via `readonly_session(snapshot_id=...)`), but the latest version would be incorrect. A **read-modify-write** with the basin mask avoids this: only pixels where the mask is `True` are updated, so NaN values outside the basin are never written to the store.

!!! note
    The current GitHub Actions pipeline writes the full bounding box and is susceptible to this issue. The alternative design's read-modify-write approach handles this correctly by skipping NaN pixels outside the mask.

---

## Comparison

| | Current (GitHub Actions) | Alternative (Direct from HPC) |
|---|---|---|
| **Automation** | Fully automated via CI/CD | Manual script invocation |
| **Source data** | Must be uploaded to S3 first | Local files on HPC |
| **Audit trail** | Git commits + workflow logs | Icechunk snapshots only |
| **Basin precision** | Writes full bounding box (including NaN) | Read-modify-write skips NaN outside mask |
| **Overlapping basins** | NaN can overwrite valid data from neighbours | Safe, only masked pixels changed |
| **Network** | S3 &rarr; runner &rarr; S3 (double transfer) | HPC &rarr; S3 (single transfer, but bbox read + write) |
