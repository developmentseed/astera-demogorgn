# Data Upload to source.coop

This page walks through uploading data to the [Source.Coop](https://source.coop/englacial/demogorgn) repository using the AWS CLI, starting from scratch.

## Background

Source.coop is a utility for hosting open datasets that provides a public data catalog and standardized access. The data itself physically lives on an Amazon Web Services S3 bucket; the following upload instructions explain how to get started with the AWS CLI in order to upload data to the public repository.
---

## Install the AWS CLI

=== "macOS"

    ```bash
    brew install awscli
    ```

=== "Linux"

    ```bash
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip awscliv2.zip
    sudo ./aws/install
    ```

=== "Windows"

    Download and run the [official MSI installer](https://awscli.amazonaws.com/AWSCLIV2.msi).

Verify the installation:

```bash
aws --version
```

---

## Get credentials from Source.Coop

Source.Coop provides S3-compatible credentials scoped to a specific repository. To retrieve them:

1. Go to the repository page: [source.coop/englacial/demogorgn](https://source.coop/englacial/demogorgn)
2. Click the **Lock Icon** to the right of "Product Contents" (you must be logged in and have write access)
3. Click **View Credentials**
4. Click **Environment Variables**
5. Copy and paste the content under "For terminal/shell usage" into your CLI

---

## Verify access

List the repository prefix to confirm your credentials work:

```bash
aws s3 ls s3://us-west-2.opendata.source.coop/englacial/demogorgn/ 
```

You should see the sample files (`antarctica_fast_flow_mask_50m.nc`, `realizations.nc`, etc.). A `NoCredentialsError` or `AccessDenied` response means the credentials or profile configuration need rechecking.

---

## Upload files

Use `aws s3 cp` for single files or `aws s3 sync` for directories.

### Single file

```bash
aws s3 cp realizations.nc \
    s3://us-west-2.opendata.source.coop/englacial/demogorgn/realizations.nc
```

### Directory

```bash
aws s3 sync ./model-output/ \
    s3://us-west-2.opendata.source.coop/englacial/demogorgn/model-output/
```

### Proposed path layout

| File type | S3 path |
|-----------|---------|
| Methodology mask | `s3://us-west-2.opendata.source.coop/englacial/demogorgn/masks/antarctica_fast_flow_mask_50m.nc` |
| Model output | `s3://us-west-2.opendata.source.coop/englacial/demogorgn/data/realizations.nc` |
| Updates | `s3://us-west-2.opendata.source.coop/englacial/demogorgn/data/updates/update1.nc` |
| Icechunk store | `s3://us-west-2.opendata.source.coop/englacial/demogorgn/realizations.icechunk` |

