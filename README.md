# FreshCart Data Engineering Pipelines

This repository contains an end-to-end Data Engineering assignment that demonstrates Git workflows and CI/CD, FreshCart ELT implementation using Snowflake, ML pipeline development with Mage, and real-time message streaming through RabbitMQ.

## Repository Structure

- `elt/` – Contains the FreshCart ELT implementation.
- `iris_project/` – Includes the Mage-based Iris classification pipeline.
- `stream/` – Holds the RabbitMQ dispatcher and driver components.
- `tests/` – Contains automated test cases.
- `.github/workflows/` – Defines the GitHub Actions CI workflows.

## FreshCart ELT Pipeline (Part B)

The Snowflake-based ELT workflow is managed through `elt.run_pipeline`. Before executing the pipeline, set up the required Snowflake configuration variables in the `.env` file using `.env.example` as a reference.

To execute the initial pipeline using simulated source data, run:

```powershell
python -m elt.run_pipeline --mode mock
```

If the Bronze layer has already been populated and you only want to execute the transformation steps again, use `--mode transform-only`. This avoids extracting and loading an additional batch of source data. It is also the default execution mode.

```powershell
python -m elt.run_pipeline --mode transform-only
```

You can also execute a specific transformation layer by providing the `--stage` argument. For example, to run only the Gold layer:

```powershell
python -m elt.run_pipeline --stage gold
```

Pincode API responses are generated using predefined deterministic mock data by default. To retrieve actual responses, enable the `--live-pincodes` option.

All SQL-based transformation scripts are maintained in `elt/sql/`. Pure Python business-rule tests can be executed using `python -m pytest`.

For live data extraction, the required service endpoints and authentication tokens must be configured in `.env`. The corresponding variable names and configuration details are provided in `.env.example`.

### Bronze File Ingestion and Tracking

File ingestion into the Bronze layer is managed through the additional migration script `elt/sql/01_ingestion_files.sql`.

Each ingested file is identified using a SHA-256 hash generated from the normalized source and its staged `source/date/filename` path. Additionally, a checksum of the compressed file content is maintained to ensure that modified files cannot accidentally reuse an existing identity.

The ingestion process follows a status-based tracking mechanism:

1. A `LOADING` status is recorded in the ingestion ledger before executing the PUT and COPY operations.
2. The status changes to `LOADED` only after COPY results have been validated and the corresponding Bronze source-file row counts have been confirmed.
3. If an ingestion attempt fails, its status is updated to `FAILED`, allowing the file to be processed again.

If the process terminates unexpectedly, an entry may remain in the `LOADING` state. In such cases, check the corresponding Bronze `SRC_FILE` row count and ensure that no loader process is still running before manually updating the ledger status.

For compatibility with Snowflake trial accounts, the ingestion ledger is implemented as a regular Snowflake table. Its `FILE_ID` column does not have an enforced uniqueness constraint, so the ingestion mechanism supports only single-writer execution.

A host-level lock is used to prevent multiple ingestion processes from running simultaneously on the same machine. However, scheduled executions and manually triggered loaders across different machines must also be coordinated to ensure that only one writer operates at a time.

Before proceeding with ingestion, the loader checks for duplicate entries in the ledger. It also rejects ambiguous legacy Bronze records that do not have corresponding ledger entries. Such records must be reconciled before attempting another load.

It is important to note that Snowflake PUT, COPY, and ledger DML operations do not execute as a single atomic transaction. Therefore, any failed or interrupted ingestion requires the documented status and row-count verification process before retrying.

## Assignment Overview

The repository is organized into five major assignment components:

- **Part A:** Git version control and CI/CD implementation.
- **Part B:** FreshCart ELT pipeline development using Snowflake.
- **Part C:** Machine Learning pipeline implementation with Mage.
- **Part D:** Food-order event streaming using RabbitMQ.
- **Part E:** Final project documentation, report preparation, and viva.