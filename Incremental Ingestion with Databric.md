### Incremental Ingestion with Databricks

**Problem**: Source system pumps out new transactions and some updates daily, with millions of rows. We need to ingest every 45 minutes.

**Goal**: Build a fast, reliable incremental load using Databricks tools, with checks to ensure we capture everything.

#### Game Plan
- **Spot Changes**:
  - Use Auto Loader with CDC to grab real-time inserts/updates/deletes from source logs (e.g., via Debezium).
  - Fallback: Query by `last_modified_timestamp` or `transaction_id`, tracked in a Delta table.
  - Blend CDC with 45-minute micro-batches via Delta Live Tables (DLT).

- **Pipeline** (Bronze > Silver > Gold):
  - **Bronze**: Auto Loader pulls raw JSON into Delta Lake, partitioned by date (year/month/day), stored in Unity Catalog.
  - **Silver**: DLT flattens JSON, merges updates (using `MERGE` on keys like `ClientId`), and dedupes. Partition by `client_id` or date.
  - **Gold**: DLT builds aggregated views, updating only changed data for speed.

- **Stay Fast**:
  - Use Adaptive Query Execution for efficient processing.
  - Z-order index Delta tables for quick queries.
  - Save as Parquet with Snappy compression.
  - Auto-scale clusters to finish in <45 minutes.
  - Keep jobs rerun-safe with DLT checkpoints.

- **Stay Accurate**:
  - Delta Lake’s ACID ensures consistency.
  - Sort by timestamp to handle out-of-order data.
  - Log runs and counts in a Delta table.

#### Data Checks
- **Before**: Verify watermark vs. source; check expected row counts.
- **During**: Enforce schema; catch duplicates; flag nulls or >1% count mismatches with DLT expectations.
- **After**: Compare aggregates (e.g., totals) with source; ensure valid data; monitor lag; audit for missed runs; rollback with Delta time-travel if needed.

#### Execution
- **Schedule**: Run every 45 minutes via Databricks Workflows with retries.
- **Monitor**: Use SQL Alerts and Unity Catalog for issues and lineage.
- **Govern**: Secure schemas and data with Unity Catalog.

This Databricks setup—using Auto Loader, DLT, and Unity Catalog—keeps ingestion quick, reliable, and bulletproof with solid checks.