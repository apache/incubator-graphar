# Kuzu — GraphAr Extension

**API:** C++
**Status:** read-only ingestion, first-cut
**Repository branch:** [https://github.com/gary-cloud/kuzu/tree/graphar-extension](https://github.com/gary-cloud/kuzu/tree/graphar-extension)

---

## TL;DR

A minimal GraphAr extension for Kuzu implemented in C++ (relying on GraphAr + Arrow C++ SDKs). It parses GraphAr YAML schema and parallel-reads data into Kuzu’s internal node/edge representations so GraphAr datasets can be consumed directly via `LOAD` / `COPY` workflows.

---

## Context

GraphAr ([https://graphar.apache.org/](https://graphar.apache.org/)) is a property-graph interchange/storage format that exposes YAML metadata and stores columnar data using Arrow / Parquet and similar formats. The extension demonstrates a pragmatic approach to supporting GraphAr in Kuzu by translating GraphAr schema/columns into Kuzu types and ingesting record batches in parallel using Arrow readers.

---

## What this extension implements

* GraphAr YAML schema parsing (node/edge tables, property columns and types).
* Mapping of GraphAr/Arrow column types to Kuzu primitive types.
* Parallel chunk/record-batch readers that convert GraphAr data → Kuzu insert streams.
* A non-invasive extension layer that translates GraphAr into Kuzu types without changing Kuzu core where possible.

---

## Usage examples

```sql
-- read and return rows (demo)
LOAD FROM ".../ldbc_sample.graph.yml" (file_format="graphar", table_name="person") RETURN *;

-- create a target table in Kuzu
CREATE NODE TABLE Person(id INT64, firstName STRING, lastName STRING, gender STRING, PRIMARY KEY (id));

-- copy GraphAr dataset into an existing Kuzu table
COPY Person FROM ".../ldbc_sample.graph.yml" (file_format="graphar", table_name="person");
```

Parameters:

* `file_format="graphar"`: instructs Kuzu to use this GraphAr reader extension.
* `table_name`: must match the node/edge table name declared in the GraphAr YAML.

---

## Design choices

* **Non-invasive architecture:** the extension lives as a translation layer to avoid touching Kuzu core.
* **Reuse of GraphAr + Arrow SDKs:** avoids reimplementing low-level IO and schema logic.
* **Parallel RecordBatch conversion:** increases ingestion throughput in many workloads.
* **Scope limitation:** read-only ingestion (LOAD/COPY). Export/write path not implemented in this branch.

---

## Forward-looking improvements

1. **Export/write path**: enable Kuzu → GraphAr export with schema & metadata generation.
2. **Comprehensive tests & benchmarks**: unit/integration tests, stress tests, and performance baselines.

---

## Expected outcome

* Users can `LOAD` or `COPY` GraphAr datasets into Kuzu without a manual pre-conversion step.
