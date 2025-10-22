# Kuzu — GraphAr Extension

**API:** C++
**Status:** first-cut, pragmatic implementation (ingest, export, metadata inspection)
**Repository branch:** [https://github.com/gary-cloud/kuzu/tree/graphar-extension](https://github.com/gary-cloud/kuzu/tree/graphar-extension)

---

## TL;DR

A pragmatic GraphAr extension for Kuzu implemented in C++ and built on GraphAr + Arrow C++ SDKs. The extension provides a translation layer that enables GraphAr datasets to be consumed and produced by Kuzu workflows. Supported operations include reading GraphAr datasets into Kuzu (`LOAD` / `COPY FROM`), inspecting GraphAr YAML metadata from within Kuzu, and exporting Kuzu query results into GraphAr-layout files (`COPY ... TO`). These capabilities are designed as pragmatic, cohesive features for integration and engineering verification.

---

## Context

GraphAr is a property-graph interchange format that expresses dataset metadata in YAML and stores columnar payloads via Arrow/Parquet. This extension implements a translation-and-IO layer that parses GraphAr YAML, maps Arrow/GraphAr schema to Kuzu types, and performs parallel record-batch conversions so Kuzu can ingest and emit GraphAr-layout data without a separate conversion step.

---

## Implemented features

The extension implements the following capabilities as a unified set:

* GraphAr YAML schema parsing (node/edge tables, property columns, Arrow/Parquet descriptors).
* Conservative mapping of GraphAr/Arrow column types to Kuzu primitive types.
* Parallel RecordBatch readers that convert GraphAr data into Kuzu insert streams for `LOAD` / `COPY FROM` ingestion.
* `COPY <Table> FROM "<graph.yml>" (file_format="graphar", table_name="<name>")` — import GraphAr data into an existing Kuzu table.
* `COPY (<query>) TO "<out.graph.yml.graphar>" (table_name="<name>", target_dir="<dir>")` — export query results into GraphAr-layout files (YAML + Arrow/Parquet payloads).
* `CALL GRAPHAR_METADATA('<path>', [vertex_type := '<v>'], [edge_type := '<e>'])` — read and return GraphAr YAML metadata entries from within Kuzu.
* Non-invasive extension architecture: translation and IO logic implemented to minimize changes to Kuzu core.

These capabilities are intended to be used together: inspect metadata, ingest GraphAr data, query and validate in Kuzu, and export GraphAr-layout outputs for downstream consumers.

---

## Usage examples

```sql
-- Inspect graph metadata
CALL GRAPHAR_METADATA('/path/to/graph.graph.yml') RETURN *;

-- Create Kuzu target table
CREATE NODE TABLE Person(id INT64, firstName STRING, lastName STRING, gender STRING, PRIMARY KEY (id));

-- Ingest GraphAr dataset into Kuzu table
COPY Person FROM "/path/to/graph.graph.yml" (file_format="graphar", table_name="person");

-- Export query results into GraphAr layout
COPY (
  MATCH (p:Person)
  RETURN p.id AS id, p.firstName AS firstName, p.lastName AS lastName, p.gender AS gender
  ORDER BY p.internal_id ASC
) TO "/path/to/out.graph.yml.graphar" (table_name="person", target_dir="/some/output/dir/");
```

Notes on parameters:

* `file_format="graphar"` selects this extension as the reader for `COPY FROM`.
* `table_name` must match the GraphAr node/edge table name in the YAML.
* `target_dir` (for `COPY TO`) is the directory where binary Arrow/Parquet payloads are written; the `TO` path serves as the logical YAML output name.
* For edge export, queries typically return `from`/`to` (often using `internal_id`) to map edge endpoints.

---

## Design principles

* **Translation layer (non-invasive):** keep modifications to Kuzu core minimal; implement mapping and IO in the extension.
* **Reuse existing SDKs:** leverage GraphAr and Arrow C++ SDKs for I/O and schema handling.
* **Parallel processing:** employ parallel RecordBatch conversion to improve ingestion throughput where applicable.
* **Conservative scope:** implement robust support for common Arrow types and GraphAr patterns before expanding to edge cases.

---

## Operational guidance

* Use `CALL GRAPHAR_METADATA` to discover table names and column layouts before creating target tables in Kuzu.
* Ensure target Kuzu table schemas align with GraphAr/Arrow column types; apply explicit conversions in SQL or transform data when necessary.
* For deterministic exports of edges or id-sensitive payloads, include `internal_id` in the query projection and order by that column.
* Validate exported YAML and payload files with your downstream GraphAr tooling when integrating exported artifacts.
* Exports stream query results to the target directory; design downstream validation to tolerate or detect partial outputs when a query or write fails.

---

## Example end-to-end workflow

1. Load the extension: `LOAD EXTENSION "<path>/libgraphar.kuzu_extension";`.
2. Inspect input metadata: `CALL GRAPHAR_METADATA('<in.graph.yml>')` and identify table names/columns.
3. Create Kuzu table(s) matching expected types and primary-key semantics.
4. Ingest data: `COPY <Table> FROM '<in.graph.yml>' (file_format='graphar', table_name='<name>');`.
5. Validate data in Kuzu with representative queries.
6. Export data when needed: `COPY (<query>) TO '<out.graph.yml.graphar>' (table_name='<name>', target_dir='<outdir>');`.
7. Optionally inspect exported YAML files and payloads with GraphAr tooling.

---

## Limitations and planned improvements

Current limitations are acknowledged and prioritized for engineering progress. Planned improvements include:

* Enhanced export metadata generation (more complete YAML with field descriptions and validation metadata).
* Expanded Arrow/GraphAr type coverage, including nested and complex types.
* Automated round-trip tests that verify fidelity for representative datasets.
* Configurable writer backend and tunable parallelism for Arrow writers.
* Improved diagnostics and error messages for developer workflows.

---

## Files of interest

* `src/extension/` — core implementation for parsing, mapping, and IO.
* `tests/` — example programs and verification utilities demonstrating metadata inspection, ingestion, query, and export workflows.

---

## Notes

This branch implements a pragmatic, coherent set of GraphAr-related capabilities for Kuzu. It is suitable for experimentation, integration testing, and engineering verification. Users should validate type mappings and metadata semantics against their target GraphAr tooling when producing or consuming exported artifacts.
