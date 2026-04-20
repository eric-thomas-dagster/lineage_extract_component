# Catalog Lineage Sync Component

A Dagster component that automatically exports your asset lineage graph to external data catalogs. Drop a `defs.yaml` into your project — no assets, no jobs, no manual triggers.

## How It Works

The component adds a **sensor** that:

1. Introspects the live Dagster asset graph at runtime (`context.repository_def.asset_graph`)
2. Builds a lineage payload (nodes, edges, metadata, freshness policies)
3. Transforms it to the target catalog's expected API format
4. Hashes the graph structure — **only pushes when lineage has actually changed**
5. POSTs to the catalog API (or writes to a local file in demo mode)

## Supported Targets

| Target | API Format | Reference |
|---|---|---|
| `alation` | `{ "dataflow_objects": [...], "paths": [...] }` | [Alation Bulk Lineage API](https://developer.alation.com/dev/reference/postbulklineage) |
| `collibra` | `[{ "source": {...}, "target": {...}, "relationType": {...} }]` | [Collibra I/O Relations API](https://developer.collibra.com/rest/2.0/input-output-relations) |
| `datahub` | MCE proposals with `datasetProperties` + `upstreamLineage` aspects | [DataHub Lineage API](https://datahubproject.io/docs/api/tutorials/lineage) |
| `openlineage` | `RunEvent` with job facets and I/O datasets | [OpenLineage Spec](https://openlineage.io/docs/spec/run-event) |
| `webhook` | Raw internal format (passthrough) | Any HTTP endpoint |
| `file` | Raw JSON to local path | Local filesystem |

Each target has a dedicated payload transformer that converts the internal graph format to the catalog's expected structure.

## Quick Start

1. Copy `component.py` into your project's `components/` directory
2. Create a `defs.yaml` in your `defs/` directory:

```yaml
type: my_project.components.catalog_lineage_sync.CatalogLineageSync

attributes:
  demo_mode: true
  catalog_target: alation
  catalog_url: "https://alation.internal/integration/v2"
  api_token_env: "ALATION_API_TOKEN"
```

3. Run `dg check defs` — a sensor named `catalog_lineage_sync` will appear
4. Enable the sensor in the Dagster UI — lineage syncs automatically

## Configuration

| Field | Default | Description |
|---|---|---|
| `demo_mode` | `true` | When true, logs the payload and writes to local file instead of pushing |
| `scope` | `code_location` | `"code_location"` for this location only, `"deployment"` for all locations via Dagster+ GraphQL |
| `catalog_target` | `alation` | Target catalog: `alation`, `collibra`, `datahub`, `openlineage`, `webhook`, `file` |
| `catalog_url` | | Base URL for the catalog API |
| `api_token_env` | `CATALOG_API_TOKEN` | Env var name containing the catalog API token |
| `dagster_plus_token_env` | `DAGSTER_PLUS_TOKEN` | Env var name for Dagster+ GraphQL token (deployment scope only) |
| `demo_export_path` | `data/exports/catalog_lineage.json` | Local file path for demo mode output |
| `sensor_interval_seconds` | `3600` | How often the sensor ticks (seconds) |
| `sensor_name` | `catalog_lineage_sync` | Name of the sensor in Dagster UI |
| `sensor_default_status` | `STOPPED` | `STOPPED` or `RUNNING` — whether sensor starts enabled |

## Scope: Code Location vs Deployment

### `scope: code_location` (default)

Uses `context.repository_def.asset_graph` — fast, no external calls, but only exports assets from the current code location.

### `scope: deployment`

Queries the Dagster+ GraphQL API for the full asset graph across **all code locations** in the deployment. Requires:
- Dagster+ (Serverless or Hybrid)
- `DAGSTER_CLOUD_URL` — auto-set by Dagster+
- `DAGSTER_CLOUD_DEPLOYMENT_NAME` — auto-set by Dagster+
- `DAGSTER_PLUS_TOKEN` — a user token you create in the Dagster+ UI (must be set as an env var)

Falls back to code location scope if the GraphQL API is unavailable.

## Change Detection

The sensor hashes the graph structure (nodes + edges) on every tick. If the hash matches the previous tick's cursor, it skips the sync entirely — no API calls, no file writes. Lineage only changes when:
- Code server reloads (new/changed assets or components)
- A `StateBackedComponent` refreshes (e.g. dbt manifest recompile)

This means the sensor is safe to run frequently (e.g. every 5 minutes) without spamming the catalog API.

## Examples

See [example.yaml](example.yaml) for configurations for each supported target.
