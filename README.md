# Catalog Lineage Sync Component

A Dagster component that automatically exports your asset lineage graph to external data catalogs. Drop a `defs.yaml` into your project — no assets, no jobs, no manual triggers.

## How It Works

The component adds a **sensor** that:

1. Introspects the live Dagster asset graph at runtime (`context.repository_def.asset_graph`)
2. Builds a lineage payload (nodes, edges, metadata, freshness policies)
3. Enriches with source system identity (platform, org, deployment, UI links)
4. Transforms to the target catalog's expected API format and auth
5. Hashes the graph structure — **only pushes when lineage has actually changed**
6. POSTs to the catalog API (or writes to a local file in demo mode)

## Supported Targets

| Target | Endpoint | Auth | Payload Format |
|---|---|---|---|
| `alation` | `POST /integration/v2/lineage/` | `TOKEN` header | `{ "dataflow_objects": [...], "paths": [...] }` with 3-segment lineage chains |
| `collibra` | `POST /rest/2.0/import/json-job` | `Authorization: Bearer` | `{ "assets": [...], "relations": [...] }` with community/domain hierarchy |
| `datahub` | `POST /aspects?action=ingestProposal` | `Authorization: Bearer` + `X-RestLi-Protocol-Version: 2.0.0` | Per-aspect proposals with `changeType: UPSERT`, aspect value as JSON string |
| `openlineage` | `POST /api/v1/lineage` | `Authorization: Bearer` (optional) | `RunEvent` with `_producer`, `_schemaURL`, UUID `runId`, versioned facets |
| `webhook` | `POST {catalog_url}` | `Authorization: Bearer` (optional) | Raw internal format (passthrough) |
| `file` | N/A | N/A | Raw JSON to local path |

## Quick Start

1. Copy `component.py` into your project's `components/` directory
2. Create a `defs.yaml` in your `defs/` directory:

```yaml
type: my_project.components.catalog_lineage_sync.CatalogLineageSync

attributes:
  demo_mode: true
  catalog_target: alation
  catalog_url: "https://alation.internal"
  api_token_env: "ALATION_API_TOKEN"
  organization: "My Company"
```

3. Run `dg check defs` — a sensor named `catalog_lineage_sync` will appear
4. Enable the sensor in the Dagster UI — lineage syncs automatically

## Configuration

### Core Settings

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

### Source System Identification

These fields tell the catalog **who is sending** the lineage. Catalogs use this to register the data source, create drill-down links, and distinguish lineage from multiple platforms.

| Field | Default | Auto-filled from | Description |
|---|---|---|---|
| `platform_name` | `dagster` | — | Platform identifier registered in the catalog |
| `platform_display_name` | `Dagster` | — | Human-readable platform name |
| `organization` | `""` | — | Your company/org name (e.g. `"Westpac"`) |
| `deployment_name` | `""` | `DAGSTER_CLOUD_DEPLOYMENT_NAME` | Deployment name (e.g. `"prod"`, `"staging"`) |
| `dagster_ui_url` | `""` | `DAGSTER_CLOUD_URL` | Base URL of the Dagster UI for drill-down links |
| `code_location_name` | `""` | `context.repository_name` | Code location name |

How each target uses the source system identity:

| Target | Registration | Drill-down links |
|---|---|---|
| **Alation** | `external_id: api/dagster/prod/...`, `content` JSON includes org/deployment/code_location | `url` field links to `{dagster_ui_url}/assets/{key}` |
| **Collibra** | `community: "{org} Data Platform"`, `domain: "{group} ({deployment})"` | `Dagster UI URL` attribute per asset |
| **DataHub** | URN: `urn:li:dataset:(urn:li:dataPlatform:{platform},{key},{env})` | `externalUrl` links to Dagster UI, `customProperties` include deployment + code location |
| **OpenLineage** | `_producer: "{dagster_ui_url}"`, `namespace: "dagster://{org}/{deployment}"` | Producer URL identifies the exact deployment |

## Scope: Code Location vs Deployment

### `scope: code_location` (default)

Uses `context.repository_def.asset_graph` — fast, no external calls, but only exports assets from the current code location.

### `scope: deployment`

Queries the Dagster+ GraphQL API for the full asset graph across **all code locations** in the deployment. The GraphQL URL is auto-derived from `DAGSTER_CLOUD_URL` + `DAGSTER_CLOUD_DEPLOYMENT_NAME`.

Requires:
- Dagster+ (Serverless or Hybrid)
- `DAGSTER_CLOUD_URL` — auto-set by Dagster+
- `DAGSTER_CLOUD_DEPLOYMENT_NAME` — auto-set by Dagster+
- A user token set via the `dagster_plus_token_env` env var — **not** auto-injected into user code by Dagster+; you must create a user token in the Dagster+ UI and set it as an environment variable

Falls back to code location scope if the GraphQL API is unavailable.

## Catalog Registration

Some catalogs require (or benefit from) registering a data source before sending lineage. The component handles this per target:

| Target | Pre-registration needed? | What the component does |
|---|---|---|
| **Alation** | **Optional.** Without `alation_datasource_id`, assets are `external` objects (no registration needed). With it, assets appear as tables under a registered Alation data source. | Set `alation_datasource_id: 5` in YAML to map assets as `5.dagster.asset_name`. Useful if you want Dagster assets to link to Snowflake/Postgres tables in Alation. |
| **Collibra** | **No.** The import API auto-creates communities, domains, and assets. | Communities and domains are derived from `organization` + `group_name`. |
| **DataHub** | **No, but recommended.** Platforms auto-exist on first URN. | When `datahub_register_platform: true` (default), prepends a `dataPlatformInfo` proposal that registers "dagster" with a display name so it shows up nicely in the UI. |
| **OpenLineage** | **No.** Event-based — the `namespace` and `_producer` identify the source implicitly. | No separate registration step. |

### Alation Data Source Mapping

By default, Dagster assets are registered as `external` objects in Alation with keys like `api/dagster/prod/marts/mart_account_health`. This works without any pre-registration.

If you want Dagster assets to appear as **tables under an existing Alation data source** (e.g. to link them to your Snowflake catalog), set `alation_datasource_id`:

```yaml
attributes:
  catalog_target: alation
  alation_datasource_id: 5  # ID of the registered Snowflake data source in Alation
```

This changes the key format to `5.dagster.marts/mart_account_health` (table otype) so assets appear under that data source in the Alation UI.

### DataHub Platform Registration

DataHub automatically creates platform entries when you ingest URNs with `urn:li:dataPlatform:dagster`. However, the platform won't have a display name or icon until you register it. With `datahub_register_platform: true` (default), the component sends a `dataPlatformInfo` aspect on every sync:

```json
{
  "name": "dagster",
  "displayName": "Dagster",
  "type": "OTHERS",
  "datasetNameDelimiter": "/"
}
```

Set `datahub_register_platform: false` if you've already registered the platform or don't want the overhead.

## Authentication Per Target

| Target | Header | Format | Notes |
|---|---|---|---|
| **Alation** | `TOKEN` | Raw token value | Not `Authorization: Bearer` — Alation uses a custom header |
| **Collibra** | `Authorization` | `Bearer {token}` | Standard OAuth2 bearer |
| **DataHub** | `Authorization` + `X-RestLi-Protocol-Version` | `Bearer {token}` + `2.0.0` | DataHub requires the Rest.li protocol version header |
| **OpenLineage** | `Authorization` | `Bearer {token}` | Optional — some backends (Marquez) don't require auth |
| **Webhook** | `Authorization` | `Bearer {token}` | Only if `api_token_env` is set |

## Change Detection

The sensor hashes the graph structure (nodes + edges) on every tick. If the hash matches the previous tick's cursor, it skips the sync entirely — no API calls, no file writes. Lineage only changes when:
- Code server reloads (new/changed assets or components)
- A `StateBackedComponent` refreshes (e.g. dbt manifest recompile)

This means the sensor is safe to run frequently (e.g. every 5 minutes) without spamming the catalog API.

## Error Handling

- **Missing token**: raises `RuntimeError` → sensor tick fails → visible in Dagster UI
- **HTTP error** (4xx/5xx): `raise_for_status()` → sensor tick fails → automatic retry on next tick
- **Cursor not updated on failure**: the hash only advances after a successful push, so the next tick retries the same payload

## Examples

See [example.yaml](example.yaml) for configurations for each supported target.
