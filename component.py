"""Catalog lineage sync component.

Drop this component into your defs/ directory and lineage is
automatically exported to your data catalog on every sensor tick —
no assets, no jobs, no manual triggers.

Supported catalog targets:
- alation:      Alation Data Catalog REST API
- collibra:     Collibra Data Intelligence REST API
- datahub:      DataHub (via REST or Kafka emitter)
- openlineage:  OpenLineage-compatible endpoint (Marquez, Atlan, etc.)
- webhook:      Generic HTTP POST to any URL
- file:         Write JSON to a local/remote path (for S3, ADLS, etc.)

In demo mode: logs the payload and writes to a local JSON file
regardless of the configured target.
"""

import json
import hashlib
import time
from pathlib import Path
from typing import Optional

import dagster as dg
from pydantic import BaseModel


def _build_lineage_payload(repo_def) -> dict:
    """Walk the live asset graph and build a catalog-ready payload."""
    asset_graph = repo_def.asset_graph
    all_keys = list(asset_graph.toposorted_asset_keys)

    nodes = []
    edges = []
    group_summary: dict[str, list[str]] = {}

    for key in all_keys:
        node = asset_graph.get(key)
        key_str = key.to_user_string()

        raw_metadata = node.metadata or {}
        safe_metadata = {}
        for k, v in raw_metadata.items():
            if k.startswith(("dagster_dbt/", "dagster/")):
                continue
            try:
                json.dumps(v)
                safe_metadata[k] = v
            except Exception:
                safe_metadata[k] = str(v)

        fp = node.freshness_policy_or_from_metadata
        nodes.append({
            "asset_key": key.path,
            "asset_key_string": key_str,
            "group": node.group_name,
            "kinds": sorted(node.kinds) if node.kinds else [],
            "description": (node.description or "")[:500],
            "metadata": safe_metadata,
            "freshness_policy": str(fp) if fp else None,
            "parent_count": len(node.parent_keys),
            "child_count": len(node.child_keys),
        })

        grp = node.group_name or "ungrouped"
        group_summary.setdefault(grp, []).append(key_str)

        for parent_key in node.parent_keys:
            edges.append({
                "upstream": parent_key.to_user_string(),
                "downstream": key_str,
            })

    return {
        "sync_metadata": {
            "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "dagster_asset_graph",
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "total_groups": len(group_summary),
            "assets_with_freshness_policy": sum(1 for n in nodes if n["freshness_policy"]),
        },
        "nodes": nodes,
        "edges": edges,
        "group_summary": {
            grp: {"count": len(assets), "assets": assets}
            for grp, assets in sorted(group_summary.items())
        },
    }


def _hash_payload(payload: dict) -> str:
    """Hash the structural content (nodes + edges) for change detection."""
    structural = json.dumps(
        {"nodes": payload["nodes"], "edges": payload["edges"]},
        sort_keys=True,
    )
    return hashlib.sha256(structural.encode()).hexdigest()[:16]


# ═════════════════════════════════════════════════════════════════════
# Payload transformers — convert internal graph to catalog-specific format
# ═════════════════════════════════════════════════════════════════════

def _transform_alation(payload: dict) -> dict:
    """Transform to Alation Lineage API format.

    Endpoint: POST /integration/v2/lineage/
    Auth header: TOKEN (not Bearer)

    Alation lineage uses "paths" — ordered chains of objects where each
    path is a list of segments. Each segment contains objects typed by
    "otype" (table, dataflow, external, etc.) with a unique "key".

    Ref: https://developer.alation.com/dev/docs/lineage-overview
    """
    ss = payload.get("source_system", {})
    platform = ss.get("platform", "dagster")
    deployment = ss.get("deployment", "")
    ui_url = ss.get("dagster_ui_url", "")
    prefix = f"api/{platform}/{deployment}" if deployment else f"api/{platform}"

    # Register each asset as an external dataflow object
    dataflow_objects = []
    for node in payload["nodes"]:
        asset_path = "/".join(node["asset_key"])
        dataflow_objects.append({
            "external_id": f"{prefix}/{asset_path}",
            "title": node["asset_key_string"],
            "description": node.get("description", ""),
            "url": f"{ui_url}/assets/{asset_path}" if ui_url else "",
            "content": json.dumps({
                "source_platform": platform,
                "deployment": deployment,
                "organization": ss.get("organization", ""),
                "code_location": node.get("code_location", ss.get("code_location", "")),
                "group": node["group"],
                "kinds": node["kinds"],
                "metadata": node["metadata"],
                "freshness_policy": node["freshness_policy"],
            }),
        })

    # Build lineage paths: each edge becomes a 3-segment path
    paths = []
    for edge in payload["edges"]:
        paths.append([
            [{"otype": "external", "key": f"{prefix}/{edge['upstream']}"}],
            [{"otype": "dataflow", "key": f"{prefix}/{edge['downstream']}"}],
            [{"otype": "external", "key": f"{prefix}/{edge['downstream']}"}],
        ])

    return {"dataflow_objects": dataflow_objects, "paths": paths}


def _transform_collibra(payload: dict) -> dict:
    """Transform to Collibra Import API format.

    Endpoint: POST /rest/2.0/import/json-job
    Auth: Basic auth or Bearer token via Authorization header

    Collibra's import API accepts assets and relations in a single
    payload. Assets are identified by name + domain. Relations connect
    assets via typed relationships. The community/domain hierarchy
    identifies the source system.
    """
    ss = payload.get("source_system", {})
    org = ss.get("organization", "") or ss.get("platform_display_name", "Dagster")
    deployment = ss.get("deployment", "")
    community_name = f"{org} Data Platform" if org else "Data Platform"

    # Build asset entries
    assets = []
    for node in payload["nodes"]:
        domain_name = node.get("group") or "Default"
        if deployment:
            domain_name = f"{domain_name} ({deployment})"

        assets.append({
            "identifier": {
                "name": node["asset_key_string"],
                "domain": {
                    "name": domain_name,
                    "community": {"name": community_name},
                },
            },
            "resourceType": "Asset",
            "type": {"name": "Data Asset"},
            "displayName": node["asset_key_string"],
            "attributes": {
                "Description": [{"value": node.get("description", "")}],
                "Source Platform": [{"value": ss.get("platform_display_name", "Dagster")}],
                "Deployment": [{"value": deployment}],
                "Code Location": [{"value": node.get("code_location", ss.get("code_location", ""))}],
                "Dagster Group": [{"value": node.get("group", "")}],
                "Dagster Kinds": [{"value": ",".join(node.get("kinds", []))}],
                **({"Dagster UI URL": [{"value": f"{ss.get('dagster_ui_url', '')}/assets/{'/'.join(node['asset_key'])}"}]}
                   if ss.get("dagster_ui_url") else {}),
                **({"Freshness Policy": [{"value": node["freshness_policy"]}]}
                   if node.get("freshness_policy") else {}),
            },
        })

    # Build relation entries
    relations = []
    for edge in payload["edges"]:
        relations.append({
            "source": {
                "name": edge["upstream"],
                "domain": {"name": "Data Platform"},
            },
            "target": {
                "name": edge["downstream"],
                "domain": {"name": "Data Platform"},
            },
            "type": {"name": "Data Flow"},
        })

    return {"assets": assets, "relations": relations}


def _transform_datahub(payload: dict) -> list[dict]:
    """Transform to DataHub Rest.li ingestProposal format.

    Endpoint: POST /aspects?action=ingestProposal
    Auth: Authorization: Bearer <personal_access_token>
    Required header: X-RestLi-Protocol-Version: 2.0.0

    Ref: https://docs.datahub.com/docs/api/restli/restli-overview
    """
    ss = payload.get("source_system", {})
    platform = ss.get("platform", "dagster")
    deployment = ss.get("deployment", "")
    env = "PROD" if deployment in ("prod", "production", "") else "DEV"
    ui_url = ss.get("dagster_ui_url", "")

    parent_map: dict[str, list[str]] = {}
    for edge in payload["edges"]:
        parent_map.setdefault(edge["downstream"], []).append(edge["upstream"])

    proposals = []
    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{key_str},{env})"

        # Dataset properties aspect
        custom_props = {
            "dagster_group": node["group"] or "",
            "dagster_kinds": ",".join(node["kinds"]),
            "dagster_deployment": deployment,
            "dagster_code_location": node.get("code_location", ss.get("code_location", "")),
            **({"dagster_ui_url": f"{ui_url}/assets/{'/'.join(node['asset_key'])}"} if ui_url else {}),
            **({"dagster_freshness_policy": node["freshness_policy"]} if node["freshness_policy"] else {}),
            **{k: str(v) for k, v in node.get("metadata", {}).items()},
        }
        properties_aspect = {
            "name": key_str,
            "description": node.get("description", ""),
            "externalUrl": f"{ui_url}/assets/{'/'.join(node['asset_key'])}" if ui_url else "",
            "customProperties": custom_props,
        }
        proposals.append({
            "proposal": {
                "entityUrn": urn,
                "entityType": "dataset",
                "aspectName": "datasetProperties",
                "changeType": "UPSERT",
                "aspect": {
                    "value": json.dumps(properties_aspect),
                    "contentType": "application/json",
                },
            }
        })

        # Upstream lineage aspect
        parents = parent_map.get(key_str, [])
        if parents:
            lineage_aspect = {
                "upstreams": [
                    {
                        "dataset": f"urn:li:dataset:(urn:li:dataPlatform:{platform},{p},{env})",
                        "type": "TRANSFORMED",
                    }
                    for p in parents
                ],
            }
            proposals.append({
                "proposal": {
                    "entityUrn": urn,
                    "entityType": "dataset",
                    "aspectName": "upstreamLineage",
                    "changeType": "UPSERT",
                    "aspect": {
                        "value": json.dumps(lineage_aspect),
                        "contentType": "application/json",
                    },
                }
            })

    return proposals


def _transform_openlineage(payload: dict) -> dict:
    """Transform to OpenLineage RunEvent format.

    Endpoint: POST /api/v1/lineage
    Auth: Authorization: Bearer <token> (optional, depends on backend)

    OpenLineage requires _producer and _schemaURL on the event.
    The namespace identifies the source system (dagster deployment).

    Ref: https://openlineage.io/docs/spec/object-model
    """
    import uuid

    ss = payload.get("source_system", {})
    producer = ss.get("dagster_ui_url") or "https://github.com/dagster-io/dagster"
    namespace = f"dagster://{ss.get('deployment', 'local')}"
    if ss.get("organization"):
        namespace = f"dagster://{ss['organization']}/{ss.get('deployment', 'local')}"

    # Build input datasets from all assets
    input_datasets = []
    for node in payload["nodes"]:
        facets = {
            "dagster_metadata": {
                "_producer": producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DagsterMetadataFacet.json",
                "group": node["group"],
                "kinds": node["kinds"],
                "freshness_policy": node["freshness_policy"],
            },
        }
        # Add schema facet if metadata has fields
        if node.get("metadata"):
            facets["schema"] = {
                "_producer": producer,
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json",
                "fields": [
                    {"name": k, "type": str(type(v).__name__)}
                    for k, v in node["metadata"].items()
                ],
            }

        input_datasets.append({
            "namespace": namespace,
            "name": node["asset_key_string"],
            "facets": facets,
            "inputFacets": {},
        })

    return {
        "_producer": producer,
        "_schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "eventType": "COMPLETE",
        "eventTime": payload["sync_metadata"]["synced_at"],
        "job": {
            "namespace": namespace,
            "name": "dagster_lineage_sync",
            "facets": {},
        },
        "inputs": input_datasets,
        "outputs": [],
        "run": {
            "runId": str(uuid.uuid4()),
            "facets": {
                "dagster_lineage": {
                    "_producer": producer,
                    "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DagsterLineageFacet.json",
                    "edges": payload["edges"],
                    "total_nodes": payload["sync_metadata"]["total_nodes"],
                    "total_edges": payload["sync_metadata"]["total_edges"],
                },
            },
        },
    }


def _transform_passthrough(payload: dict) -> dict:
    """No transformation — send the raw internal format."""
    return payload


_TRANSFORMERS = {
    "alation": _transform_alation,
    "collibra": _transform_collibra,
    "datahub": _transform_datahub,
    "openlineage": _transform_openlineage,
    "webhook": _transform_passthrough,
    "file": _transform_passthrough,
}


# ═════════════════════════════════════════════════════════════════════
# Catalog push functions — one per target type, with correct auth
# ═════════════════════════════════════════════════════════════════════

def _get_token(token_env: str) -> str:
    """Get API token from environment, raising if missing."""
    import os
    token = os.environ.get(token_env)
    if not token:
        raise RuntimeError(f"Missing {token_env} environment variable")
    return token


def _push_alation(log, transformed, base_url: str, token_env: str):
    """POST to Alation. Auth: TOKEN header (not Bearer)."""
    import requests
    token = _get_token(token_env)
    resp = requests.post(
        f"{base_url}/integration/v2/lineage/",
        json=transformed,
        headers={"TOKEN": token, "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    # Alation returns a job ID for async processing
    log.info(f"Alation lineage job submitted: {resp.json()}")


def _push_collibra(log, transformed, base_url: str, token_env: str):
    """POST to Collibra. Auth: Bearer token via Authorization header."""
    import requests
    token = _get_token(token_env)
    resp = requests.post(
        f"{base_url}/rest/2.0/import/json-job",
        json=transformed,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    log.info(f"Collibra import: {resp.status_code}")


def _push_datahub(log, transformed, base_url: str, token_env: str):
    """POST each proposal to DataHub. Auth: Bearer + X-RestLi-Protocol-Version."""
    import requests
    token = _get_token(token_env)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-RestLi-Protocol-Version": "2.0.0",
    }
    # DataHub ingestProposal accepts one proposal at a time
    for i, proposal in enumerate(transformed):
        resp = requests.post(
            f"{base_url}/aspects?action=ingestProposal",
            json=proposal,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
    log.info(f"DataHub: ingested {len(transformed)} aspect proposals")


def _push_openlineage(log, transformed, base_url: str, token_env: str):
    """POST to OpenLineage. Auth: Bearer token (optional for some backends)."""
    import os, requests
    headers = {"Content-Type": "application/json"}
    token = os.environ.get(token_env) if token_env else None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(
        f"{base_url}/api/v1/lineage",
        json=transformed,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    log.info(f"OpenLineage: {resp.status_code}")


def _push_webhook(log, transformed, base_url: str, token_env: str):
    """POST to any URL. Auth: Bearer token if token_env is set."""
    import os, requests
    headers = {"Content-Type": "application/json"}
    token = os.environ.get(token_env) if token_env else None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(base_url, json=transformed, headers=headers, timeout=30)
    resp.raise_for_status()
    log.info(f"Webhook: {resp.status_code}")


def _push_file(log, transformed, base_url: str, **_):
    out = Path(base_url)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(transformed, indent=2))
    log.info(f"File write: {out} ({out.stat().st_size:,} bytes)")


_PUSH_FUNCTIONS = {
    "alation": _push_alation,
    "collibra": _push_collibra,
    "datahub": _push_datahub,
    "openlineage": _push_openlineage,
    "webhook": _push_webhook,
    "file": _push_file,
}


# ═════════════════════════════════════════════════════════════════════
# Component
# ═════════════════════════════════════════════════════════════════════

def _build_lineage_from_dagster_plus_graphql(log, dagster_plus_token_env: str) -> Optional[dict]:
    """Query the Dagster+ GraphQL API for the full deployment-wide asset graph.

    This gives lineage across ALL code locations — not just the current one.

    Auto-derives the GraphQL URL from DAGSTER_CLOUD_URL (auto-set by Dagster+)
    and DAGSTER_CLOUD_DEPLOYMENT_NAME. The user must provide a token via the
    configured env var (dagster_plus_token_env) — this is NOT auto-injected
    into user code by Dagster+.

    Returns None if the API is unavailable (OSS, missing token, etc.).
    """
    import os

    # DAGSTER_CLOUD_URL is auto-set (e.g. "https://myorg.dagster.cloud")
    cloud_url = os.environ.get("DAGSTER_CLOUD_URL")
    deployment = os.environ.get("DAGSTER_CLOUD_DEPLOYMENT_NAME", "prod")
    token = os.environ.get(dagster_plus_token_env)

    if cloud_url and token:
        graphql_url = f"{cloud_url}/{deployment}/graphql"
    else:
        graphql_url = None

    if not graphql_url or not token:
        return None

    try:
        import requests

        query = """
        query AssetLineageQuery {
            assetNodes {
                assetKey { path }
                groupName
                computeKind
                description
                dependencyKeys { path }
                dependedByKeys { path }
                opNames
                repository { name location { name } }
                freshnessPolicy { cronSchedule maximumLagMinutes }
            }
        }
        """

        resp = requests.post(
            graphql_url,
            json={"query": query},
            headers={"Dagster-Cloud-Api-Token": token, "Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        asset_nodes = data.get("data", {}).get("assetNodes", [])
        if not asset_nodes:
            return None

        nodes = []
        edges = []
        group_summary: dict[str, list[str]] = {}

        for an in asset_nodes:
            key_str = "/".join(an["assetKey"]["path"])
            code_location = an.get("repository", {}).get("location", {}).get("name", "unknown")
            grp = an.get("groupName") or "ungrouped"

            nodes.append({
                "asset_key": an["assetKey"]["path"],
                "asset_key_string": key_str,
                "group": grp,
                "kinds": [an["computeKind"]] if an.get("computeKind") else [],
                "description": (an.get("description") or "")[:500],
                "metadata": {"code_location": code_location},
                "freshness_policy": str(an["freshnessPolicy"]) if an.get("freshnessPolicy") else None,
                "code_location": code_location,
                "parent_count": len(an.get("dependencyKeys", [])),
                "child_count": len(an.get("dependedByKeys", [])),
            })

            group_summary.setdefault(grp, []).append(key_str)

            for dep_key in an.get("dependencyKeys", []):
                edges.append({
                    "upstream": "/".join(dep_key["path"]),
                    "downstream": key_str,
                })

        log.info(f"GraphQL: fetched {len(nodes)} assets across deployment")
        return {
            "sync_metadata": {
                "synced_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source": "dagster_plus_graphql",
                "total_nodes": len(nodes),
                "total_edges": len(edges),
                "total_groups": len(group_summary),
                "assets_with_freshness_policy": sum(1 for n in nodes if n["freshness_policy"]),
                "code_locations": list({n["code_location"] for n in nodes}),
            },
            "nodes": nodes,
            "edges": edges,
            "group_summary": {
                grp: {"count": len(assets), "assets": assets}
                for grp, assets in sorted(group_summary.items())
            },
        }

    except Exception as e:
        log.warning(f"GraphQL query failed, falling back to local graph: {e}")
        return None


class CatalogLineageSync(dg.Component, dg.Model, dg.Resolvable):
    """Automatic catalog lineage sync — just add the YAML.

    Adds a sensor that periodically exports the Dagster asset lineage
    graph to your data catalog. Supports Alation, Collibra, DataHub,
    OpenLineage, generic webhooks, and file output.

    **Scope:**
    - ``scope: "code_location"`` (default) — exports lineage from the
      current code location only, via ``repository_def.asset_graph``.
    - ``scope: "deployment"`` — queries the Dagster+ GraphQL API for
      the full deployment-wide graph across ALL code locations. Requires
      Dagster+ (Serverless or Hybrid) with ``DAGSTER_CLOUD_GRAPHQL_URL``
      and ``DAGSTER_CLOUD_API_TOKEN`` env vars. Falls back to code
      location scope if the API is unavailable.

    The sensor hashes the graph structure and only pushes when the
    lineage has actually changed (code deploy, defs state refresh).
    """

    demo_mode: bool = True
    scope: str = "code_location"  # "code_location" or "deployment"
    catalog_target: str = "alation"  # alation, collibra, datahub, openlineage, webhook, file
    catalog_url: str = "https://catalog.internal/api"
    api_token_env: str = "CATALOG_API_TOKEN"  # env var for catalog API auth
    dagster_plus_token_env: str = "DAGSTER_PLUS_TOKEN"  # env var for Dagster+ GraphQL (deployment scope only)
    demo_export_path: Optional[str] = "data/exports/catalog_lineage.json"
    sensor_interval_seconds: int = 3600
    sensor_name: str = "catalog_lineage_sync"
    sensor_default_status: str = "STOPPED"

    # Source system identification — tells the catalog WHO is sending this lineage
    platform_name: str = "dagster"  # registered platform name in the catalog
    platform_display_name: str = "Dagster"
    deployment_name: str = ""  # e.g. "prod", "staging" — auto-filled from DAGSTER_CLOUD_DEPLOYMENT_NAME if empty
    dagster_ui_url: str = ""  # e.g. "https://myorg.dagster.cloud/prod" — for drill-down links
    organization: str = ""  # e.g. "Westpac", "Maple Finance"
    code_location_name: str = ""  # auto-filled from context if empty

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        import os

        demo_mode = self.demo_mode
        scope = self.scope
        catalog_target = self.catalog_target
        catalog_url = self.catalog_url
        token_env = self.api_token_env
        dagster_plus_token_env = self.dagster_plus_token_env
        export_path = self.demo_export_path
        sensor_name = self.sensor_name

        # Build source system identity (auto-fill from env where possible)
        source_system = {
            "platform": self.platform_name,
            "platform_display_name": self.platform_display_name,
            "deployment": self.deployment_name or os.environ.get("DAGSTER_CLOUD_DEPLOYMENT_NAME", "local"),
            "dagster_ui_url": self.dagster_ui_url or os.environ.get("DAGSTER_CLOUD_URL", ""),
            "organization": self.organization,
            "code_location": self.code_location_name,
        }
        default_status = (
            dg.DefaultSensorStatus.RUNNING
            if self.sensor_default_status == "RUNNING"
            else dg.DefaultSensorStatus.STOPPED
        )

        push_fn = _PUSH_FUNCTIONS.get(catalog_target)
        transform_fn = _TRANSFORMERS.get(catalog_target, _transform_passthrough)
        if not push_fn and not demo_mode:
            raise ValueError(
                f"Unknown catalog_target '{catalog_target}'. "
                f"Supported: {', '.join(_PUSH_FUNCTIONS.keys())}"
            )

        @dg.sensor(
            name=sensor_name,
            minimum_interval_seconds=self.sensor_interval_seconds,
            default_status=default_status,
            description=(
                f"Syncs the Dagster asset lineage graph to {catalog_target}. "
                f"Only pushes when the graph changes."
            ),
        )
        def catalog_sync_sensor(context: dg.SensorEvaluationContext):
            # Choose data source based on scope
            payload = None
            if scope == "deployment":
                # Try the Dagster+ GraphQL API for full deployment-wide lineage
                payload = _build_lineage_from_dagster_plus_graphql(context.log, dagster_plus_token_env)
                if payload:
                    context.log.info(
                        f"Using deployment-wide graph via Dagster+ GraphQL "
                        f"({len(payload.get('sync_metadata', {}).get('code_locations', []))} code locations)"
                    )

            if payload is None:
                # Fall back to current code location's asset graph
                if scope == "deployment":
                    context.log.info("Dagster+ GraphQL unavailable, falling back to code location scope")
                payload = _build_lineage_payload(context.repository_def)

            meta = payload["sync_metadata"]
            payload["sync_metadata"]["catalog_target"] = catalog_target
            payload["sync_metadata"]["scope"] = scope

            # Enrich with source system identity
            _ss = dict(source_system)
            if not _ss["code_location"]:
                _ss["code_location"] = context.repository_name
            if _ss["dagster_ui_url"] and _ss["deployment"]:
                _ss["dagster_ui_url"] = f"{_ss['dagster_ui_url']}/{_ss['deployment']}"
            payload["source_system"] = _ss

            current_hash = _hash_payload(payload)
            previous_hash = context.cursor

            if previous_hash == current_hash:
                context.log.info(
                    f"Lineage unchanged (hash={current_hash}), skipping sync. "
                    f"Graph: {meta['total_nodes']} nodes, {meta['total_edges']} edges."
                )
                return

            context.log.info(
                f"Lineage changed ({previous_hash or 'first run'} → {current_hash}): "
                f"{meta['total_nodes']} nodes, {meta['total_edges']} edges, "
                f"{meta['assets_with_freshness_policy']} with freshness policies"
            )

            # Transform the internal payload to the target's expected format
            # All transformers receive the full payload which now includes source_system
            transformed = transform_fn(payload)

            if demo_mode:
                if export_path:
                    out = Path(export_path)
                    out.parent.mkdir(parents=True, exist_ok=True)
                    # Write both the raw graph and the transformed payload
                    out.write_text(json.dumps({
                        "internal_graph": payload,
                        f"{catalog_target}_payload": transformed,
                    }, indent=2))
                    context.log.info(f"[DEMO] Wrote lineage to {out} ({out.stat().st_size:,} bytes)")

                context.log.info(f"[DEMO] Target: {catalog_target}")
                context.log.info(f"[DEMO] Transformed payload type: {type(transformed).__name__}")
                context.log.info(f"[DEMO] Would push to: {catalog_url}")
                for grp, info in sorted(payload["group_summary"].items()):
                    context.log.info(f"  {grp} ({info['count']}): {', '.join(info['assets'])}")
            else:
                push_fn(context.log, transformed, catalog_url, token_env)
                context.log.info(
                    f"Pushed to {catalog_target}: "
                    f"{meta['total_nodes']} nodes, {meta['total_edges']} edges"
                )

            context.update_cursor(current_hash)

        return dg.Definitions(sensors=[catalog_sync_sensor])
