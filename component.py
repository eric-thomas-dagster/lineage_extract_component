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
    """Transform to Alation Bulk Lineage API format.

    Alation expects: { "dataflow_objects": [...], "paths": [[src, tgt], ...] }
    Ref: https://developer.alation.com/dev/reference/postbulklineage
    """
    dataflow_objects = []
    for node in payload["nodes"]:
        dataflow_objects.append({
            "external_id": f"dagster://{'/'.join(node['asset_key'])}",
            "title": node["asset_key_string"],
            "description": node.get("description", ""),
            "content": json.dumps({
                "group": node["group"],
                "kinds": node["kinds"],
                "metadata": node["metadata"],
                "freshness_policy": node["freshness_policy"],
            }),
        })

    paths = []
    for edge in payload["edges"]:
        paths.append([
            f"dagster://{edge['upstream']}",
            f"dagster://{edge['downstream']}",
        ])

    return {"dataflow_objects": dataflow_objects, "paths": paths}


def _transform_collibra(payload: dict) -> list[dict]:
    """Transform to Collibra Input/Output Relations API format.

    Collibra expects a list of relation objects:
    [{ "sourceId": "...", "targetId": "...", "relationType": "..." }, ...]
    Ref: https://developer.collibra.com/rest/2.0/input-output-relations
    """
    relations = []
    for edge in payload["edges"]:
        relations.append({
            "source": {"name": edge["upstream"], "domain": "Dagster"},
            "target": {"name": edge["downstream"], "domain": "Dagster"},
            "relationType": {"name": "Dagster Lineage"},
        })
    return relations


def _transform_datahub(payload: dict) -> list[dict]:
    """Transform to DataHub MCE (Metadata Change Event) format.

    DataHub expects dataset URNs and UpstreamLineage aspects.
    Ref: https://datahubproject.io/docs/api/tutorials/lineage
    """
    # Build a parent lookup
    parent_map: dict[str, list[str]] = {}
    for edge in payload["edges"]:
        parent_map.setdefault(edge["downstream"], []).append(edge["upstream"])

    proposals = []
    for node in payload["nodes"]:
        key_str = node["asset_key_string"]
        urn = f"urn:li:dataset:(urn:li:dataPlatform:dagster,{key_str},PROD)"

        # Dataset properties aspect
        proposals.append({
            "entityUrn": urn,
            "entityType": "dataset",
            "aspectName": "datasetProperties",
            "aspect": {
                "name": key_str,
                "description": node.get("description", ""),
                "customProperties": {
                    "dagster_group": node["group"] or "",
                    "dagster_kinds": ",".join(node["kinds"]),
                    **({"freshness_policy": node["freshness_policy"]} if node["freshness_policy"] else {}),
                    **{k: str(v) for k, v in node.get("metadata", {}).items()},
                },
            },
        })

        # Upstream lineage aspect
        parents = parent_map.get(key_str, [])
        if parents:
            proposals.append({
                "entityUrn": urn,
                "entityType": "dataset",
                "aspectName": "upstreamLineage",
                "aspect": {
                    "upstreams": [
                        {
                            "dataset": f"urn:li:dataset:(urn:li:dataPlatform:dagster,{p},PROD)",
                            "type": "TRANSFORMED",
                        }
                        for p in parents
                    ],
                },
            })

    return proposals


def _transform_openlineage(payload: dict) -> dict:
    """Transform to OpenLineage RunEvent format.

    OpenLineage expects a RunEvent with job facets and I/O datasets.
    Ref: https://openlineage.io/docs/spec/run-event
    """
    datasets = []
    for node in payload["nodes"]:
        datasets.append({
            "namespace": "dagster",
            "name": node["asset_key_string"],
            "facets": {
                "schema": {
                    "fields": [
                        {"name": k, "type": str(type(v).__name__)}
                        for k, v in node.get("metadata", {}).items()
                    ],
                },
                "dagster_metadata": {
                    "group": node["group"],
                    "kinds": node["kinds"],
                    "freshness_policy": node["freshness_policy"],
                },
            },
        })

    return {
        "eventType": "COMPLETE",
        "eventTime": payload["sync_metadata"]["synced_at"],
        "job": {
            "namespace": "dagster",
            "name": "lineage_export",
        },
        "inputs": datasets,
        "outputs": [],
        "run": {
            "runId": payload["sync_metadata"]["synced_at"],
            "facets": {
                "dagster_lineage": {
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
# Catalog push functions — one per target type
# ═════════════════════════════════════════════════════════════════════

def _post_to_api(log, transformed_payload, url: str, token_env: str, endpoint_suffix: str = ""):
    """Generic authenticated POST."""
    import os, requests
    token = os.environ.get(token_env) if token_env else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif token_env:
        raise RuntimeError(f"Missing {token_env} environment variable")
    full_url = f"{url}{endpoint_suffix}" if endpoint_suffix else url
    resp = requests.post(full_url, json=transformed_payload, headers=headers, timeout=30)
    resp.raise_for_status()
    log.info(f"POST {full_url}: {resp.status_code}")


def _push_alation(log, transformed, base_url: str, token_env: str):
    _post_to_api(log, transformed, base_url, token_env, "/lineage/bulk")


def _push_collibra(log, transformed, base_url: str, token_env: str):
    _post_to_api(log, transformed, base_url, token_env, "/rest/2.0/inputOutputRelations/bulk")


def _push_datahub(log, transformed, base_url: str, token_env: str):
    _post_to_api(log, transformed, base_url, token_env, "/aspects?action=ingestProposal")


def _push_openlineage(log, transformed, base_url: str, token_env: str):
    _post_to_api(log, transformed, base_url, token_env, "/api/v1/lineage")


def _push_webhook(log, transformed, base_url: str, token_env: str):
    _post_to_api(log, transformed, base_url, token_env)


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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        demo_mode = self.demo_mode
        scope = self.scope
        catalog_target = self.catalog_target
        catalog_url = self.catalog_url
        token_env = self.api_token_env
        dagster_plus_token_env = self.dagster_plus_token_env
        export_path = self.demo_export_path
        sensor_name = self.sensor_name
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
