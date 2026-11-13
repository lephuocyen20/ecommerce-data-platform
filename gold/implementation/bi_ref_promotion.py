"""Fail-closed promotion of a verified Gold Nessie ref.

This module intentionally has no Spark, MinIO, Iceberg client, SQL, or object
storage write path. The only optional external operation is the Nessie REST
reference GET/create adapter; creation is guarded by ``--apply`` and an
expected current state.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen

from gold.implementation.gold_table_registry import RUN06_NESSIE_REF, RUN06_SCHEMA, RUN06_TABLES


TARGET_REF = "bi_gold_current"
EXPECTED_CANDIDATE_REF = RUN06_NESSIE_REF
EXPECTED_SCHEMA = RUN06_SCHEMA
EXPECTED_TABLES = RUN06_TABLES
HASH_RE = re.compile(r"^[0-9a-fA-F]{8,128}$")
REF_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")
LOCAL_NESSIE_HOSTS = {"localhost", "127.0.0.1", "host.docker.internal"}
LOCAL_NESSIE_PORTS = {19120, 19130, 19132}


class PromotionError(ValueError):
    """Raised when promotion input is unsafe or does not match the contract."""


class NessieReferenceNotFound(PromotionError):
    """The requested Nessie reference is absent (a safe CAS state)."""


@dataclass(frozen=True)
class CandidateManifest:
    candidate_ref: str
    candidate_hash: str
    schema: str
    tables: tuple[str, ...]
    status: str


@dataclass(frozen=True)
class NessieReference:
    name: str
    hash: str
    ref_type: str = "BRANCH"


def _require_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PromotionError(f"{label} must be a non-empty string")
    return value.strip()


def _require_hash(value: Any, label: str) -> str:
    value = _require_string(value, label)
    if not HASH_RE.fullmatch(value):
        raise PromotionError(f"{label} must be a hexadecimal Nessie hash")
    return value


def _load_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PromotionError(f"manifest is not valid JSON: {path}") from exc
    if not isinstance(value, dict):
        raise PromotionError("manifest must be a JSON object")
    return value


def validate_candidate_manifest(
    manifest: Mapping[str, Any],
    *,
    candidate_ref: str = EXPECTED_CANDIDATE_REF,
    candidate_hash: str,
    schema: str = EXPECTED_SCHEMA,
) -> CandidateManifest:
    """Validate exact run/schema/table/status identity before any REST call."""

    candidate_ref = _require_string(candidate_ref, "candidate_ref")
    if not REF_RE.fullmatch(candidate_ref):
        raise PromotionError("candidate_ref has an invalid format")
    candidate_hash = _require_hash(candidate_hash, "candidate_hash")
    schema = _require_string(schema, "schema")
    if candidate_ref != EXPECTED_CANDIDATE_REF:
        raise PromotionError("candidate_ref is outside the certified run-06 scope")
    if schema != EXPECTED_SCHEMA:
        raise PromotionError("schema is outside the certified run-06 scope")

    if manifest.get("status") != "VERIFIED":
        raise PromotionError("manifest status must be exactly VERIFIED")
    if manifest.get("can_write") is not False:
        raise PromotionError("manifest can_write must be exactly false")
    if manifest.get("nessie_ref") != candidate_ref:
        raise PromotionError("manifest Nessie ref does not match candidate_ref")
    if manifest.get("nessie_hash") != candidate_hash:
        raise PromotionError("manifest Nessie hash does not match candidate_hash")
    if manifest.get("schema") != schema:
        raise PromotionError("manifest schema does not match the certified schema")

    raw_tables = manifest.get("tables")
    if not isinstance(raw_tables, list) or len(raw_tables) != len(EXPECTED_TABLES):
        raise PromotionError("manifest must contain exactly seven table records")
    seen: list[str] = []
    for item in raw_tables:
        if not isinstance(item, dict):
            raise PromotionError("manifest table records must be objects")
        table = _require_string(item.get("table"), "manifest table")
        if table in seen or table not in EXPECTED_TABLES:
            raise PromotionError("manifest table set is not the certified seven-table set")
        if item.get("status") != "VERIFIED":
            raise PromotionError(f"manifest table {table} status must be VERIFIED")
        snapshot_id = item.get("snapshot_id")
        if not isinstance(snapshot_id, int) or snapshot_id <= 0:
            raise PromotionError(f"manifest table {table} needs a positive snapshot_id")
        seen.append(table)
    if tuple(sorted(seen)) != tuple(sorted(EXPECTED_TABLES)):
        raise PromotionError("manifest table set is incomplete or arbitrary")

    return CandidateManifest(candidate_ref, candidate_hash, schema, tuple(sorted(seen)), "VERIFIED")


def load_candidate_manifest(
    path: Path,
    *,
    candidate_ref: str = EXPECTED_CANDIDATE_REF,
    candidate_hash: str,
    schema: str = EXPECTED_SCHEMA,
) -> CandidateManifest:
    return validate_candidate_manifest(
        _load_json(path), candidate_ref=candidate_ref, candidate_hash=candidate_hash, schema=schema
    )


class NessieRestAdapter:
    """Small Nessie v2 reference adapter; no data/object client is involved."""

    def __init__(self, base_uri: str, *, timeout: float = 10.0) -> None:
        self.base_uri = validate_local_nessie_uri(base_uri)
        self.timeout = timeout

    def _request(self, method: str, path: str, body: Optional[Mapping[str, Any]] = None) -> Mapping[str, Any]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = Request(
            f"{self.base_uri}{path}",
            data=data,
            method=method,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 404:
                raise NessieReferenceNotFound("Nessie reference was not found") from exc
            raise PromotionError(f"Nessie REST {method} failed") from exc
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise PromotionError(f"Nessie REST {method} failed") from exc
        if not isinstance(payload, dict):
            raise PromotionError("Nessie REST response must be a JSON object")
        return payload

    def get_ref(self, ref: str) -> NessieReference:
        payload = self._request("GET", f"/trees/{quote(ref, safe='')}")
        name = _require_string(payload.get("name", ref), "Nessie reference name")
        digest = _require_hash(payload.get("hash"), "Nessie reference hash")
        return NessieReference(name=name, hash=digest, ref_type=str(payload.get("type", "BRANCH")).upper())

    def create_tag(self, ref: str, target_hash: str) -> NessieReference:
        """Create an immutable TAG at an already verified commit hash."""
        ref = _require_string(ref, "ref")
        target_hash = _require_hash(target_hash, "target_hash")
        payload = self._request(
            "POST",
            f"/trees?{urlencode({'name': ref, 'type': 'TAG'})}",
            {"type": "DETACHED", "hash": target_hash},
        )
        response = payload.get("reference")
        if not isinstance(response, dict):
            raise PromotionError("Nessie create response is missing required reference object")
        for key in ("name", "hash", "type"):
            if key not in response:
                raise PromotionError(f"Nessie create response is missing required field: {key}")
        response_name = _require_string(response["name"], "Nessie reference name")
        if response_name != ref:
            raise PromotionError("Nessie create response name does not match requested ref")
        if response["type"] != "TAG":
            raise PromotionError("Nessie create response type must be exactly TAG")
        return NessieReference(
            name=response_name,
            hash=_require_hash(response["hash"], "Nessie reference hash"),
            ref_type="TAG",
        )


def promote_candidate(
    manifest: CandidateManifest,
    *,
    adapter: Optional[NessieRestAdapter] = None,
    target_ref: str = TARGET_REF,
    expected_current_hash: Optional[str] = None,
    apply: bool = False,
) -> dict[str, Any]:
    if target_ref != TARGET_REF:
        raise PromotionError("target_ref must be exactly bi_gold_current")
    if apply and adapter is None:
        raise PromotionError("--apply requires a Nessie REST adapter")
    if apply and not expected_current_hash:
        raise PromotionError("--apply requires expected_current_hash for CAS (hash or ABSENT)")
    if expected_current_hash is not None:
        expected_current_hash = expected_current_hash.strip()
        if expected_current_hash != "ABSENT":
            expected_current_hash = _require_hash(expected_current_hash, "expected_current_hash")

    current = None
    target_absent = False
    if adapter is not None:
        try:
            current = adapter.get_ref(target_ref)
        except NessieReferenceNotFound:
            target_absent = True

    if target_absent:
        if expected_current_hash not in (None, "ABSENT"):
            raise PromotionError("target is absent but expected_current_hash is not ABSENT")
    elif current is not None:
        if current.ref_type != "TAG":
            raise PromotionError("stable target must be an immutable TAG")
        if expected_current_hash == "ABSENT":
            raise PromotionError("target exists but expected_current_hash is ABSENT")
        if expected_current_hash is not None and current.hash != expected_current_hash:
            raise PromotionError("current target hash does not match expected_current_hash")
        if current.hash != manifest.candidate_hash:
            raise PromotionError("existing stable TAG hash differs from candidate_hash")

    if apply:
        if target_absent:
            if expected_current_hash != "ABSENT":
                raise PromotionError("creating an absent target requires expected_current_hash=ABSENT")
            created = adapter.create_tag(target_ref, manifest.candidate_hash)  # type: ignore[union-attr]
            if created.ref_type != "TAG" or created.hash != manifest.candidate_hash:
                raise PromotionError("Nessie create response did not verify the immutable TAG")
            return {
                "mode": "APPLY_CREATE_TAG",
                "target_ref": target_ref,
                "assigned_hash": created.hash,
                "schema": manifest.schema,
            }
        if current is None:
            raise PromotionError("unable to verify target state before apply")
        return {
            "mode": "APPLY_NOOP",
            "target_ref": target_ref,
            "assigned_hash": current.hash,
            "schema": manifest.schema,
        }
    return {
        "mode": "DRY_RUN",
        "target_ref": target_ref,
        "candidate_ref": manifest.candidate_ref,
        "candidate_hash": manifest.candidate_hash,
        "current_hash": None if current is None else current.hash,
        "current_state": "ABSENT" if target_absent else ("VERIFIED_TAG" if current is not None else "UNVERIFIED"),
        "planned_operation": "CREATE_TAG" if target_absent else "NOOP",
        "schema": manifest.schema,
        "tables": list(manifest.tables),
    }


def validate_local_nessie_uri(value: str) -> str:
    """Return a normalized approved local Nessie v2 URI or fail closed."""

    value = _require_string(value, "nessie_uri").rstrip("/")
    parsed = urlparse(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise PromotionError("nessie_uri has an invalid port") from exc
    if (
        parsed.scheme != "http"
        or parsed.hostname not in LOCAL_NESSIE_HOSTS
        or port not in LOCAL_NESSIE_PORTS
        or parsed.path != "/api/v2"
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        raise PromotionError("nessie_uri must be an approved local HTTP /api/v2 endpoint")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate or promote the stable local BI Nessie ref")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--candidate-hash", required=True)
    parser.add_argument("--candidate-ref", default=EXPECTED_CANDIDATE_REF)
    parser.add_argument("--schema", default=EXPECTED_SCHEMA)
    parser.add_argument("--nessie-uri")
    parser.add_argument("--expected-current-hash")
    parser.add_argument("--apply", action="store_true", help="perform guarded Nessie TAG creation or no-op")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    try:
        manifest = load_candidate_manifest(
            args.manifest,
            candidate_ref=args.candidate_ref,
            candidate_hash=args.candidate_hash,
            schema=args.schema,
        )
        if args.apply and os.environ.get("BI_REF_PROMOTION_APPLY") != "1":
            raise PromotionError("--apply requires BI_REF_PROMOTION_APPLY=1")
        adapter = NessieRestAdapter(args.nessie_uri) if args.nessie_uri else None
        if args.apply and adapter is None:
            raise PromotionError("--apply requires --nessie-uri")
        print(json.dumps(promote_candidate(
            manifest,
            adapter=adapter,
            expected_current_hash=args.expected_current_hash,
            apply=args.apply,
        ), sort_keys=True))
        return 0
    except PromotionError as exc:
        print(f"PROMOTION_REJECTED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
