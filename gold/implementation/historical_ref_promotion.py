"""Independent production Gold candidate promotion contract.

Only a verified candidate branch may advance ``gold_prod_current``.  The
adapter is injected so validation and unit tests cannot accidentally contact
Nessie or write lakehouse data.
"""

from __future__ import annotations

import re
import json
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from ingestion.historical.config import SUPPORTED_TABLES, candidate_ref_for_run

GOLD_TABLES = ("dim_date", "dim_customer", "dim_product", "fact_orders", "fact_order_items", "fact_payments", "fact_shipments")
STABLE_REF = "gold_prod_current"
_HASH = re.compile(r"^[0-9a-fA-F]{8,128}$")
_IDENTITY = re.compile(r"^[0-9a-fA-F]{64}$")


class PromotionError(ValueError):
    pass


class NessieReferenceNotFound(PromotionError):
    """Raised when a named Nessie reference does not exist."""


class NessieReferenceConflict(PromotionError):
    """Raised when a concurrent Nessie reference create wins with HTTP 409."""


class RefAdapter(Protocol):
    def get_ref(self, name: str): ...
    def create_branch(self, name: str, source_ref: str, source_hash: str): ...
    def update_branch(self, name: str, target_hash: str, expected_hash: str): ...


class NessieRestAdapter:
    """Nessie REST adapter used only after all candidate evidence is verified."""
    def __init__(self, base_uri: str, *, timeout: float = 10.0):
        parsed = urlparse(base_uri.rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
            raise PromotionError("invalid Nessie REST URI")
        self.base_uri, self.timeout = base_uri.rstrip("/"), timeout
        if not (parsed.path.endswith("/api/v2") or parsed.path.endswith("/v2")):
            raise PromotionError("Nessie REST URI must target the v2 API")

    def _request(self, method, path, body=None, *, base_uri=None):
        request = Request((self.base_uri if base_uri is None else base_uri) + path, data=None if body is None else json.dumps(body).encode(), method=method, headers={"Accept": "application/json", "Content-Type": "application/json"})
        try:
            with urlopen(request, timeout=self.timeout) as response:
                value = json.loads(response.read().decode())
        except HTTPError as exc:
            if exc.code == 404:
                raise NessieReferenceNotFound("Nessie reference was not found") from exc
            if exc.code == 409:
                raise NessieReferenceConflict("Nessie reference already exists") from exc
            raise PromotionError(f"Nessie REST {method} failed: HTTP {exc.code}") from exc
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise PromotionError(f"Nessie REST {method} failed") from exc
        if not isinstance(value, dict):
            raise PromotionError("Nessie response must be an object")
        return value.get("reference", value)

    def get_ref(self, name):
        value = self._request("GET", f"/trees/{quote(name, safe='')}")
        if not isinstance(value, dict) or value.get("name") != name or value.get("type", "").upper() not in {"BRANCH", "TAG"}:
            raise PromotionError("Nessie ref readback identity is invalid")
        return type("NessieRef", (), {"name": name, "ref_type": value["type"].upper(), "hash": _hash(value.get("hash"), "Nessie ref hash")})()

    def create_branch(self, name, source_ref, source_hash):
        source_hash = _hash(source_hash, "Nessie branch source hash")
        value = self._request(
            "POST",
            f"/trees?{urlencode({'name': name, 'type': 'BRANCH'})}",
            {"type": "DETACHED", "hash": source_hash},
        )
        if not isinstance(value, dict) or value.get("name") != name or value.get("type", "").upper() != "BRANCH":
            raise PromotionError("Nessie branch create response identity is invalid")
        return type("NessieRef", (), {"name": name, "ref_type": "BRANCH", "hash": _hash(value.get("hash"), "Nessie branch hash")})()

    def update_branch(self, name, target_hash, expected_hash):
        value = self._request("PUT", f"/trees/{quote(name + '@' + expected_hash, safe='@')}", {"type": "BRANCH", "name": name, "hash": target_hash})
        if not isinstance(value, dict) or value.get("name") != name or value.get("type") != "BRANCH":
            raise PromotionError("Nessie CAS response identity is invalid")
        return type("NessieRef", (), {"name": name, "ref_type": "BRANCH", "hash": _hash(value.get("hash"), "Nessie CAS hash")})()


@dataclass(frozen=True)
class HistoricalCandidate:
    run_id: str
    candidate_ref: str
    candidate_hash: str
    tables: tuple[str, ...] = GOLD_TABLES
    status: str = "VERIFIED"


def ensure_candidate_branch(adapter: RefAdapter, candidate_ref: str, *, base_ref: str = "main"):
    """Ensure an isolated candidate branch exists at the configured base hash."""
    if candidate_ref == STABLE_REF:
        raise PromotionError("candidate bootstrap cannot target the stable ref")
    if not isinstance(base_ref, str) or not base_ref or base_ref == STABLE_REF:
        raise PromotionError("candidate bootstrap requires a non-stable base ref")

    try:
        candidate = adapter.get_ref(candidate_ref)
    except NessieReferenceNotFound:
        base = adapter.get_ref(base_ref)
        if getattr(base, "name", None) != base_ref or getattr(base, "ref_type", "").upper() not in {"BRANCH", "TAG"}:
            raise PromotionError("candidate bootstrap base ref identity is invalid")
        expected_hash = _hash(getattr(base, "hash", None), "candidate bootstrap base hash")
        try:
            adapter.create_branch(candidate_ref, base_ref, expected_hash)
        except NessieReferenceConflict:
            # A concurrent creator is safe only if its immutable readback is
            # exactly the branch we intended to create.
            candidate = adapter.get_ref(candidate_ref)
        else:
            candidate = adapter.get_ref(candidate_ref)
        if (
            getattr(candidate, "name", None) != candidate_ref
            or getattr(candidate, "ref_type", "").upper() != "BRANCH"
            or getattr(candidate, "hash", None) != expected_hash
        ):
            raise PromotionError("candidate bootstrap readback identity does not match base ref")
        return candidate

    if getattr(candidate, "name", None) != candidate_ref or getattr(candidate, "ref_type", "").upper() != "BRANCH":
        raise PromotionError("existing candidate ref must be an exact BRANCH identity")
    _hash(getattr(candidate, "hash", None), "existing candidate hash")
    return candidate


def _hash(value, label):
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise PromotionError(f"{label} must be a hexadecimal Nessie hash")
    return value


def validate_candidate(report: Mapping[str, Any], *, run_id: str) -> HistoricalCandidate:
    expected_ref = candidate_ref_for_run(run_id)
    if not isinstance(report, Mapping) or report.get("status") != "VERIFIED":
        raise PromotionError("candidate report must be VERIFIED")
    if report.get("candidate_ref") != expected_ref:
        raise PromotionError("candidate ref does not match run_id")
    if report.get("stable_ref") not in (None, STABLE_REF):
        raise PromotionError("candidate report has an invalid stable ref")
    tables = report.get("tables")
    if tuple(sorted(tables or ())) != tuple(sorted(GOLD_TABLES)):
        raise PromotionError("candidate report must contain exactly seven Gold tables")
    outputs = report.get("outputs", [])
    required_evidence = {
        "table", "snapshot_id", "readback_status", "row_count", "file_count",
        "file_bytes", "manifest_identity", "readback_identity", "consistency_status",
    }
    for item in outputs:
        if not isinstance(item, Mapping) or item.get("table") not in GOLD_TABLES:
            raise PromotionError("candidate outputs contain an invalid Gold table")
        missing = sorted(required_evidence.difference(item))
        if missing:
            raise PromotionError(f"candidate output {item.get('table')} is missing evidence fields: {missing}")
        if (
            item.get("readback_status") != "PASS"
            or item.get("consistency_status") != "PASS"
            or not isinstance(item.get("snapshot_id"), int) or isinstance(item["snapshot_id"], bool) or item["snapshot_id"] <= 0
            or not isinstance(item.get("row_count"), int) or isinstance(item["row_count"], bool) or item["row_count"] < 0
            or not isinstance(item.get("file_count"), int) or isinstance(item["file_count"], bool) or item["file_count"] <= 0
            or not isinstance(item.get("file_bytes"), int) or isinstance(item["file_bytes"], bool) or item["file_bytes"] <= 0
            or not isinstance(item.get("manifest_identity"), str) or _IDENTITY.fullmatch(item["manifest_identity"]) is None
            or not isinstance(item.get("readback_identity"), str) or _IDENTITY.fullmatch(item["readback_identity"]) is None
            or item["manifest_identity"] == item["readback_identity"]
        ):
            raise PromotionError(f"candidate output {item['table']} has inconsistent Gold readback evidence (identity must be independent)")
    if len(outputs) != len(GOLD_TABLES) or tuple(sorted(item["table"] for item in outputs)) != tuple(sorted(GOLD_TABLES)):
        raise PromotionError("candidate report must contain seven outputs")
    return HistoricalCandidate(run_id, expected_ref, _hash(report.get("candidate_hash"), "candidate_hash"), GOLD_TABLES)


def promote_candidate(candidate: HistoricalCandidate, *, adapter: RefAdapter, expected_current_hash: str):
    if candidate.status != "VERIFIED" or candidate.candidate_ref != candidate_ref_for_run(candidate.run_id):
        raise PromotionError("candidate is not a verified historical candidate")
    expected_current_hash = _hash(expected_current_hash, "expected_current_hash")
    candidate_ref = adapter.get_ref(candidate.candidate_ref)
    if getattr(candidate_ref, "name", None) != candidate.candidate_ref or getattr(candidate_ref, "ref_type", "").upper() != "BRANCH" or getattr(candidate_ref, "hash", None) != candidate.candidate_hash:
        raise PromotionError("candidate ref/hash readback does not match verified candidate")
    current = adapter.get_ref(STABLE_REF)
    if getattr(current, "name", None) != STABLE_REF or getattr(current, "ref_type", "").upper() != "BRANCH":
        raise PromotionError("stable ref must be an existing BRANCH")
    if getattr(current, "hash", None) != expected_current_hash:
        raise PromotionError("stable ref changed before CAS promotion")
    updated = adapter.update_branch(STABLE_REF, candidate.candidate_hash, expected_current_hash)
    if getattr(updated, "hash", None) != candidate.candidate_hash:
        raise PromotionError("CAS response did not verify candidate hash")
    return {"mode": "APPLY_CAS", "stable_ref": STABLE_REF, "candidate_ref": candidate.candidate_ref, "assigned_hash": candidate.candidate_hash}


__all__ = ["GOLD_TABLES", "HistoricalCandidate", "NessieReferenceConflict", "NessieReferenceNotFound", "NessieRestAdapter", "PromotionError", "STABLE_REF", "ensure_candidate_branch", "promote_candidate", "validate_candidate"]
