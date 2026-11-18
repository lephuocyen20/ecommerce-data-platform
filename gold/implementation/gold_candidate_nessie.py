"""Injected Nessie contract for isolated candidate bootstrap and CAS checks."""

from __future__ import annotations

from dataclasses import dataclass

from .gold_candidate_contract import CandidateContractError, STABLE_REF, _CANDIDATE_REF


class CandidateReferenceConflict(CandidateContractError):
    pass


@dataclass(frozen=True)
class CandidateReference:
    name: str
    ref_type: str
    hash: str


def _identity(value, name, *, ref_type=None):
    if getattr(value, "name", None) != name or not isinstance(getattr(value, "hash", None), str) or not getattr(value, "hash", ""):
        raise CandidateContractError(f"invalid Nessie {name} readback")
    if ref_type and getattr(value, "ref_type", "").upper() != ref_type:
        raise CandidateContractError(f"Nessie {name} must be a {ref_type}")
    return value


def ensure_candidate_ref(adapter, candidate_ref: str, *, source_ref: str = STABLE_REF):
    """Create/read back a branch from a stable source hash, without publication."""
    if _CANDIDATE_REF.fullmatch(candidate_ref or "") is None or source_ref != STABLE_REF:
        raise CandidateContractError("candidate ref cannot be the stable ref")
    source = _identity(adapter.get_ref(source_ref), source_ref)
    try:
        existing = adapter.get_ref(candidate_ref)
    except Exception as exc:
        if exc.__class__.__name__ not in {"NessieReferenceNotFound", "ReferenceNotFound"}:
            raise
        reread = _identity(adapter.get_ref(source_ref), source_ref)
        if reread.hash != source.hash:
            raise CandidateReferenceConflict("stable source head changed before candidate create")
        try:
            adapter.create_branch(candidate_ref, source_ref, reread.hash)
        except Exception as create_exc:
            if create_exc.__class__.__name__ not in {"NessieReferenceConflict", "ReferenceConflict"}:
                raise
        existing = adapter.get_ref(candidate_ref)
    existing = _identity(existing, candidate_ref, ref_type="BRANCH")
    if existing.hash != source.hash:
        raise CandidateReferenceConflict("candidate ref is not based on the read-back stable head")
    return existing


def assert_source_head_unchanged(adapter, source_ref: str, expected_hash: str):
    current = _identity(adapter.get_ref(source_ref), source_ref)
    if current.hash != expected_hash:
        raise CandidateReferenceConflict("stable source head changed before candidate WRITE")
    return current


__all__ = ["CandidateReference", "CandidateReferenceConflict", "assert_source_head_unchanged", "ensure_candidate_ref"]
