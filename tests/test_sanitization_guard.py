"""Sanitization guard for the open-sourceable melius-core surface.

The Phase-0 carve-out (see
``docs/plans/melius-core-separation-and-opensourcing-plan.md`` section 9.1)
scrubbed internal/sensitive references from core so it can be ported to a
public repo. These tests fail if any of those references are reintroduced:

* the ``ConstantStrings`` members removed during sanitization -- hardcoded CDE
  ADLS accounts and Kusto key-vault secret references -- must not come back by
  name;
* no ``ConstantStrings`` string value may hardcode an Azure storage endpoint or
  a key-vault (``kv-``) secret reference (caught generically, so this guard does
  not itself re-embed the former account name);
* ``RegionMapping`` must not reference the internal ``GuestAgentGenericLogs``
  table in its comments.

Reintroducing any of these is a sanitization regression and must be a deliberate
decision -- update this guard in the same change, with justification.
"""
import inspect
import re

import pytest

# ConstantStrings lives in Utility, which imports pyspark at module load time.
# Skip cleanly where pyspark is absent (bare dev box); CI has it installed.
pytest.importorskip("pyspark")

from melius.libraries.Utility import ConstantStrings  # noqa: E402
import melius.libraries.RegionMapping as regionmapping_module  # noqa: E402

# Members deleted during sanitization -- must never reappear on ConstantStrings.
REMOVED_CONSTANT_MEMBERS = [
    "KustoAppKey",
    "KustoScope",
    "CdeAdlsUrlBase_Prod",
    "CdeAdlsUrlBase_Test",
]

# Generic anti-patterns for hardcoded internal references (do not hardcode the
# specific former values here, to avoid re-leaking them through the guard).
_STORAGE_ENDPOINT = re.compile(
    r"https?://[A-Za-z0-9-]+\.(?:dfs|blob)\.core\.windows\.net", re.IGNORECASE
)
_KEYVAULT_SECRET = re.compile(r"\bkv-[\w-]+", re.IGNORECASE)


def _public_string_constants(cls):
    """Return {name: value} for public string attributes declared on ``cls``."""
    return {
        name: value
        for name, value in vars(cls).items()
        if not name.startswith("_") and isinstance(value, str)
    }


def test_removed_sensitive_constants_absent():
    present = [m for m in REMOVED_CONSTANT_MEMBERS if hasattr(ConstantStrings, m)]
    assert not present, (
        "Sanitization regression: ConstantStrings must not define "
        f"{present} -- these hardcoded CDE ADLS accounts / Kusto key-vault "
        "references were removed for open-sourcing. Source them from "
        "config/env instead."
    )


def test_no_hardcoded_storage_endpoint_in_constants():
    offenders = {
        name: value
        for name, value in _public_string_constants(ConstantStrings).items()
        if _STORAGE_ENDPOINT.search(value)
    }
    assert not offenders, (
        "Sanitization regression: hardcoded Azure storage endpoint(s) in "
        f"ConstantStrings: {sorted(offenders)}. Source storage accounts from "
        "config/env, not literals."
    )


def test_no_keyvault_secret_reference_in_constants():
    offenders = {
        name: value
        for name, value in _public_string_constants(ConstantStrings).items()
        if _KEYVAULT_SECRET.search(value)
    }
    assert not offenders, (
        "Sanitization regression: key-vault secret reference(s) in "
        f"ConstantStrings: {sorted(offenders)}. Reference secrets via the "
        "configured scope, not literals."
    )


def test_regionmapping_has_no_internal_table_reference():
    source = inspect.getsource(regionmapping_module)
    assert "GuestAgentGenericLogs" not in source, (
        "Sanitization regression: RegionMapping references the internal "
        "'GuestAgentGenericLogs' table; keep the module's comments generic."
    )
