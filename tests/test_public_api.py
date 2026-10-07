"""Backward-compatibility freeze for the melius-core public API surface.

These tests lock the *public contract* that melius-core exposes to notebooks
(60+ Databricks notebooks call ``melius.init``) and to the extension packages
(melius-vmwatch / melius-altus / melius-prism), which subclass the core base
sources/sinks. They are intentionally signature-based and require no live Spark
session -- only that the modules import (pyspark must be installed, which it is
in CI). If any of these assertions fails, a public symbol was moved, renamed,
or its signature changed -- i.e. a breaking change that must be made
deliberately (update the expected surface below in the same change).

To intentionally evolve the surface, edit the expected data structures here in
the same commit as the code change, so the contract change is explicit and
reviewable.
"""
import importlib
import inspect
from enum import Enum

import pytest

# The whole surface depends on pyspark being importable (modules import
# pyspark at load time). Skip cleanly where it is absent (e.g. a bare dev box);
# CI has pyspark installed and will run these for real.
pytest.importorskip("pyspark")


def _param_spec(func, skip_self=True):
    """Return an ordered list of (param_name, has_default) for a callable."""
    params = list(inspect.signature(func).parameters.values())
    if skip_self and params and params[0].name == "self":
        params = params[1:]
    return [(p.name, p.default is not inspect.Parameter.empty) for p in params]


def _resolve(module_path, symbol):
    module = importlib.import_module(module_path)
    assert hasattr(module, symbol), (
        f"Public symbol '{symbol}' missing from module '{module_path}'"
    )
    return getattr(module, symbol)


# ---------------------------------------------------------------------------
# Tier 2 -- import path + kind. Every symbol notebooks import from core must
# resolve at its documented path and be a class (the entry point is a function
# and the exit mode is an Enum; both are checked in their own tests below).
# ---------------------------------------------------------------------------
CORE_CLASS_SYMBOLS = [
    ("melius.connectors.BlobStorageConnector", "BlobStorageConnector"),
    ("melius.connectors.KustoConnector", "KustoConnector"),
    ("melius.libraries.AccessToken", "AccessToken"),
    ("melius.libraries.Constants", "KnownStrings"),
    ("melius.libraries.Constants", "KnownTableSchemas"),
    ("melius.libraries.Logger", "Logger"),
    ("melius.libraries.RegionMapping", "RegionMapping"),
    ("melius.libraries.Utility", "Utils"),
    ("melius.libraries.WatermarkHelper", "WatermarkHelper"),
    ("melius.restapis.AzureOpenAIClient", "AzureOpenAIClient"),
    ("melius.session.sources.DeltaLakeSource", "DeltaLakeSource"),
    ("melius.session.sources.KustoSource", "KustoSource"),
]


@pytest.mark.parametrize("module_path,symbol", CORE_CLASS_SYMBOLS)
def test_core_class_symbol_importable(module_path, symbol):
    obj = _resolve(module_path, symbol)
    assert inspect.isclass(obj), (
        f"'{module_path}.{symbol}' is expected to be a class"
    )


# ---------------------------------------------------------------------------
# Tier 1 -- the single most important public entry point. 60+ notebooks call
# melius.init(dbutils, sparkSession, sqlContext=None). Lock it exactly.
# ---------------------------------------------------------------------------
def test_init_entrypoint_signature():
    melius = importlib.import_module("melius")
    assert hasattr(melius, "init"), "melius.init entry point is missing"
    assert inspect.isfunction(melius.init), "melius.init must be a function"

    spec = _param_spec(melius.init, skip_self=False)
    assert spec == [
        ("dbutils", False),
        ("sparkSession", False),
        ("sqlContext", True),
    ], f"melius.init signature changed: {spec}"

    # The default of sqlContext is part of the public contract.
    default = inspect.signature(melius.init).parameters["sqlContext"].default
    assert default is None, f"melius.init sqlContext default changed: {default!r}"


# ---------------------------------------------------------------------------
# Tier 3 -- JobRunExitMode enum value contract. Notebooks branch on these.
# ---------------------------------------------------------------------------
def test_job_run_exit_mode_enum_contract():
    exit_mode = _resolve("melius.session.JobRunSession", "JobRunExitMode")
    assert issubclass(exit_mode, Enum), "JobRunExitMode must be an Enum"

    actual = {member.name: member.value for member in exit_mode}
    assert actual == {
        "SUCCESS": "SUCCESS",
        "FAILURE": "FAILURE",
        "NONEWDATA": "NONEWDATA",
        "NONEWDATARETRY": "NONEWDATARETRY",
    }, f"JobRunExitMode members changed: {actual}"


# ---------------------------------------------------------------------------
# Tier 4 -- constructor signatures. The base classes are the contract the
# extension packages inherit; the concrete sources are instantiated directly
# by notebooks. Parameter names + ordering + which are optional are locked;
# default *values* are deliberately not asserted (some derive from constants).
# ---------------------------------------------------------------------------
CONSTRUCTOR_SIGNATURES = {
    ("melius.session.sources.BaseSource", "BaseSource"): [
        ("session", False),
        ("name", False),
        ("lookBackDelta", False),
        ("backFillStartDateTime", False),
    ],
    ("melius.session.sinks.BaseSink", "BaseSink"): [
        ("dataName", False),
        ("dataVersion", False),
    ],
    ("melius.session.sources.DeltaLakeSource", "DeltaLakeSource"): [
        ("session", False),
        ("dataName", False),
        ("dataVersion", False),
        ("storageConfig", False),
        ("deltaLakePath", False),
        ("watermarkPath", False),
        ("lookBackDelta", False),
        ("backFillStartDateTime", False),
        ("watermarkStorageConfig", True),
    ],
    ("melius.session.sources.KustoSource", "KustoSource"): [
        ("session", False),
        ("kustoCluster", False),
        ("kustoTable", False),
        ("kustoDatabase", False),
        ("query", False),
        ("watermarkQuery", False),
        ("parallelism", False),
        ("lookBackDelta", False),
        ("backFillStartDateTime", False),
        ("ingestionTimeMoniker", True),
    ],
}


@pytest.mark.parametrize("location,expected", list(CONSTRUCTOR_SIGNATURES.items()))
def test_constructor_signature(location, expected):
    module_path, symbol = location
    cls = _resolve(module_path, symbol)
    spec = _param_spec(cls.__init__)
    assert spec == expected, (
        f"{module_path}.{symbol}.__init__ signature changed.\n"
        f"  expected: {expected}\n"
        f"  actual:   {spec}"
    )
