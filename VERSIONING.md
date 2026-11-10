# Versioning & Release Policy — melius-core

This document defines the version scheme, public API contract, release process,
and dependency pinning guidance for `melius-core`.

---

## 1. Version scheme — Semantic Versioning 2.0.0

`melius-core` follows [SemVer](https://semver.org/): **`MAJOR.MINOR.PATCH`**.

| Part | Bump when… | Example |
| --- | --- | --- |
| **MAJOR** | A backward-**incompatible** change to the public API — a symbol is removed/renamed, a signature changes, or documented behavior changes. | `1.4.2 → 2.0.0` |
| **MINOR** | Backward-**compatible** functionality is added (new module, new optional parameter with a default, new source/sink). | `1.4.2 → 1.5.0` |
| **PATCH** | Backward-compatible bug fixes only — no API surface change. | `1.4.2 → 1.4.3` |

The version is the single value in [__version__.py](./__version__.py).

### First stable release
Core currently sits at `1.0.dev0` (a pre-release marker). The first published
build **graduates to `1.0.0`**. Until then, `1.0.devN` builds carry **no**
compatibility guarantee and must not be pinned by exact version outside CI.

Application-specific table schemas and configuration keys are not part of core.
Applications and extension packages must define these values themselves. Consumers
of pre-release builds must migrate those imports and attribute references before
they adopt the cleaned core API.

---

## 2. The public API contract

The compatibility promise applies to the **documented public surface** — the
symbols locked by [tests/test_public_api.py](./tests/test_public_api.py):

- `melius.init(dbutils, sparkSession, sqlContext=None)` — the entry point.
- The public classes/enums under `melius.connectors`, `melius.libraries`,
  `melius.session`, `melius.restapis`, `melius.sparkplatform`, and their
  documented constructor signatures.
- `JobRunExitMode` enum member names/values.

Anything **not** in that surface — private helpers (`_`-prefixed), internal
constants, module internals — is **not** covered and may change in any release.

Any change to the locked surface requires updating `test_public_api.py` **in the
same commit** so the change is explicit and reviewable. Backward-**incompatible**
changes to the locked surface are, by definition, **MAJOR** changes; backward-
compatible additions (e.g., new modules, new optional parameters with defaults)
are **MINOR** changes.

---

## 3. Deprecation policy

A public symbol is **not** removed without a deprecation period:

1. Mark it deprecated in a **MINOR** release: emit a `DeprecationWarning` and
   document the replacement in the changelog.
2. Keep it working for **at least one full MINOR cycle**.
3. Remove it only in the next **MAJOR** release.

This lets downstream consumers migrate on a compatible line before a major bump
forces action.

---

## 4. Release process & changelog

1. Land the change with public-API tests, package tests, and lint passing.
2. Record the changes in the release notes: Added / Changed / Deprecated /
   Removed / Fixed. A MAJOR release must list every breaking change and its
   migration instructions.
3. Update [__version__.py](./__version__.py) for the release in a reviewed change.
   Tag the approved release commit.
4. Build the wheel (`pip wheel --no-deps --use-pep517 . -w dist`) from that
   approved commit and validate its metadata and contents.
5. Publish the approved artifacts to public PyPI through Microsoft ESRP Release.
   GitHub Actions provides CI only; a successful CI build is not a publication.
6. Verify installation of the exact published version in a fresh environment.

---

## 5. Dependency pinning

Three consumer tiers, three pinning disciplines:

### 5a. Extension packages → **compatible-range floor**
Extension packages should declare a **compatible range** in their dependency
metadata, not an exact pin — a floor at the
feature they need, capped below the next MAJOR:

```python
install_requires = [
    "melius-core>=1.5,<2",   # needs a 1.5+ feature; accepts any 1.x
]
```

Raise the floor only when the extension starts using a newer core feature; bump
the cap only after validating against the new MAJOR.

### 5b. Reproducible deployments → **exact pin**
A deployment should pin the **exact** core version it validated against, so a
new core publication cannot silently change a production job:

```
melius-core==1.5.3
```

Bumping this pin is a deliberate, reviewed change. Update the pin, run the
consumer's tests and integration checks, then approve the deployment.

### 5c. Applications → **compatible release**
Applications that accept compatible updates can use `melius-core~=1.5`
(equivalent to `>=1.5,<2`). Upgrade MAJOR deliberately after reading the
release notes.

---

## 6. Summary

| Consumer | Pin style | Example |
| --- | --- | --- |
| Extension package | compatible range (floor + MAJOR cap) | `melius-core>=1.5,<2` |
| Reproducible deployment | exact | `melius-core==1.5.3` |
| Application | compatible release | `melius-core~=1.5` |

Core promises: **PATCH** = safe drop-in, **MINOR** = safe additive upgrade,
**MAJOR** = review the changelog before upgrading. Downstreams never need to
track core's internal history — only its SemVer number and this contract.
