# Versioning & Release Policy — melius-core

This document defines how `melius-core` is versioned, how compatibility is
guaranteed, and how downstream consumers (team extensions and the Health-Model
pipeline) must pin against it. It is the authoritative policy referenced by
Task 20 of the open-sourcing plan
(`docs/plans/melius-core-separation-and-opensourcing-plan.md`).

> Scope: the **public contract** below (Sections 1-4) travels with `melius-core`
> to its public repository. The **downstream** rules (Section 5) are how *this*
> repo's extensions and the Health-Model pipeline consume core; they stay
> internal.

---

## 1. Version scheme — Semantic Versioning 2.0.0

`melius-core` follows [SemVer](https://semver.org/): **`MAJOR.MINOR.PATCH`**.

| Part | Bump when… | Example |
| --- | --- | --- |
| **MAJOR** | A backward-**incompatible** change to the public API — a symbol is removed/renamed, a signature changes, or documented behavior changes. | `1.4.2 → 2.0.0` |
| **MINOR** | Backward-**compatible** functionality is added (new module, new optional parameter with a default, new source/sink). | `1.4.2 → 1.5.0` |
| **PATCH** | Backward-compatible bug fixes only — no API surface change. | `1.4.2 → 1.4.3` |

The version is the single value in `pypkg/core/__version__.py` and is bumped with
[`bumpversion`](https://github.com/c4urself/bump2version) (never edited by hand).

### First stable release
Core currently sits at `1.0.dev0` (a pre-release marker). The first published
build **graduates to `1.0.0`**. Until then, `1.0.devN` builds carry **no**
compatibility guarantee and must not be pinned by exact version outside CI.

---

## 2. The public API contract

The compatibility promise applies to the **documented public surface** — the
symbols locked by `pypkg/core/tests/test_public_api.py`:

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

1. Land the change (public-API tests green).
2. Record the change under the new version in `CHANGELOG.md` (Keep-a-Changelog
   style: Added / Changed / Deprecated / Removed / Fixed). A MAJOR entry must
   list every breaking change and its migration note.
3. `bumpversion {major|minor|patch}` — updates `__version__.py`, commits, tags
   (the tag must include the changelog entry).
4. Build the wheel (`pip wheel --no-deps --use-pep517 . -w dist`) and publish to
   the configured index/feed.

> **Action (Phase 2):** each independently-versioned package needs its own
> `bumpversion` config. Today only the combined framework is wired
> (`pypkg/src/.bumpversion.cfg`); when core is fed from the artifact feed,
> add a per-package config for `pypkg/core` (and each extension) so versions
> move independently.

---

## 5. Downstream pinning (internal consumers)

Three consumer tiers, three pinning disciplines:

### 5a. Team extensions → **compatible-range floor**
`melius-vmwatch`, `melius-altus`, `melius-prism` declare a **compatible range**
in their `setup.py` `install_requires`, not an exact pin — a floor at the
feature they need, capped below the next MAJOR:

```python
install_requires = [
    "melius-core>=1.5,<2",   # needs a 1.5+ feature; accepts any 1.x
]
```

Raise the floor only when the extension starts using a newer core feature; bump
the cap only after validating against the new MAJOR. *(Extensions currently
declare an unpinned `"melius-core"` — acceptable inside the Phase-0 combined
wheel where one build supplies all packages; the range floor becomes required in
Phase 2 when core is installed separately from the feed.)*

### 5b. Health-Model pipeline → **exact pin**
The HM build/deploy pins the **exact** core version it validated against, so a
new core publish can never silently change a production job:

```
melius-core==1.5.3
```

Bumping this pin is a deliberate, reviewed change (update the pin → CI runs the
full staged pytest/flake8/integrity gates → merge). This is the mechanism that
absorbs "version skew between core (feed) and extensions" (Risk in the plan doc §7).

### 5c. Public/external consumers → **compatible release**
External users are advised to pin to a compatible range (`melius-core~=1.5`,
i.e. `>=1.5,<2`) and upgrade MAJOR deliberately after reading the changelog.

---

## 6. Summary

| Consumer | Pin style | Example |
| --- | --- | --- |
| Team extension (in-repo) | compatible range (floor + MAJOR cap) | `melius-core>=1.5,<2` |
| Health-Model pipeline | exact | `melius-core==1.5.3` |
| External user | compatible release | `melius-core~=1.5` |

Core promises: **PATCH** = safe drop-in, **MINOR** = safe additive upgrade,
**MAJOR** = review the changelog before upgrading. Downstreams never need to
track core's internal history — only its SemVer number and this contract.
