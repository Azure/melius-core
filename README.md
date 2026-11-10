# melius-core

Core of the **Melius** framework: a lightweight, reusable Python toolkit for building
Azure Databricks data pipelines.

It provides:

- **Session management** (`melius.session`) — pipeline session lifecycle, checkpointing,
  and incremental (watermark-based) processing.
- **Connectors** (`melius.connectors`) — Kusto, Delta Lake, and ADLS/Blob storage
  source/sink primitives.
- **Spark platform helpers** (`melius.sparkplatform`) — Databricks/Spark utilities.
- **Libraries** (`melius.libraries`) — shared utilities, region mapping, and helpers.
- **REST API clients** (`melius.restapis`) — Azure OpenAI and Event Hub clients.
- **`melius.init(dbutils, sparkSession, sqlContext)`** — the public entry point used by
  notebooks to bootstrap a Melius session.

## Installation

```
pip install melius-core
```

> **Note:** `melius-core` is the intended public package name. It is **not yet published**
> to a public index — until then, install from a locally built wheel (see [Build](#build)):
>
> ```
> pip install dist/melius_core-<version>-py3-none-any.whl
> ```

`melius-core` targets Python 3.8+ and Apache Spark 3.5 (PySpark). It is designed to run
inside an Azure Databricks notebook, where `dbutils` and a `SparkSession` are provided by
the runtime.

## Getting Started

From a Databricks notebook, bootstrap Melius once at the top of your job by handing it the
runtime-provided `dbutils`, Spark session, and (optionally) SQL context:

```python
import melius

# dbutils and spark are provided by the Databricks runtime.
melius.init(dbutils, spark, sqlContext)
```

`melius.init(dbutils, sparkSession, sqlContext=None)` wires the runtime handles into the
framework so the connectors, sources, and sinks can access Spark and Databricks utilities.

From there, a pipeline typically:

1. Creates a session (`melius.session.JobRunSession`) to track the run and its watermark.
2. Reads from a **source** — e.g. `melius.session.sources.KustoSource` or
   `melius.session.sources.DeltaLakeSource`.
3. Writes to a **sink** (Delta Lake, Kusto, or partitioned Parquet), or to a
   custom sink provided by an extension package.

> A full, end-to-end Getting Started guide (session setup, incremental processing, and a
> worked source-to-sink example) is published separately alongside the how-to guides. This
> section is an orientation stub; see the project documentation for the complete walkthrough.

## Extending melius-core

`melius-core` is designed to be extended without modification. Separate extension packages can
depend on `melius-core` and add their own sources, sinks, and sessions under the shared
`melius.*` namespace. Core never imports an extension package.

## Build

```
pip wheel --no-deps --use-pep517 . -w dist
```

## Versioning

Version is tracked in [__version__.py](./__version__.py). See
[VERSIONING.md](./VERSIONING.md) for the compatibility policy and release process.

## Contributing

This project welcomes contributions and suggestions. See [CONTRIBUTING.md](./CONTRIBUTING.md)
for details, including the Contributor License Agreement (CLA) and the Microsoft Open Source
Code of Conduct.

## Security

To report a security issue, please follow the guidance in [SECURITY.md](./SECURITY.md). Do
**not** report security vulnerabilities through public GitHub issues.

## Support

See [SUPPORT.md](./SUPPORT.md) for how to file issues and get help.

## License

This project is licensed under the [MIT License](./LICENSE).

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized
use of Microsoft trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause
confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are
subject to those third-party's policies.
