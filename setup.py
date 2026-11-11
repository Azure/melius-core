import os
from setuptools import setup, find_packages
from __version__ import __version__

dir = os.path.dirname(__file__)
reqfile = os.path.join(dir, 'requirements.in')


def _parse_requirements(path):
    """Parse a requirements file into a list of requirement specifiers.

    Skips blank lines and comments (both full-line and inline; inline
    comments must be preceded by whitespace, matching pip semantics).
    """
    requirements = []
    with open(path, encoding='utf-8') as fp:
        for raw in fp:
            line = raw.strip()
            if not line or line.startswith('#'):
                continue
            if ' #' in line:
                line = line[:line.index(' #')].strip()
            if line:
                requirements.append(line)
    return requirements


install_requires = _parse_requirements(reqfile)

setup(
    name="melius-core",
    version=__version__,
    description="Core Melius framework for Azure Databricks pipelines - session management, "
                "incremental processing, Kusto/DeltaLake/ADLS connectors, and telemetry. "
                "Lightweight and reusable; extension packages build on top.",
    author="Brain Reasoning Intake & Experiences4",
    author_email="brie@microsoft.com",
    include_package_data=True,
    packages=find_packages(dir, exclude=['tests', 'tests.*']),
    python_requires=">=3.8",
    install_requires=install_requires,  # external packages as dependencies
)
