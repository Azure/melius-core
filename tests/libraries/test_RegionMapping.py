from melius.libraries.RegionMapping import RegionMapping
import pytest


def test_getListOfRegions():
    assert RegionMapping.getRegionsOfAzcoreGeo('azcore5.northeurope') == ["northeurope","swedencentral","spaincentral","norwayeast","swedensouth","polandcentral","norwaywest"]


def test_getListOfInternalRegions():
    assert RegionMapping.getInternalRegionsOfAzcoreGeo('azcore5.northeurope') == ['europenorth', 'swedenc', 'spainc', 'norwaye', 'swedens', 'polandc', 'norwayw']


def test_getListOfRegionsOfUnknownGeo():
    with pytest.raises(ValueError):
        RegionMapping.getRegionsOfAzcoreGeo('azcore18.unknown')
