

class RegionMapping(object):
    """Region mapping class to handle region-related operations."""

    # Mapping of Azure core clusters to their respective regions.
    # Derived from telemetry: for each cluster, the distinct set of regions
    # observed over a recent time window.

    azCoreGeoToRegions = {
        "azcore1.southeastasia": ["southeastasia", "qatarcentral", "malaysiawest", "chilecentral", "taiwannorth", "malaysiasouth", "indonesiacentral", "newzealandnorth", "taiwannorthwest", "austriaeast", "southwestus", "southeastus3", "belgiumcentral", "southeastus", "southcentralus2", "israelnorthwest"],
        "azcore2.australiaeast": ["australiaeast", "australiasoutheast", "australiacentral", "australiacentral2"],
        "azcore3.brazilsouth": ["brazilsouth", "southafricanorth", "southafricawest", "brazilsoutheast"],
        "azcore4.canadacentral": ["canadacentral", "canadaeast"],
        "azcore5.northeurope": ["northeurope", "swedencentral", "spaincentral", "norwayeast", "swedensouth", "polandcentral", "norwaywest"],
        "azcore6.westeurope": ["westeurope", "switzerlandnorth", "denmarkeast", "switzerlandwest"],
        "azcore7.francecentral": ["francecentral", "germanywestcentral", "italynorth", "germanynorth", "israelcentral", "francesouth"],
        "azcore8.japaneast": ["japaneast", "japanwest", "koreacentral", "koreasouth"],
        "azcore9.uksouth": ["uksouth", "ukwest"],
        "azcore10.centralus": ["centralus", "northcentralus", "centraluseuap", "southeastus5"],
        "azcore11.southcentralus": ["southcentralus", "mexicocentral", "southcentralusstg"],
        "azcore12.eastus": ["eastus", "eastusstg", "eastusslv"],
        "azcore13.eastus2": ["eastus2", "eastus2euap"],
        "azcore14.westus2": ["westus2", "westcentralus", "westus"],
        "azcore15.westus3": ["westus3"],
        "azcore16.eastasia": ["eastasia"],
        "azcore17.centralindia": ["centralindia", "uaenorth", "southindia", "jioindiacentral", "jioindiawest", "uaecentral", "westindia"]
    }

    # Mapping of Azure regions to their corresponding internal region names (name mapping only).
    # This mapping should be updated whenever new regions are added to Azure,
    # or consider automating this mapping process.
    regionToInternalRegion = {
        "eastasia": "asiaeast",
        "southeastasia": "asiasoutheast",
        "centralus": "uscentral",
        "eastus": "useast",
        "eastus2": "useast2",
        "westus": "uswest",
        "northcentralus": "usnorth",
        "southcentralus": "ussouth",
        "northeurope": "europenorth",
        "westeurope": "europewest",
        "japanwest": "japanwest",
        "japaneast": "japaneast",
        "brazilsouth": "brazilsouth",
        "australiaeast": "australiaeast",
        "australiasoutheast": "australiasoutheast",
        "southindia": "indiasouth",
        "centralindia": "indiacentral",
        "westindia": "indiawest",
        "canadacentral": "canadacentral",
        "canadaeast": "canadaeast",
        "uksouth": "uksouth",
        "ukwest": "ukwest",
        "westus2": "uswest2",
        "koreacentral": "koreacentral",
        "koreasouth": "koreasouth",
        "francecentral": "francec",
        "centraluseuap": "uscentraleuap",
        "eastus2euap": "useast2euap",
        "australiacentral": "australiac",
        "australiacentral2": "australiac2",
        "francesouth": "frances",
        "germanynorth": "germanyn",
        "germanywestcentral": "germanywc",
        "norwayeast": "norwaye",
        "norwaywest": "norwayw",
        "southafricanorth": "southafrican",
        "southafricawest": "southafricaw",
        "switzerlandnorth": "switzerlandn",
        "switzerlandwest": "switzerlandw",
        "uaecentral": "uaec",
        "uaenorth": "uaen",
        "westcentralus": "uswestcentral",
        "usgovvirginia": "usgoveast",
        "usdodcentral": "usdodcentral",
        "usdodeast": "usdodeast",
        "usgovarizona": "usgovsw",
        "usgoviowa": "usgovcentral",
        "usgovtexas": "usgovsc",
        "westus3": "uswest3",
        "swedencentral": "swedenc",
        "chinaeast2": "chinae2",
        "usseceast": "ussece",
        "usnatwest": "usnatw",
        "chinaeast": "chinaeast",
        "chinanorth": "chinanorth",
        "germanycentral": "germanycentral",
        "germanynortheast": "germanynortheast",
        "uknorth": "uknorth",
        "uksouth2": "uksouth2",
        "ussecwest": "ussecw",
        "usnateast": "usnate",
        "chinanorth2": "chinan2",
        "eastusstg": "usstagee",
        "southcentralusstg": "usstagesc",
        "swedensouth": "swedens",
        "brazilsoutheast": "brazilse",
        "jioindiawest": "jioinw",
        "jioindiacentral": "jioinc",
        "qatarcentral": "qatarc",
        "chinaeast3": "chinae3",
        "chinanorth3": "chinan3",
        "eastusslv": "eusslv",
        "polandcentral": "polandc",
        "taiwannorth": "taiwann",
        "taiwannorthwest": "taiwannw",
        "malaysiasouth": "malaysias",
        "israelcentral": "israelc",
        "italynorth": "italyn",
        "mexicocentral": "mexicoc",
        "spaincentral": "spainc",
        "malaysiawest": "malaysiaw",
        "newzealandnorth": "newzealandn",
        "chilecentral": "chilec",
        "usdodsouthwest": "usdodsw",
        "usdodsouthcentral": "usdodsc",
        "DevFabric": "DevFabric",
        "apacsoutheast2": "asiasoutheast2",
        "arlem": "arlem",
        "austriaeast": "austriae",
        "belgiumcentral": "belgiumc",
        "bleufrancecentral": "bleuc",
        "bleufrancesouth": "bleus",
        "brazilnortheast": "brazilne",
        "chilenorthcentral": "chilenc",
        "chinanorth10": "chinan10",
        "deloscloudgermanycentral": "delosc",
        "deloscloudgermanynorth": "delosn",
        "denmarkeast": "denmarke",
        "easteurope": "europeeast",
        "finlandcentral": "finlandc",
        "indiasouthcentral": "indiasc",
        "indonesiacentral": "indonesiac",
        "israelnorthwest": "israelnw",
        "koreasouth2": "koreas2",
        "northeurope2": "europenorth2",
        "ocave": "ocave",
        "singaporecentral": "singaporec",
        "singaporegov": "singaporegov",
        "singaporenorth": "singaporen",
        "southeastus": "ussoutheast",
        "usdodwestcentral": "usdodwc",
        "usgovwyoming": "usgovwc",
        "usnatnorth": "usnatn",
        "usseccentral": "ussecc",
        "ussecwestcentral": "ussecwc",
        "southcentralus2": "ussouth2",
    }

    @staticmethod
    def getRegionsOfAzcoreGeo(clusterName: str) -> list:
        """Get regions of an Azure core geo cluster."""
        if not clusterName:
            return []

        if clusterName.lower() in RegionMapping.azCoreGeoToRegions:
            return RegionMapping.azCoreGeoToRegions[clusterName.lower()]
        else:
            raise ValueError(f"AzCore geo cluster name: '{clusterName}' not found in azCoreToRegions mapping.")

    @staticmethod
    def getInternalRegionsOfAzcoreGeo(clusterName: str) -> list:
        """Get regions of an Azure core geo cluster."""
        if not clusterName:
            return []

        regions = RegionMapping.azCoreGeoToRegions[clusterName.lower()]
        return [RegionMapping.regionToInternalRegion.get(region, region) for region in regions]
