def test_BaseConnector_authMode_roundtrip():
    from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes

    # default
    assert BaseConnector.getAuthMode() == ConnectorAuthModes.ManagedIdentiy

    BaseConnector.setAuthMode(ConnectorAuthModes.InterActive)
    assert BaseConnector.getAuthMode() == ConnectorAuthModes.InterActive

    BaseConnector.setAuthMode(ConnectorAuthModes.Sni)
    assert BaseConnector.getAuthMode() == ConnectorAuthModes.Sni


def test_BaseConnector_getDefaultFileType_and_config_override():
    from melius.connectors.BaseConnector import BaseConnector
    from melius.libraries.Constants import FileTypes

    base = BaseConnector()

    assert base.getDefaultFileType() == FileTypes.Unknown

    cfg = {"DefaultFileType": "CSV"}
    assert base._getFileTypeFromConfig(cfg, FileTypes.Parquet) == FileTypes.CSV

    # without config key returns provided default
    assert base._getFileTypeFromConfig({}, FileTypes.Parquet) == FileTypes.Parquet
