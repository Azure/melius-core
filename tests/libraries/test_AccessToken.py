import types
import sys
import pytest
from types import SimpleNamespace

# Pre-seed fake external modules to avoid hard dependency during import
if "azure" not in sys.modules:
    azure = types.ModuleType("azure")
    sys.modules["azure"] = azure
    identity = types.ModuleType("azure.identity")

    # minimal classes used by AccessToken; will be patched in tests
    class DefaultAzureCredential:  # pragma: no cover - replaced by patch
        def get_token(self, scope):
            return SimpleNamespace(token="t")

    class ManagedIdentityCredential:  # pragma: no cover - replaced by patch
        def __init__(self, client_id=None):
            pass

    class CertificateCredential:  # pragma: no cover - replaced by patch
        def __init__(self, tenant_id, client_id, certificate_data, send_certificate_chain=False):
            pass

    class ClientAssertionCredential:  # pragma: no cover - replaced by patch
        def __init__(self, tenant_id, client_id, func):
            pass

    def get_bearer_token_provider(cred, scope):  # pragma: no cover
        return lambda: "token"

    identity.DefaultAzureCredential = DefaultAzureCredential
    identity.ManagedIdentityCredential = ManagedIdentityCredential
    identity.CertificateCredential = CertificateCredential
    identity.ClientAssertionCredential = ClientAssertionCredential
    identity.get_bearer_token_provider = get_bearer_token_provider
    sys.modules["azure.identity"] = identity

if "jwt" not in sys.modules:
    jwt = types.ModuleType("jwt")

    def _decode(token, options=None):
        return {"tid": "t", "appid": "c"}

    jwt.decode = _decode
    sys.modules["jwt"] = jwt


def _patch_jwt_decode(mocker, tid="tenant-id", appid="client-id", azp=None):
    # Patch jwt.decode to avoid real JWT parsing
    def _fake_decode(token, options=None):
        data = {"tid": tid}
        if azp is not None:
            data["azp"] = azp
        else:
            data["appid"] = appid
        return data

    return mocker.patch("melius.libraries.AccessToken.jwt.decode", side_effect=_fake_decode)


@pytest.fixture
def AccessToken():
    from melius.libraries.AccessToken import AccessToken as _AT
    # ensure a clean global config per test
    _AT.global_config = None
    return _AT


def test_getSafeScope_defaults_and_appends(AccessToken):
    assert AccessToken.getSafeScope(None) == AccessToken.defaultScope
    assert AccessToken.getSafeScope("https://graph.microsoft.com") == "https://graph.microsoft.com/.default"
    assert AccessToken.getSafeScope("https://graph.microsoft.com/.default") == "https://graph.microsoft.com/.default"


def test__getCredential_default_uses_DefaultAzureCredential(mocker, AccessToken):
    # Arrange
    fake_cred = object()
    mocker.patch("melius.libraries.AccessToken.DefaultAzureCredential", return_value=fake_cred)
    # Avoid printing account info
    mocker.patch("melius.libraries.AccessToken.AccessToken._printAccountInfo", return_value=("t","c"))

    at = AccessToken(scope=None, config=None)

    # Act
    cred = at._getCredential()

    # Assert
    assert cred is fake_cred


def test__getCredential_msi_uses_ManagedIdentityCredential(mocker, AccessToken):
    from melius.libraries.Constants import KnownStrings

    created = {}

    class FakeMICred:
        def __init__(self, client_id=None):
            created["client_id"] = client_id

    mocker.patch("melius.libraries.AccessToken.ManagedIdentityCredential", FakeMICred)
    mocker.patch("melius.libraries.AccessToken.AccessToken._printAccountInfo", return_value=("t","c"))

    cfg = {KnownStrings.AccessType: KnownStrings.AccessType_MSI, KnownStrings.Config_ClientId: "abc"}
    at = AccessToken(scope=None, config=cfg)
    cred = at._getCredential()

    assert isinstance(cred, FakeMICred)
    assert created["client_id"] == "abc"


def test__getCredential_appid_uses_CertificateCredential(mocker, AccessToken):
    from melius.libraries.Constants import KnownStrings

    captured = {}

    class FakeCertCred:
        def __init__(self, tenant_id, client_id, certificate_data, send_certificate_chain=False):
            captured.update({
                "tenant_id": tenant_id,
                "client_id": client_id,
                "certificate_data": certificate_data,
                "send_certificate_chain": send_certificate_chain,
            })

    class FakeSecrets:
        def get(self, scope, key):
            assert scope == "myscope" and key == "mycert"
            return "PEM\\nLINE2"

    class FakeDBUtils:
        secrets = FakeSecrets()

    mocker.patch("melius.libraries.AccessToken.CertificateCredential", FakeCertCred)
    mocker.patch("melius.libraries.DatabricksHelper.DatabricksHelper.getDBUtils", return_value=FakeDBUtils())
    mocker.patch("melius.libraries.AccessToken.AccessToken._printAccountInfo", return_value=("t","c"))

    cfg = {
        KnownStrings.AccessType: KnownStrings.AccessType_AppId,
        KnownStrings.Config_TenantId: "ten",
        KnownStrings.Config_ClientId: "cli",
        KnownStrings.Config_Scope: "myscope",
        KnownStrings.Config_Cert: "mycert",
    }
    at = AccessToken(scope=None, config=cfg)
    _ = at._getCredential()

    assert captured["tenant_id"] == "ten"
    assert captured["client_id"] == "cli"
    assert captured["send_certificate_chain"] is True
    # certificate bytes should have real newlines
    assert captured["certificate_data"] == b"PEM\nLINE2".replace(b"\\n", b"\n")


def test_getTenantIdAndClientId_uses_decoded_token(mocker, AccessToken):
    _patch_jwt_decode(mocker, tid="tid-x", azp="client-x")

    class FakeCred:
        def get_token(self, scope):
            return SimpleNamespace(token="dummy")

    at = AccessToken(scope=None, config=None)
    mocker.patch("melius.libraries.AccessToken.DefaultAzureCredential", return_value=FakeCred())

    tid, cid = at.getTenantIdAndClientId()
    assert tid == "tid-x"
    assert cid == "client-x"


def test_getAccessToken_and_getClientId_set_from_refresh(mocker, AccessToken):
    _patch_jwt_decode(mocker, tid="t1", appid="cid-123")

    class FakeCred:
        def get_token(self, scope):
            return SimpleNamespace(token="dummy")

    at = AccessToken(scope=None, config=None)
    # ensure getCredential returns FakeCred instance
    mocker.patch.object(at, "getCredential", return_value=FakeCred())

    tok = at.getAccessToken()
    assert isinstance(tok, str)
    assert at.getClientId() == "cid-123"


def test_setAccessConfig_validates_input(mocker, AccessToken):
    with pytest.raises(ValueError):
        AccessToken.setAccessConfig(None)

    AccessToken.setAccessConfig({"x": 1})
    assert AccessToken.global_config == {"x": 1}


def test__getCredential_fic_uses_ClientAssertionCredential(mocker, AccessToken):
    """FIC accessType creates a ClientAssertionCredential via MI token exchange."""
    from melius.libraries.Constants import KnownStrings

    captured = {}

    class FakeMICred:
        def get_token(self, scope):
            return SimpleNamespace(token="fake-mi-token")

    class FakeClientAssertionCred:
        def __init__(self, tenant_id, client_id, func):
            captured["tenant_id"] = tenant_id
            captured["client_id"] = client_id
            captured["func"] = func

    mocker.patch("melius.libraries.AccessToken.ManagedIdentityCredential", FakeMICred)
    mocker.patch("melius.libraries.AccessToken.ClientAssertionCredential", FakeClientAssertionCred)
    mocker.patch("melius.libraries.AccessToken.AccessToken._printAccountInfo", return_value=("t", "c"))

    cfg = {
        KnownStrings.AccessType: KnownStrings.AccessType_FIC,
        KnownStrings.Config_TenantId: "fic-tenant-123",
        KnownStrings.Config_ClientId: "fic-client-456",
    }
    at = AccessToken(scope=None, config=cfg)
    cred = at._getCredential()

    assert isinstance(cred, FakeClientAssertionCred)
    assert captured["tenant_id"] == "fic-tenant-123"
    assert captured["client_id"] == "fic-client-456"
    # The func should return the MI token for api://AzureADTokenExchange
    assert callable(captured["func"])
    assert captured["func"]() == "fake-mi-token"


def test__getCredential_fic_via_global_config(mocker, AccessToken):
    """FIC works when accessConfig comes from AccessToken.global_config fallback."""
    from melius.libraries.Constants import KnownStrings

    class FakeMICred:
        def get_token(self, scope):
            return SimpleNamespace(token="mi-token")

    class FakeClientAssertionCred:
        def __init__(self, tenant_id, client_id, func):
            self.tenant_id = tenant_id
            self.client_id = client_id

    mocker.patch("melius.libraries.AccessToken.ManagedIdentityCredential", FakeMICred)
    mocker.patch("melius.libraries.AccessToken.ClientAssertionCredential", FakeClientAssertionCred)
    mocker.patch("melius.libraries.AccessToken.AccessToken._printAccountInfo", return_value=("t", "c"))

    # Set global config to FIC
    AccessToken.setAccessConfig({
        KnownStrings.AccessType: KnownStrings.AccessType_FIC,
        KnownStrings.Config_TenantId: "global-tenant",
        KnownStrings.Config_ClientId: "global-client",
    })

    # Create AccessToken with NO config -- should use global_config
    at = AccessToken(scope="https://kusto.kusto.windows.net/.default", config=None)
    cred = at._getCredential()

    assert isinstance(cred, FakeClientAssertionCred)
    assert cred.tenant_id == "global-tenant"
    assert cred.client_id == "global-client"


def test__getCredential_invalid_access_type_raises(mocker, AccessToken):
    """Invalid accessType raises ValueError."""
    from melius.libraries.Constants import KnownStrings

    cfg = {KnownStrings.AccessType: "INVALID_TYPE"}
    at = AccessToken(scope=None, config=cfg)

    with pytest.raises(ValueError, match="Invalid access configuration"):
        at._getCredential()
