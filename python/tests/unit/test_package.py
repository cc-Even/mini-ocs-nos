from gnmi_server import __version__ as server_version
from ocsctl import __version__ as client_version
from web_gateway import __version__ as gateway_version


def test_packages_share_project_version() -> None:
    assert server_version == "0.1.0"
    assert client_version == server_version
    assert gateway_version == server_version
