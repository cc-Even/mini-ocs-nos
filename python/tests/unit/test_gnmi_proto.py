from gnmi_server.proto import gnmi_pb2


def test_official_gnmi_service_descriptor_is_available() -> None:
    service = gnmi_pb2.DESCRIPTOR.services_by_name["gNMI"]

    assert service.full_name == "gnmi.gNMI"
    assert [method.name for method in service.methods] == [
        "Capabilities",
        "Get",
        "Set",
        "Subscribe",
    ]
    assert gnmi_pb2.DESCRIPTOR.GetOptions().Extensions[gnmi_pb2.gnmi_service] == "0.10.0"
