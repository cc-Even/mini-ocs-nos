"""Stable application imports for the vendored official gNMI bindings."""

from github.com.openconfig.gnmi.proto.gnmi import gnmi_pb2, gnmi_pb2_grpc
from github.com.openconfig.gnmi.proto.gnmi_ext import gnmi_ext_pb2

__all__ = ["gnmi_ext_pb2", "gnmi_pb2", "gnmi_pb2_grpc"]
