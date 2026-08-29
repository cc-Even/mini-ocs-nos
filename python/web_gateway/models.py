"""Stable version-one HTTP request models for the browser gateway."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

SafeIdentifier = Annotated[
    str,
    StringConstraints(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$"),
]
PortId = Annotated[int, Field(ge=1, le=2**31 - 1)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ConnectionWrite(StrictModel):
    input_port: PortId = Field(alias="input-port")
    output_port: PortId = Field(alias="output-port")
    operation: Literal["UPDATE", "REPLACE"] = "UPDATE"


class ConnectionBatchItem(StrictModel):
    id: SafeIdentifier
    input_port: PortId = Field(alias="input-port")
    output_port: PortId = Field(alias="output-port")


class ConnectionBatchWrite(StrictModel):
    operation: Literal["UPDATE", "REPLACE"] = "UPDATE"
    connections: list[ConnectionBatchItem] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def unique_ids(self) -> ConnectionBatchWrite:
        ids = [connection.id for connection in self.connections]
        if len(ids) != len(set(ids)):
            raise ValueError("connection ids must be unique within a batch")
        return self


class FaultWrite(StrictModel):
    fault: Literal[
        "NEXT_APPLY_TIMEOUT",
        "NEXT_APPLY_ERROR",
        "INPUT_PORT_DOWN",
        "OUTPUT_PORT_DOWN",
    ]
    port: PortId | None = None

    @model_validator(mode="after")
    def validate_port(self) -> FaultWrite:
        port_fault = self.fault in {"INPUT_PORT_DOWN", "OUTPUT_PORT_DOWN"}
        if port_fault != (self.port is not None):
            requirement = "requires" if port_fault else "does not accept"
            raise ValueError(f"{self.fault} {requirement} port")
        return self

    @property
    def fault_id(self) -> str:
        base = self.fault.lower().replace("_", "-")
        return f"{base}-{self.port}" if self.port is not None else base
