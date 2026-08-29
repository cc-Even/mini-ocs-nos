// @vitest-environment jsdom

import { describe, expect, it } from "vitest";

import { renderMatrix } from "./matrix";
import type { DashboardSnapshot, PortView } from "./types";

function ports(): PortView[] {
  return Array.from({ length: 16 }, (_, index) => ({
    id: index + 1,
    "oper-status": index === 4 ? "DOWN" : "UP",
  }));
}

function snapshot(): DashboardSnapshot {
  return {
    device: { name: "ocs0" },
    ports: { "input-port": ports(), "output-port": ports() },
    connections: {
      connection: [
        {
          id: "active",
          config: { "input-port": 1, "output-port": 9 },
          state: {
            "input-port": 1,
            "output-port": 9,
            "actual-present": true,
            "apply-status": "ACTIVE",
          },
        },
        {
          id: "pending",
          config: { "input-port": 2, "output-port": 10 },
          state: { "apply-status": "APPLYING", "actual-present": false },
        },
      ],
    },
    alarms: { alarm: [] },
    counters: {},
    diagnostics: {},
  };
}

describe("renderMatrix", () => {
  it("renders all 256 crosspoints with desired, actual, and port state", () => {
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    renderMatrix(svg, snapshot());

    expect(svg.querySelectorAll(".matrix-cell")).toHaveLength(256);
    expect(svg.querySelector('[data-input="1"][data-output="9"]')?.classList).toContain(
      "converged",
    );
    expect(svg.querySelector('[data-input="2"][data-output="10"]')?.classList).toContain(
      "desired",
    );
    expect(svg.querySelector('[data-input="2"][data-output="10"]')?.classList).not.toContain(
      "actual",
    );
    expect(svg.querySelector('[data-input="5"][data-output="3"]')?.classList).toContain(
      "port-unavailable",
    );
    expect(svg.querySelector('[data-input="1"][data-output="5"]')?.classList).toContain(
      "port-unavailable",
    );
  });
});
