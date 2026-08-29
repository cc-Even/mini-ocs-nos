// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import { GatewayApi, GatewayError, decodeEvent } from "./api";

afterEach(() => vi.unstubAllGlobals());

describe("GatewayApi", () => {
  it("sends a bounded native connection request", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ operations: [{ operation: "UPDATE" }] }), {
        status: 202,
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await new GatewayApi("ocs0").writeConnection("web-1", 3, 11, "REPLACE");

    expect(fetchMock).toHaveBeenCalledOnce();
    const [path, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(path).toBe("/api/v1/devices/ocs0/connections/web-1");
    expect(init.method).toBe("PUT");
    expect(JSON.parse(String(init.body))).toEqual({
      "input-port": 3,
      "output-port": 11,
      operation: "REPLACE",
    });
    expect(init.signal).toBeInstanceOf(AbortSignal);
  });

  it("preserves the stable gateway error code", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: { code: "UNAVAILABLE", message: "gNMI offline", retryable: true },
          }),
          { status: 503, headers: { "content-type": "application/json" } },
        ),
      ),
    );

    await expect(new GatewayApi().snapshot()).rejects.toEqual(
      new GatewayError("UNAVAILABLE", "gNMI offline", 503),
    );
  });
});

describe("decodeEvent", () => {
  it("accepts typed gateway events and rejects malformed JSON shapes", () => {
    expect(decodeEvent('{"type":"sync","sync-response":true}').type).toBe("sync");
    expect(() => decodeEvent("[]")).toThrow("Invalid gateway event");
  });
});
