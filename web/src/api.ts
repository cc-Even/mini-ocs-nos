import type { DashboardSnapshot, GatewayEvent } from "./types";

interface ErrorEnvelope {
  error?: { code?: string; message?: string; retryable?: boolean };
}

export class GatewayError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly status: number,
  ) {
    super(message);
  }
}

export class GatewayApi {
  constructor(
    private readonly device = "ocs0",
    private readonly timeoutMs = 5_000,
  ) {}

  private async request(path: string, init?: RequestInit): Promise<Record<string, unknown>> {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const response = await fetch(path, {
        ...init,
        headers: { "content-type": "application/json", ...init?.headers },
        signal: controller.signal,
      });
      const payload = (await response.json()) as Record<string, unknown> & ErrorEnvelope;
      if (!response.ok) {
        throw new GatewayError(
          payload.error?.code ?? `HTTP_${response.status}`,
          payload.error?.message ?? "Gateway request failed",
          response.status,
        );
      }
      return payload;
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") {
        throw new GatewayError("DEADLINE_EXCEEDED", "Gateway request timed out", 504);
      }
      throw error;
    } finally {
      window.clearTimeout(timer);
    }
  }

  async snapshot(): Promise<DashboardSnapshot> {
    return (await this.request(`/api/v1/devices/${this.device}/snapshot`)) as unknown as DashboardSnapshot;
  }

  async writeConnection(
    connectionId: string,
    inputPort: number,
    outputPort: number,
    operation: "UPDATE" | "REPLACE",
  ): Promise<void> {
    await this.request(`/api/v1/devices/${this.device}/connections/${connectionId}`, {
      method: "PUT",
      body: JSON.stringify({
        "input-port": inputPort,
        "output-port": outputPort,
        operation,
      }),
    });
  }

  async deleteConnection(connectionId: string): Promise<void> {
    await this.request(`/api/v1/devices/${this.device}/connections/${connectionId}`, {
      method: "DELETE",
    });
  }

  async injectFault(fault: string, port?: number): Promise<void> {
    await this.request(`/api/v1/devices/${this.device}/faults`, {
      method: "POST",
      body: JSON.stringify(port === undefined ? { fault } : { fault, port }),
    });
  }

  async clearFaults(): Promise<void> {
    await this.request(`/api/v1/devices/${this.device}/faults`, { method: "DELETE" });
  }

  eventStream(): WebSocket {
    const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
    return new WebSocket(
      `${scheme}//${window.location.host}/api/v1/devices/${this.device}/events?duration_seconds=300`,
    );
  }
}

export function decodeEvent(raw: string): GatewayEvent {
  const value = JSON.parse(raw) as unknown;
  if (typeof value !== "object" || value === null || !("type" in value)) {
    throw new Error("Invalid gateway event");
  }
  return value as GatewayEvent;
}
