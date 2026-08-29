import "./styles.css";

import { GatewayApi, GatewayError, decodeEvent } from "./api";
import { renderMatrix } from "./matrix";
import {
  type ConnectionView,
  type DashboardSnapshot,
  type GatewayEvent,
  type NativeRecord,
  type PortView,
  connectionStatus,
  isActual,
  numberField,
  safeArray,
  stringField,
} from "./types";

function required<T extends Element>(selector: string): T {
  const element = document.querySelector<T>(selector);
  if (!element) throw new Error(`Missing dashboard element ${selector}`);
  return element;
}

function text(selector: string, value: unknown): void {
  required<HTMLElement>(selector).textContent = String(value);
}

function displayKey(value: string): string {
  return value.replaceAll("-", " ").replace(/\b\w/g, (character) => character.toUpperCase());
}

function renderDataGrid(element: HTMLElement, record: NativeRecord): void {
  const entries = Object.entries(record).filter(([, value]) => typeof value !== "object");
  element.replaceChildren(
    ...entries.flatMap(([key, value]) => {
      const term = document.createElement("dt");
      term.textContent = displayKey(key);
      const description = document.createElement("dd");
      description.textContent = String(value);
      return [term, description];
    }),
  );
}

function renderPorts(element: HTMLElement, ports: PortView[]): void {
  element.replaceChildren(
    ...ports.map((port) => {
      const card = document.createElement("article");
      const status = stringField(port, "oper-status") || "UNKNOWN";
      const safeStatus = status.replace(/[^A-Za-z0-9_-]/g, "").toLowerCase();
      card.className = `port-card status-${safeStatus}`;
      card.dataset.port = String(port.id);
      const power = port["optical-power-dbm"];
      const identifier = document.createElement("strong");
      identifier.textContent = String(port.id).padStart(2, "0");
      const state = document.createElement("span");
      state.textContent = status;
      const opticalPower = document.createElement("small");
      opticalPower.textContent = typeof power === "number" ? `${power.toFixed(1)} dBm` : "—";
      card.append(identifier, state, opticalPower);
      return card;
    }),
  );
}

function endpoint(record: NativeRecord | undefined): string {
  const input = numberField(record, "input-port");
  const output = numberField(record, "output-port");
  return input !== null && output !== null ? `${input} → ${output}` : "—";
}

class Dashboard {
  private readonly api = new GatewayApi("ocs0");
  private snapshot: DashboardSnapshot | null = null;
  private websocket: WebSocket | null = null;
  private reconnectCount = 0;
  private refreshTimer: number | null = null;

  async start(): Promise<void> {
    this.bindControls();
    await this.refresh();
    this.connectEvents();
  }

  private bindControls(): void {
    required<HTMLButtonElement>("#refresh-button").addEventListener("click", () => void this.refresh());
    required<HTMLFormElement>("#connection-form").addEventListener("submit", (event) => {
      event.preventDefault();
      void this.submitConnection(new FormData(event.currentTarget as HTMLFormElement));
    });
    required<HTMLSelectElement>("#fault-kind").addEventListener("change", (event) => {
      const value = (event.currentTarget as HTMLSelectElement).value;
      required<HTMLInputElement>("#fault-port").disabled = !value.includes("PORT_DOWN");
    });
    required<HTMLFormElement>("#fault-form").addEventListener("submit", (event) => {
      event.preventDefault();
      void this.submitFault(new FormData(event.currentTarget as HTMLFormElement));
    });
    required<HTMLButtonElement>("#fault-clear").addEventListener("click", () => void this.clearFaults());
  }

  private async refresh(): Promise<void> {
    try {
      this.snapshot = await this.api.snapshot();
      this.render();
      this.hideError();
    } catch (error) {
      this.showError(error);
    }
  }

  private render(): void {
    if (!this.snapshot) return;
    renderMatrix(required<SVGSVGElement>("#matrix"), this.snapshot);
    const diagnostics = this.snapshot.diagnostics;
    const counters = this.snapshot.counters;
    const deviceState = this.snapshot.device.state ?? {};
    text("#device-health", diagnostics["device-health"] ?? deviceState["oper-status"] ?? "UNKNOWN");
    text("#active-connections", counters["active-connections"] ?? 0);
    text(
      "#desired-actual",
      `${diagnostics["desired-connections"] ?? 0} / ${diagnostics["actual-connections"] ?? 0}`,
    );
    text("#active-alarms", diagnostics["active-alarms"] ?? 0);
    text("#config-revision", deviceState["config-revision"] ?? "—");
    this.renderConnections(this.snapshot.connections.connection);
    renderPorts(required("#input-ports"), this.snapshot.ports["input-port"]);
    renderPorts(required("#output-ports"), this.snapshot.ports["output-port"]);
    this.renderAlarms(this.snapshot.alarms.alarm);
    renderDataGrid(required("#counters"), counters);
    renderDataGrid(required("#diagnostics"), diagnostics);
  }

  private renderConnections(connections: ConnectionView[]): void {
    const body = required<HTMLTableSectionElement>("#connections-table tbody");
    body.replaceChildren(
      ...connections.map((connection) => {
        const row = document.createElement("tr");
        row.dataset.connectionId = connection.id;
        const applyStatus = connectionStatus(connection);
        const cells = Array.from({ length: 5 }, () => document.createElement("td"));
        const identifier = document.createElement("strong");
        identifier.textContent = connection.id;
        cells[0]?.append(identifier);
        if (cells[1]) cells[1].textContent = endpoint(connection.config);
        if (cells[2]) cells[2].textContent = isActual(connection) ? endpoint(connection.state) : "—";
        const chip = document.createElement("span");
        const safeStatus = applyStatus.replace(/[^A-Za-z0-9_-]/g, "").toLowerCase();
        chip.className = `status-chip status-${safeStatus}`;
        chip.textContent = applyStatus;
        cells[3]?.append(chip);
        const button = document.createElement("button");
        button.type = "button";
        button.className = "icon-button";
        button.textContent = "Delete";
        button.setAttribute("aria-label", `Delete ${connection.id}`);
        button.addEventListener("click", () => void this.deleteConnection(connection.id));
        cells[4]?.append(button);
        row.append(...cells);
        return row;
      }),
    );
  }

  private renderAlarms(alarms: NativeRecord[]): void {
    const container = required<HTMLElement>("#alarms");
    if (alarms.length === 0) {
      const empty = document.createElement("p");
      empty.className = "empty-state";
      empty.textContent = "No active alarms";
      container.replaceChildren(empty);
      return;
    }
    container.replaceChildren(
      ...alarms.map((alarm) => {
        const item = document.createElement("article");
        item.className = "alarm-item";
        const identifier = alarm.id ?? alarm["alarm-type"] ?? "alarm";
        const message = alarm.message ?? alarm["error-message"] ?? "Device attention required";
        const title = document.createElement("strong");
        title.textContent = String(identifier);
        const detail = document.createElement("span");
        detail.textContent = String(message);
        item.append(title, detail);
        return item;
      }),
    );
  }

  private async submitConnection(form: FormData): Promise<void> {
    const connectionId = String(form.get("connection-id") ?? "");
    const inputPort = Number(form.get("input-port"));
    const outputPort = Number(form.get("output-port"));
    const operation = String(form.get("operation")) as "UPDATE" | "REPLACE";
    await this.operation(
      `Submitting ${connectionId}…`,
      `Desired state accepted for ${connectionId}; awaiting confirmation.`,
      () => this.api.writeConnection(connectionId, inputPort, outputPort, operation),
    );
  }

  private async deleteConnection(connectionId: string): Promise<void> {
    await this.operation(
      `Deleting ${connectionId}…`,
      `Delete accepted for ${connectionId}.`,
      () => this.api.deleteConnection(connectionId),
    );
  }

  private async submitFault(form: FormData): Promise<void> {
    const fault = String(form.get("fault"));
    const rawPort = form.get("port");
    const port = rawPort && String(rawPort) ? Number(rawPort) : undefined;
    await this.operation(
      `Injecting ${fault}…`,
      `${fault} confirmed by the simulator.`,
      () => this.api.injectFault(fault, port),
    );
  }

  private async clearFaults(): Promise<void> {
    await this.operation("Clearing simulator faults…", "All simulator faults cleared.", () => this.api.clearFaults());
  }

  private async operation(pending: string, success: string, action: () => Promise<void>): Promise<void> {
    text("#operation-status", pending);
    try {
      await action();
      text("#operation-status", success);
      this.hideError();
      this.scheduleRefresh(75);
    } catch (error) {
      text("#operation-status", "Operation failed.");
      this.showError(error);
    }
  }

  private connectEvents(): void {
    this.websocket?.close();
    const websocket = this.api.eventStream();
    this.websocket = websocket;
    this.setStreamState("connecting", "Connecting telemetry");
    websocket.addEventListener("message", (event) => {
      try {
        this.handleEvent(decodeEvent(String(event.data)));
      } catch (error) {
        this.showError(error);
      }
    });
    websocket.addEventListener("close", () => {
      if (this.websocket !== websocket) return;
      this.setStreamState("offline", "Telemetry disconnected");
      if (this.reconnectCount >= 5) return;
      this.reconnectCount += 1;
      window.setTimeout(() => this.connectEvents(), Math.min(500 * 2 ** this.reconnectCount, 5_000));
    });
    websocket.addEventListener("error", () => this.setStreamState("offline", "Telemetry unavailable"));
  }

  private handleEvent(event: GatewayEvent): void {
    if (event.type === "ready" || event.type === "sync") {
      this.reconnectCount = 0;
      this.setStreamState("live", event.type === "sync" ? "Live · synchronized" : "Live · syncing");
      return;
    }
    if (event.type === "reconnecting") {
      this.setStreamState("connecting", `Gateway reconnect ${String(event.attempt ?? "")}`);
      return;
    }
    if (event.type === "error") {
      this.setStreamState("offline", "Telemetry dependency failed");
      this.showError(new Error("Telemetry dependency failed"));
      return;
    }
    if (event.type !== "update" || !this.snapshot || !event.path || !event.value) return;
    if (event.path.endsWith("/connections")) {
      this.snapshot.connections.connection = safeArray<ConnectionView>(event.value.connection);
    } else if (event.path.endsWith("/ports")) {
      this.snapshot.ports["input-port"] = safeArray<PortView>(event.value["input-port"]);
      this.snapshot.ports["output-port"] = safeArray<PortView>(event.value["output-port"]);
    } else if (event.path.endsWith("/alarms")) {
      this.snapshot.alarms.alarm = safeArray<NativeRecord>(event.value.alarm);
    }
    this.render();
    this.scheduleRefresh(200);
  }

  private scheduleRefresh(delay: number): void {
    if (this.refreshTimer !== null) window.clearTimeout(this.refreshTimer);
    this.refreshTimer = window.setTimeout(() => {
      this.refreshTimer = null;
      void this.refresh();
    }, delay);
  }

  private setStreamState(state: "live" | "connecting" | "offline", label: string): void {
    const dot = required<HTMLElement>("#stream-dot");
    dot.className = `status-dot ${state}`;
    text("#stream-status", label);
  }

  private showError(error: unknown): void {
    const banner = required<HTMLElement>("#error-banner");
    const message = error instanceof GatewayError ? `${error.code}: ${error.message}` : error instanceof Error ? error.message : "Unexpected dashboard error";
    banner.textContent = message;
    banner.classList.remove("hidden");
  }

  private hideError(): void {
    required<HTMLElement>("#error-banner").classList.add("hidden");
  }
}

void new Dashboard().start();
