export type NativeRecord = Record<string, unknown>;

export interface ConnectionView {
  id: string;
  config?: NativeRecord;
  state?: NativeRecord;
}

export interface PortView extends NativeRecord {
  id: number;
}

export interface DashboardSnapshot {
  device: { name: string; config?: NativeRecord; state?: NativeRecord };
  ports: { "input-port": PortView[]; "output-port": PortView[] };
  connections: { connection: ConnectionView[] };
  alarms: { alarm: NativeRecord[] };
  counters: NativeRecord;
  diagnostics: NativeRecord;
}

export interface GatewayEvent extends NativeRecord {
  type: "ready" | "sync" | "update" | "reconnecting" | "error";
  path?: string;
  value?: NativeRecord;
  deleted?: boolean;
}

export function numberField(record: NativeRecord | undefined, name: string): number | null {
  const value = record?.[name];
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && /^\d+$/.test(value)) return Number(value);
  return null;
}

export function stringField(record: NativeRecord | undefined, name: string): string {
  const value = record?.[name];
  return typeof value === "string" ? value : "";
}

export function booleanField(record: NativeRecord | undefined, name: string): boolean {
  return record?.[name] === true || record?.[name] === "true";
}

export function isActual(connection: ConnectionView): boolean {
  if ("actual-present" in (connection.state ?? {})) {
    return booleanField(connection.state, "actual-present");
  }
  return (numberField(connection.state, "applied-version") ?? 0) > 0;
}

export function connectionStatus(connection: ConnectionView): string {
  return stringField(connection.state, "apply-status") || "PENDING";
}

export function safeArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}
