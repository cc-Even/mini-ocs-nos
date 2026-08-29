import {
  type ConnectionView,
  type DashboardSnapshot,
  type PortView,
  connectionStatus,
  isActual,
  numberField,
  stringField,
} from "./types";

const SVG_NS = "http://www.w3.org/2000/svg";
const PORT_COUNT = 16;
const ORIGIN = 76;
const CELL = 31;

function svgElement<K extends keyof SVGElementTagNameMap>(
  name: K,
  attributes: Record<string, string>,
): SVGElementTagNameMap[K] {
  const element = document.createElementNS(SVG_NS, name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
  return element;
}

function portUnavailable(ports: PortView[], id: number): boolean {
  const port = ports.find((candidate) => Number(candidate.id) === id);
  return port !== undefined && stringField(port, "oper-status") !== "UP";
}

function desiredAt(connections: ConnectionView[], input: number, output: number): ConnectionView | undefined {
  return connections.find(
    (connection) =>
      numberField(connection.config, "input-port") === input &&
      numberField(connection.config, "output-port") === output,
  );
}

function actualAt(connections: ConnectionView[], input: number, output: number): ConnectionView | undefined {
  return connections.find(
    (connection) =>
      isActual(connection) &&
      numberField(connection.state, "input-port") === input &&
      numberField(connection.state, "output-port") === output,
  );
}

export function renderMatrix(svg: SVGSVGElement, snapshot: DashboardSnapshot): void {
  svg.replaceChildren();
  const connections = snapshot.connections.connection;
  const inputs = snapshot.ports["input-port"];
  const outputs = snapshot.ports["output-port"];

  svg.append(
    svgElement("text", { x: "300", y: "22", class: "axis-title" }),
    svgElement("text", {
      x: "18",
      y: "335",
      class: "axis-title vertical-title",
      transform: "rotate(-90 18 335)",
    }),
  );
  const labels = svg.querySelectorAll(".axis-title");
  if (labels[0]) labels[0].textContent = "OUTPUT PORTS";
  if (labels[1]) labels[1].textContent = "INPUT PORTS";

  for (let index = 1; index <= PORT_COUNT; index += 1) {
    const x = ORIGIN + (index - 1) * CELL + CELL / 2;
    const y = ORIGIN + (index - 1) * CELL + CELL / 2;
    const outputLabel = svgElement("text", {
      x: String(x),
      y: "61",
      class: portUnavailable(outputs, index) ? "port-label unavailable" : "port-label",
      "text-anchor": "middle",
    });
    outputLabel.textContent = String(index).padStart(2, "0");
    const inputLabel = svgElement("text", {
      x: "60",
      y: String(y + 4),
      class: portUnavailable(inputs, index) ? "port-label unavailable" : "port-label",
      "text-anchor": "end",
    });
    inputLabel.textContent = String(index).padStart(2, "0");
    svg.append(outputLabel, inputLabel);
  }

  for (let input = 1; input <= PORT_COUNT; input += 1) {
    for (let output = 1; output <= PORT_COUNT; output += 1) {
      const desired = desiredAt(connections, input, output);
      const actual = actualAt(connections, input, output);
      const unavailable = portUnavailable(inputs, input) || portUnavailable(outputs, output);
      const classes = ["matrix-cell"];
      if (unavailable) classes.push("port-unavailable");
      if (desired) classes.push("desired");
      if (actual) classes.push("actual");
      if (desired && actual) classes.push("converged");
      const cell = svgElement("rect", {
        x: String(ORIGIN + (output - 1) * CELL + 2),
        y: String(ORIGIN + (input - 1) * CELL + 2),
        width: String(CELL - 4),
        height: String(CELL - 4),
        rx: "4",
        class: classes.join(" "),
        "data-input": String(input),
        "data-output": String(output),
        "aria-label": `Input ${input} to output ${output}`,
      });
      const title = svgElement("title", {});
      const connection = actual ?? desired;
      title.textContent = connection
        ? `${connection.id}: ${connectionStatus(connection)}`
        : `Input ${input} → output ${output}`;
      cell.append(title);
      svg.append(cell);
    }
  }
}
