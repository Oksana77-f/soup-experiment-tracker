export type Metric = { seq: number; step: number; name: string; value: number };
export type Run = { id: string; name: string };
export type Snapshot = Run & { last_seq: number; points: Metric[] };
export type Project = { project: string; runs: Run[] };

export class RequestError extends Error {
  constructor(readonly status: number) {
    super("Request failed (" + status + ")");
  }
}

export async function request(path: string, options: RequestInit = {}) {
  const response = await fetch(path, { ...options, cache: "no-store" });
  if (!response.ok) throw new RequestError(response.status);
  return response;
}

export function readMetric(event: MessageEvent): Metric {
  const point = JSON.parse(event.data);
  if (!point || !Number.isSafeInteger(point.seq) || point.seq < 1 ||
      String(point.seq) !== event.lastEventId || !Number.isSafeInteger(point.step) ||
      point.step < 0 || typeof point.name !== "string" || !Number.isFinite(point.value)) {
    throw new Error("Invalid metric event");
  }
  return point;
}
