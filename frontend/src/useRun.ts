import { useEffect, useState } from "react";
import { readMetric, request, RequestError, type Snapshot } from "./api";

type Connection = "Loading" | "Live" | "Reconnecting" | "Offline";

export function useRun(id: string, signedOut: () => void) {
  const [run, setRun] = useState<Snapshot>();
  const [connection, setConnection] = useState<Connection>("Loading");
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    const path = "/runs/" + encodeURIComponent(id);
    let stream: EventSource | undefined;
    let timer: number | undefined;
    let lastApplied = 0;
    let retryMs = 500;

    function schedule() {
      if (signal.aborted || timer !== undefined) return;
      setConnection("Offline");
      timer = window.setTimeout(() => {
        timer = undefined;
        void checkAccess();
      }, retryMs);
      retryMs = Math.min(retryMs * 2, 8000);
    }

    async function checkAccess() {
      try {
        await request(path + "/access", { signal });
        if (!signal.aborted) connect();
      } catch (failure) {
        if (signal.aborted) return;
        const status = failure instanceof RequestError ? failure.status : 0;
        if (status === 401) signedOut();
        else if (status === 403 || status === 404 || status === 422) setError("Run unavailable.");
        else schedule();
      }
    }

    function connect() {
      const current = new EventSource(path + "/events?after=" + lastApplied);
      stream = current;
      current.onopen = () => {
        retryMs = 500;
        setConnection("Live");
      };
      current.onerror = () => {
        if (current.readyState === EventSource.CONNECTING) setConnection("Reconnecting");
        if (current.readyState === EventSource.CLOSED) schedule();
      };
      current.addEventListener("metric", (event: MessageEvent) => {
        try {
          const point = readMetric(event);
          if (point.seq <= lastApplied) return;
          if (point.seq !== lastApplied + 1) throw new Error("Stream gap");
          lastApplied = point.seq;
          setRun(previous => previous && {
            ...previous, last_seq: point.seq, points: [...previous.points, point],
          });
        } catch {
          current.close();
          schedule();
        }
      });
    }

    async function load() {
      try {
        const response = await request(path, { signal });
        const snapshot: Snapshot = await response.json();
        if (signal.aborted) return;
        setRun(snapshot);
        lastApplied = snapshot.last_seq;
        connect();
      } catch (failure) {
        if (signal.aborted) return;
        if (failure instanceof RequestError && failure.status === 401) signedOut();
        else setError("Could not load this run. Try reloading the page.");
      }
    }

    void load();
    return () => {
      controller.abort();
      stream?.close();
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [id, signedOut]);

  return { run, connection, error };
}
