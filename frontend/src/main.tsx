import { useCallback, useEffect, useMemo, useState, type FormEvent } from "react";
import { createRoot } from "react-dom/client";
import { request, RequestError, type Project } from "./api";
import { useRun } from "./useRun";
import "./style.css";

function SignIn({ onSuccess }: { onSuccess: () => Promise<void> }) {
  const [key, setKey] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      await request("/api/session", {
        method: "POST", headers: { Authorization: "Bearer " + key.trim() },
      });
      setKey("");
      await onSuccess();
    } catch (failure) {
      setError(failure instanceof RequestError && failure.status === 401
        ? "This read key is invalid or has been rotated."
        : "Could not sign in. Try again.");
    } finally {
      setBusy(false);
    }
  }
  return <section className="panel login">
    <p className="eyebrow">Project access</p>
    <h1>Sign in</h1>
    <p>Use your project’s read key to view runs and live metrics.</p>
    <form onSubmit={submit}>
      <label htmlFor="project-key">Project read key</label>
      <input id="project-key" value={key} type="password" autoComplete="off" required
        onChange={event => setKey(event.target.value)} />
      <button disabled={busy}>{busy ? "Signing in…" : "Sign in"}</button>
    </form>
    {error && <p className="error" role="alert">{error}</p>}
  </section>;
}

function RunView({ id, signedOut }: { id: string; signedOut: () => void }) {
  const { run, connection, error } = useRun(id, signedOut);
  const points = useMemo(() => run ? [...run.points].sort((a, b) =>
    a.step - b.step || a.name.localeCompare(b.name)) : [], [run]);
  return <>
    <a className="back" href="/">← All runs</a>
    {error ? <p className="error" role="alert">{error}</p> : <>
      <div className="heading">
        <div><p className="eyebrow">Experiment run</p><h1>{run?.name ?? "Loading run…"}</h1></div>
        <span role="status" data-testid="connection" className={"status " + connection.toLowerCase()}>
          {connection}
        </span>
      </div>
      {run && <section className="panel metrics">
        <h2>Metrics</h2>
        <p className="muted">{points.length} points · last sequence {run.last_seq}</p>
        {points.length === 0 ? <p>Waiting for the first metric.</p> : <div className="table-scroll">
          <table>
            <thead><tr><th scope="col">Step</th><th scope="col">Metric</th>
              <th scope="col">Value</th><th scope="col">Sequence</th></tr></thead>
            <tbody>{points.map(point => <tr key={point.seq} data-testid="metric-row">
              <td>{point.step}</td><td>{point.name}</td><td>{point.value}</td><td>{point.seq}</td>
            </tr>)}</tbody>
          </table>
        </div>}
      </section>}
    </>}
  </>;
}

function App() {
  const [project, setProject] = useState<Project | null>();
  const [error, setError] = useState("");
  const selected = location.pathname.startsWith("/view/") ? location.pathname.split("/")[2] : undefined;

  const signedOut = useCallback(() => setProject(null), []);
  const loadProject = useCallback(async () => {
    try {
      const response = await request("/runs");
      setProject(await response.json());
      setError("");
    } catch (failure) {
      if (failure instanceof RequestError && failure.status === 401) signedOut();
      else setError("Could not load your project. Try reloading the page.");
    }
  }, [signedOut]);
  useEffect(() => { void loadProject(); }, [loadProject]);

  async function logout() {
    try {
      await request("/api/session/logout", { method: "POST" });
      window.location.replace("/");
    } catch {
      setError("Could not sign out. Try again.");
    }
  }

  return <>
    <header><div className="shell toolbar">
      <a href="/" className="brand">Soup <span>Experiment Tracker</span></a>
      {project && <div className="account"><span>{project.project}</span>
        <button className="quiet" onClick={() => void logout()}>Sign out</button></div>}
    </div></header>
    <main className="shell">
      {error && <p className="error" role="alert">{error}</p>}
      {project === undefined && !error && <p role="status">Loading project…</p>}
      {project === null && <SignIn onSuccess={loadProject} />}
      {project && (selected ? <RunView id={selected} signedOut={signedOut} /> : <>
        <p className="eyebrow">{project.project}</p><h1>Training runs</h1>
        {project.runs.length === 0 ? <p>No training runs yet.</p> :
          <ul className="runs">{project.runs.map(run => <li key={run.id}>
            <a href={"/view/" + run.id}>{run.name} <span aria-hidden="true">→</span></a>
          </li>)}</ul>}
      </>)}
    </main>
  </>;
}

// A browser-restored page must revalidate its session before showing old data.
window.addEventListener("pagehide", event => {
  if (event.persisted) document.documentElement.classList.add("suspended");
});
window.addEventListener("pageshow", event => {
  if (event.persisted) window.location.reload();
});
createRoot(document.getElementById("root")!).render(<App />);
