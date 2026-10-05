import { useState } from "react";
import {
  CircleHelp,
  RefreshCw
} from "lucide-react";

import { api, isDemoMode } from "../../lib/api";
import { useLocation } from "react-router-dom";

import { useCluster } from "../../hooks/useCluster";
import StatusPill from "../common/StatusPill";

const titles: Record<string, string> = {
  "/": "Cluster Overview",
  "/keys": "Key Explorer",
  "/console": "Command Console",
  "/raft": "Raft Visualizer",
  "/metrics": "Metrics",
  "/simulation": "Simulation Lab",
  "/administration": "Administration"
};

export default function Topbar() {
  const location = useLocation();
  const [token,setToken]=useState("");const [authOpen,setAuthOpen]=useState(false);const [authError,setAuthError]=useState("");

  const cluster = useCluster();

  const title =
    titles[location.pathname] ??
    "RaftKV";

  return (
    <header className="topbar">
      <div>
        <div className="topbar__eyebrow">
          {isDemoMode()?"Demo workspace · simulated data":"Live distributed KV control plane"}
        </div>

        <h1 className="topbar__title">
          {title}
        </h1>
      </div>

      <div className="topbar__actions">
        {!isDemoMode()&&<button className="button button--ghost" onClick={()=>setAuthOpen(!authOpen)}>Sign in</button>}
        {authOpen&&<form className="member-form" onSubmit={async event=>{event.preventDefault();try{await api.login(token);setToken("");setAuthOpen(false);setAuthError("");await cluster.refetch();}catch(error){setAuthError(error instanceof Error?error.message:"Sign-in failed");}}}><label>Admin token<input type="password" autoComplete="off" value={token} onChange={e=>setToken(e.target.value)}/></label><button className="button" type="submit">Authenticate</button>{authError&&<span role="alert">{authError}</span>}</form>}
        {cluster.data && (
          <>
            <div className="topbar__meta">
              <span>Term</span>
              <strong>
                {cluster.data.term}
              </strong>
            </div>

            <div className="topbar__meta">
              <span>Leader</span>
              <strong>
                {cluster.data.leaderId
                  ? `Node ${cluster.data.leaderId}`
                  : "None"}
              </strong>
            </div>

            <StatusPill
              status={
                cluster.data.health === "healthy"
                  ? "healthy"
                  : "degraded"
              }
              label={
                cluster.data.health === "healthy"
                  ? "Cluster healthy"
                  : "Cluster degraded"
              }
            />
          </>
        )}

        <button
          className="icon-button"
          title="Refresh cluster state"
          onClick={() => cluster.refetch()}
        >
          <RefreshCw size={16} />
        </button>

        <button
          className="icon-button"
          title="About this dashboard"
          onClick={()=>window.alert("RaftKV: observer and administration interface. Simulation Lab is isolated from the live cluster.")}
        >
          <CircleHelp size={16} />
        </button>
      </div>
    </header>
  );
}
