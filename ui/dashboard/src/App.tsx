import { lazy, Suspense } from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import AppShell from "./components/layout/AppShell";

const OverviewPage = lazy(() => import("./pages/OverviewPage"));
const KeyExplorerPage = lazy(() => import("./pages/KeyExplorerPage"));
const CommandConsolePage = lazy(() => import("./pages/CommandConsolePage"));
const RaftVisualizerPage = lazy(() => import("./pages/RaftVisualizerPage"));
const MetricsPage = lazy(() => import("./pages/MetricsPage"));
const SimulationLabPage = lazy(() => import("./pages/SimulationLabPage"));
const AdministrationPage = lazy(() => import("./pages/AdministrationPage"));

export default function App() {
  return (
    <Suspense fallback={<div role="status">Loading page…</div>}><Routes>
      <Route element={<AppShell />}>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/keys" element={<KeyExplorerPage />} />
        <Route path="/console" element={<CommandConsolePage />} />
        <Route path="/raft" element={<RaftVisualizerPage />} />
        <Route path="/metrics" element={<MetricsPage />} />
        <Route path="/simulation" element={<SimulationLabPage />} />
        <Route path="/administration" element={<AdministrationPage />} />
      </Route>

      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes></Suspense>
  );
}
