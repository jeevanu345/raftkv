import {
  Activity,
  Boxes,
  Database,
  FlaskConical,
  Gauge,
  Network,
  Settings,
  TerminalSquare
} from "lucide-react";

import {
  NavLink
} from "react-router-dom";

const navItems = [
  {
    to: "/",
    label: "Overview",
    icon: Boxes,
    end: true
  },
  {
    to: "/keys",
    label: "Key Explorer",
    icon: Database
  },
  {
    to: "/console",
    label: "Command Console",
    icon: TerminalSquare
  },
  {
    to: "/raft",
    label: "Raft Visualizer",
    icon: Network
  },
  {
    to: "/metrics",
    label: "Metrics",
    icon: Gauge
  },
  {
    to: "/simulation",
    label: "Simulation Lab",
    icon: FlaskConical
  },
  {
    to: "/administration",
    label: "Administration",
    icon: Settings
  }
];

export default function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar__brand">
        <div className="sidebar__brand-mark">
          <Activity size={18} strokeWidth={2} />
        </div>

        <div>
          <div className="sidebar__brand-title">
            RaftKV
          </div>

          <div className="sidebar__brand-subtitle">
            Control Plane
          </div>
        </div>
      </div>

      <nav className="sidebar__nav">
        <div className="sidebar__nav-label">
          Workspace
        </div>

        {navItems.map(
          ({ to, label, icon: Icon, end }) => (
            <NavLink
              key={to}
              title={label}
              aria-label={label}
              to={to}
              end={end}
              className={({ isActive }) =>
                `sidebar-link ${
                  isActive
                    ? "sidebar-link--active"
                    : ""
                }`
              }
            >
              <Icon
                size={17}
                strokeWidth={1.8}
              />

              <span>{label}</span>
            </NavLink>
          )
        )}
      </nav>

      <div className="sidebar__footer">
        <div className="sidebar__version">
          <span>raftkv</span>
          <span>0.2 dashboard</span>
        </div>
      </div>
    </aside>
  );
}
