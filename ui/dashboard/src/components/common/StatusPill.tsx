interface StatusPillProps {
  status:
    | "healthy"
    | "degraded"
    | "unreachable"
    | "leader"
    | "follower"
    | "candidate"
    | "running"
    | "paused"
    | "stable"
    | "joint"
    | "connected"
    | "disconnected";

  label?: string;
}

export default function StatusPill({
  status,
  label
}: StatusPillProps) {
  return (
    <span
      className={`status-pill status-pill--${status}`}
    >
      <span className="status-pill__dot" />
      {label ?? status}
    </span>
  );
}
