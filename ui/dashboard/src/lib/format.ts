export function formatNumber(value: number | null | undefined) {
  if (value === null || value === undefined) {
    return "—";
  }

  return new Intl.NumberFormat("en-US").format(value);
}

export function formatBytes(bytes: number | null | undefined) {
  if (bytes === null || bytes === undefined) {
    return "—";
  }

  if (bytes === 0) {
    return "0 B";
  }

  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    units.length - 1
  );

  const value = bytes / 1024 ** exponent;

  return `${value >= 10 ? value.toFixed(0) : value.toFixed(1)} ${
    units[exponent]
  }`;
}

export function formatDuration(seconds: number | null | undefined) {
  if (seconds === null || seconds === undefined) {
    return "—";
  }

  if (seconds < 60) {
    return `${Math.floor(seconds)}s`;
  }

  const minutes = Math.floor(seconds / 60);

  if (minutes < 60) {
    return `${minutes}m`;
  }

  const hours = Math.floor(minutes / 60);

  if (hours < 24) {
    return `${hours}h ${minutes % 60}m`;
  }

  const days = Math.floor(hours / 24);

  return `${days}d ${hours % 24}h`;
}

export function formatTtl(ttlMs: number | null) {
  if (ttlMs === null) {
    return "Persistent";
  }

  if (ttlMs <= 0) {
    return "Expired";
  }

  const seconds = Math.floor(ttlMs / 1000);

  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);

  if (minutes < 60) {
    return `${minutes}m ${seconds % 60}s`;
  }

  const hours = Math.floor(minutes / 60);

  return `${hours}h ${minutes % 60}m`;
}

export function formatRole(role: string) {
  switch (role) {
    case "leader":
      return "Leader";

    case "follower":
      return "Follower";

    case "candidate":
      return "Candidate";

    case "pre-candidate":
      return "Pre-candidate";

    default:
      return "Unknown";
  }
}
