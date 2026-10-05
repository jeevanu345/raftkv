interface LogEntryView {
  index: number;
  term: number;
  state:
    | "applied"
    | "committed"
    | "uncommitted"
    | "conflict";
}

interface LogStripProps {
  entries: LogEntryView[];
  snapshotIndex?: number;
}

export default function LogStrip({
  entries,
  snapshotIndex
}: LogStripProps) {
  return (
    <div className="log-strip">
      {snapshotIndex !== undefined && (
        <div className="log-strip__snapshot">
          ≤ {snapshotIndex}
        </div>
      )}

      {entries.map((entry) => (
        <div
          key={entry.index}
          className={`log-entry log-entry--${entry.state}`}
          title={`Index ${entry.index}, term ${entry.term}, ${entry.state}`}
        >
          <span>{entry.index}</span>
          <small>t{entry.term}</small>
        </div>
      ))}
    </div>
  );
}
