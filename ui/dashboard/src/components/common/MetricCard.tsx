import type { ReactNode } from "react";

interface MetricCardProps {
  label: string;
  value: ReactNode;
  detail?: ReactNode;
  tone?: "default" | "good" | "warn" | "danger";
}

export default function MetricCard({
  label,
  value,
  detail,
  tone = "default"
}: MetricCardProps) {
  return (
    <article
      className={`metric-card metric-card--${tone}`}
    >
      <div className="metric-card__label">
        {label}
      </div>

      <div className="metric-card__value">
        {value}
      </div>

      {detail && (
        <div className="metric-card__detail">
          {detail}
        </div>
      )}
    </article>
  );
}
