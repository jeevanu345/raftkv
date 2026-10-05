import type {
  HTMLAttributes,
  ReactNode
} from "react";

interface PanelProps
  extends HTMLAttributes<HTMLDivElement> {
  title?: string;
  description?: string;
  action?: ReactNode;
  children: ReactNode;
}

export default function Panel({
  title,
  description,
  action,
  children,
  className = "",
  ...props
}: PanelProps) {
  return (
    <section
      className={`panel ${className}`}
      {...props}
    >
      {(title || description || action) && (
        <header className="panel__header">
          <div>
            {title && (
              <h2 className="panel__title">
                {title}
              </h2>
            )}

            {description && (
              <p className="panel__description">
                {description}
              </p>
            )}
          </div>

          {action && (
            <div className="panel__action">
              {action}
            </div>
          )}
        </header>
      )}

      <div className="panel__body">
        {children}
      </div>
    </section>
  );
}
