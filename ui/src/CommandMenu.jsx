import { useEffect, useRef, useState } from "react";
import { ArrowRight, Search, X } from "lucide-react";

export default function CommandMenu({ open, onClose, panes, onNavigate }) {
  const dialog = useRef(null);
  const [query, setQuery] = useState("");
  const results = panes.filter(pane => `${pane.label} ${pane.description}`.toLowerCase().includes(query.toLowerCase()));

  useEffect(() => {
    if (open && !dialog.current.open) {
      setQuery("");
      dialog.current.showModal();
      dialog.current.querySelector("input")?.focus();
    } else if (!open && dialog.current.open) dialog.current.close();
  }, [open]);

  const moveFocus = (event) => {
    if (event.key === "Escape") { event.preventDefault(); dialog.current.close(); return; }
    if (!["ArrowDown", "ArrowUp"].includes(event.key)) return;
    const buttons = [...dialog.current.querySelectorAll(".command-result")];
    if (!buttons.length) return;
    event.preventDefault();
    const current = buttons.indexOf(document.activeElement);
    const next = current < 0 ? (event.key === "ArrowDown" ? 0 : buttons.length - 1) : (current + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length;
    buttons[next].focus();
  };
  const choose = id => {
    dialog.current.close();
    onNavigate(id);
    onClose();
  };

  return <dialog ref={dialog} className="command-dialog" aria-labelledby="command-title" onClose={onClose} onClick={event => { if (event.target === dialog.current) dialog.current.close(); }} onKeyDown={moveFocus}>
    <div className="command-heading">
      <Search size={20} aria-hidden="true" />
      <h2 id="command-title">Go to a tool</h2>
      <button className="icon-button" onClick={() => dialog.current.close()} aria-label="Close navigation"><X size={18} /></button>
    </div>
    <label className="sr-only" htmlFor="command-search">Search pages and tools</label>
    <input id="command-search" name="navigation-search" type="search" value={query} onChange={event => setQuery(event.target.value)} placeholder="Search pages and tools…" autoComplete="off" onKeyDown={event => {
      if (event.key === "Enter" && results.length) { event.preventDefault(); choose(results[0].id); }
    }} />
    <div className="command-results" aria-label="Matching pages">
      {results.map(({ id, icon: Icon, label, description }) => <button className="command-result" key={id} onClick={() => choose(id)}>
        <Icon size={20} aria-hidden="true" />
        <span><strong>{label}</strong><small>{description}</small></span>
        <ArrowRight size={16} aria-hidden="true" />
      </button>)}
      {!results.length && <p className="command-empty" role="status">No tools match “{query}”. Try “keys”, “nodes”, or “connections”.</p>}
    </div>
    <div className="command-footer"><span>↑ ↓ to move · Enter to open</span><span>Esc to close</span></div>
  </dialog>;
}
