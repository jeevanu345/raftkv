import { useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Check, ChevronDown } from "lucide-react";

type Option = Readonly<{ value: string; label: string }>;
interface Props {
  options: readonly Option[];
  value?: string;
  defaultValue?: string;
  onChange?: (value: string) => void;
  disabled?: boolean;
  name?: string;
  id?: string;
  "aria-label"?: string;
}

export default function Select({ options, value, defaultValue, onChange, disabled, name, id, "aria-label": label }: Props) {
  const generatedId = useId();
  const controlId = id ?? generatedId;
  const listId = `${controlId}-options`;
  const [internal, setInternal] = useState(defaultValue ?? options[0]?.value ?? "");
  const requested = value ?? internal;
  const selected = options.some(option => option.value === requested) ? requested : options[0]?.value ?? "";
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const [position, setPosition] = useState({ left: 0, top: 0, width: 0, maxHeight: 280 });
  const button = useRef<HTMLButtonElement>(null);
  const menu = useRef<HTMLDivElement>(null);
  const search = useRef({ text: "", at: 0 });
  const show = () => { setActive(Math.max(0, options.findIndex(option => option.value === selected))); setOpen(true); };
  const choose = (index: number) => {
    const option = options[index];
    if (!option) return;
    setInternal(option.value); onChange?.(option.value); setOpen(false); button.current?.focus();
  };
  useEffect(() => { if (disabled) setOpen(false); }, [disabled]);
  useLayoutEffect(() => {
    if (!open) return;
    const place = () => {
      const rect = button.current?.getBoundingClientRect();
      if (!rect) return;
      const desired = Math.min(280, options.length * 44 + 12);
      const below = innerHeight - rect.bottom - 12;
      const above = rect.top - 12;
      const upward = below < desired && above > below;
      const height = Math.max(44, Math.min(desired, upward ? above : below));
      const width = Math.min(Math.max(rect.width, 200), innerWidth - 24);
      setPosition({ left: Math.max(12, Math.min(rect.left, innerWidth - width - 12)), top: upward ? rect.top - height - 4 : rect.bottom + 4, width, maxHeight: height });
    };
    place(); window.addEventListener("resize", place); window.addEventListener("scroll", place, true);
    return () => { window.removeEventListener("resize", place); window.removeEventListener("scroll", place, true); };
  }, [open, options.length]);
  useEffect(() => {
    if (!open) return;
    const outside = (event: PointerEvent) => {
      if (event.target instanceof Node && !button.current?.contains(event.target) && !menu.current?.contains(event.target)) setOpen(false);
    };
    document.addEventListener("pointerdown", outside);
    return () => document.removeEventListener("pointerdown", outside);
  }, [open]);
  useEffect(() => { if (open) document.getElementById(`${listId}-${active}`)?.scrollIntoView({ block: "nearest" }); }, [active, open, listId]);
  return <>
    {name && <input type="hidden" name={name} value={selected} disabled={disabled}/>}
    <button ref={button} id={controlId} type="button" className="gui-select" role="combobox" aria-label={label} aria-controls={listId} aria-expanded={open} aria-haspopup="listbox" aria-activedescendant={open ? `${listId}-${active}` : undefined} disabled={disabled}
      onClick={() => open ? setOpen(false) : show()}
      onKeyDown={event => {
        if (event.key === "Tab") { setOpen(false); return; }
        if (event.key === "Escape") { event.preventDefault(); setOpen(false); return; }
        if (["ArrowDown", "ArrowUp", "Home", "End"].includes(event.key)) {
          event.preventDefault();
          if (!open) { show(); return; }
          setActive(index => event.key === "Home" ? 0 : event.key === "End" ? options.length - 1 : Math.max(0, Math.min(options.length - 1, index + (event.key === "ArrowDown" ? 1 : -1))));
        } else if (event.key === "Enter" || event.key === " ") {
          event.preventDefault(); if (open) choose(active); else show();
        } else if (event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
          event.preventDefault();
          const now = Date.now(); search.current.text = (now - search.current.at > 700 ? "" : search.current.text) + event.key.toLowerCase(); search.current.at = now;
          const index = options.findIndex(option => option.label.toLowerCase().startsWith(search.current.text));
          if (!open) setOpen(true); if (index >= 0) setActive(index);
        }
      }}>
      <span>{options.find(option => option.value === selected)?.label ?? "Choose an option"}</span><ChevronDown size={16} aria-hidden="true"/>
    </button>
    {open && createPortal(<div ref={menu} id={listId} role="listbox" aria-label={label ?? "Options"} className="gui-select__menu" style={position}>
      {options.map((option, index) => <div id={`${listId}-${index}`} key={option.value} role="option" aria-selected={option.value === selected} className={`gui-select__option ${index === active ? "gui-select__option--active" : ""}`} onPointerMove={() => setActive(index)} onPointerDown={event => event.preventDefault()} onClick={() => choose(index)}>
        <span>{option.label}</span>{option.value === selected && <Check size={16} aria-hidden="true"/>}
      </div>)}
    </div>, document.body)}
  </>;
}
