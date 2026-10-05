import { useSyncExternalStore } from "react";

export type Appearance = Readonly<{ theme: "dark" | "light"; fontScale: number }>;
const storageKey = "raftkv.appearance.v1";
const defaults: Appearance = { theme: "dark", fontScale: 100 };
const listeners = new Set<() => void>();

function parse(raw: string | null): Appearance {
  try {
    const value: unknown = JSON.parse(raw ?? "null");
    if (typeof value !== "object" || value === null) return defaults;
    const { theme, fontScale } = value as Record<string, unknown>;
    return {
      theme: theme === "light" ? "light" : "dark",
      fontScale: typeof fontScale === "number" && Number.isFinite(fontScale)
        ? Math.min(150, Math.max(85, Math.round(fontScale / 5) * 5)) : 100
    };
  } catch { return defaults; }
}
function read(): Appearance {
  try { return parse(localStorage.getItem(storageKey)); } catch { return defaults; }
}
let current = read();
function apply() {
  document.documentElement.dataset.theme = current.theme;
  document.documentElement.style.setProperty("--font-scale", String(current.fontScale / 100));
  document.querySelector('meta[name="theme-color"]')?.setAttribute("content", current.theme === "light" ? "#f5f7fb" : "#0b0d10");
}
apply();
function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => { listeners.delete(listener); };
}
export function setAppearance(update: Partial<Appearance>) {
  current = parse(JSON.stringify({ ...current, ...update }));
  try { localStorage.setItem(storageKey, JSON.stringify(current)); } catch { /* Session still works when storage is unavailable. */ }
  apply();
  listeners.forEach(listener => listener());
}
window.addEventListener("storage", event => {
  if (event.key !== storageKey && event.key !== null) return;
  current = read(); apply(); listeners.forEach(listener => listener());
});
export function useAppearance() {
  return useSyncExternalStore(subscribe, () => current);
}
