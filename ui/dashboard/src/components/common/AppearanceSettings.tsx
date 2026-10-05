import Panel from "./Panel";
import { setAppearance, useAppearance } from "../../lib/appearance";

export default function AppearanceSettings() {
  const { theme, fontScale } = useAppearance();
  return <Panel title="Appearance" description="Applies across every dashboard page. Preferences are saved in this browser.">
    <div className="appearance-settings">
      <label className="appearance-settings__field">Color theme
        <select value={theme} onChange={event => setAppearance({ theme: event.target.value === "light" ? "light" : "dark" })}>
          <option value="dark">Dark mode</option><option value="light">Light mode</option>
        </select>
      </label>
      <div className="appearance-settings__field">
        <label htmlFor="font-scale">Global font size <output htmlFor="font-scale">{fontScale}%</output></label>
        <div className="appearance-settings__scale">
          <button className="button button--ghost" aria-label="Decrease font size" disabled={fontScale <= 85} onClick={() => setAppearance({ fontScale: fontScale - 5 })}>A−</button>
          <input id="font-scale" type="range" min="85" max="150" step="5" value={fontScale} aria-valuetext={`${fontScale}%`} onChange={event => setAppearance({ fontScale: Number(event.target.value) })}/>
          <button className="button button--ghost" aria-label="Increase font size" disabled={fontScale >= 150} onClick={() => setAppearance({ fontScale: fontScale + 5 })}>A+</button>
        </div>
        <span>Smaller 85% · Default 100% · Larger 150%</span>
      </div>
      <button className="button button--ghost" onClick={() => setAppearance({ theme: "dark", fontScale: 100 })}>Reset appearance</button>
    </div>
  </Panel>;
}
