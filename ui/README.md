# RaftKV Console

Run `npm install` and `npm run dev` in this directory, then open the printed localhost URL. Launch the Rust cluster separately with `./scripts/run_cluster.sh` from the repository root. The console explains how to connect when nodes are offline.

Enter a name and email to open a local operator profile. It lasts for the browser tab and survives reloads; sign out clears it. This is a convenience profile, **not API authentication**. Node APIs remain unauthenticated. A closed tab ends the profile's normal lifetime; browser session restoration may restore tab storage.

Home links to the existing cluster, node, keyspace, telemetry, architecture, and connection tools. Pages have bookmarkable URL fragments. Open the tool search with **⌘K** or **Ctrl+K**; use arrows, Enter, and Escape. Appearance and endpoint settings persist locally. Reduced-motion preferences are respected.

Validation:

```sh
npm test
npm run build
npx playwright install chromium  # first run, if Chromium is not installed
npm run test:e2e
```

Playwright uses deterministic node API fixtures to exercise sign-in, navigation, themes, connection changes, leader-routed writes, offline guidance, and all seven panes at mobile, tablet, and desktop sizes. It does not modify a running cluster. Failure traces are written to `.artifacts/test-results/`.
