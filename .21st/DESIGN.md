# RaftKV Console design context

Preserve the existing Avenir typography, blue-gray surfaces, cyan actions, and compact diagnostic layout. The home page and local profile sign-in extend the console rather than replace its tools.

Tokens live in `ui/src/styles.css`. Use existing `Panel`, `Pill`, `Metric`, `Detail`, `EventList`, and `CodeBlock` primitives. Body text is 14px; supporting text is at least 12px. Use 5–6px control radii and 7–9px panel radii.

Navigation is reflected in URL fragments, with a native modal command menu and a compact mobile menu. Motion is short, responds to interaction, and honors reduced motion. All runtime data comes from node observations. Local profile sign-in does not authenticate node APIs.

21st catalog search informed sidebar and command-menu patterns; project primitives remain the implementation.
