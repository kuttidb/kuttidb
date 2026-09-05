# KuttiDB management UI design system

Status: proposed design direction, ready for implementation handoff. This document
specifies the redesign; it does not describe an already implemented interface.
Prepared 2026-09-05. Implementation assignment:
the management UI redesign instruction (a local planning document under
`docs/plans/`).

## 1. Direction: the KuttiDB workbench

The console should feel like the working side of the KuttiDB landing page:
warm paper, olive-black type, precise rules, compact controls, and one familiar
toast mascot. Give operators a clear view of their server and a predictable place
to inspect values, manage deliveries, read streams, and run maintenance.

Personality comes from the composition and brand assets. Operational screens use
plain language, aligned numbers, and truthful status. A quiet table full of useful
information is a successful screen.

### Source of truth

| Reference | What it establishes | Console translation |
|---|---|---|
| [Landing page](../../landing/index.html) and [stylesheet](../../landing/landing.css) | Paper `#f5f2e9`, ink `#27271f`, orange `#ed742f`, green `#c8daac`; Helvetica and system monospace; ruled sections | Keep these brand primitives; reduce the scale and spacing for repeated daily use |
| [Original logo](../../landing/logo.png) | Smiling toast with bitten corner, warm crust, transparent background | Reuse the artwork, upright, at a small size |
| [Console mark](../../apps/management-ui/public/kuttidb-mark.png) | Visually matches the landing mascot | Use this existing public asset after verifying the image still matches |
| [Current console tokens](../../apps/management-ui/src/client/styles.css) | Light/dark semantic tokens and Tailwind aliases | Preserve the token interface while replacing its visual values |
| [Current components](../../apps/management-ui/src/client/components/ui/) | Radix interactions and local primitives | Restyle these primitives; preserve accessible behavior |

The primary inspiration is `landing/index.html`, not `landing/alternative/`.
No outside product is a required reference and no commercial font is needed.

### Brand boundaries

- Use one 28–32 px high mascot beside the `KuttiDB.` wordmark in the shell.
  Preserve its intrinsic aspect ratio with `object-fit: contain`; no square white
  tile, rounded background, cropping, recoloring, added shadow, or rotation.
- Wordmark: 22 px Helvetica stack, weight 700, tracking -0.055em. The period may
  use orange as decoration. Accompany it with a quiet 12 px `Console` label.
- A Connections welcome panel may use a 64 px high mascot instead of repeating
  it in the page body. Maximum one mascot per visible screen.
- Use serif emphasis only for an optional Connections welcome sentence; it has
  no role in tables, page titles, forms, dialogs, or operational messages.
- Bring over the landing page's section rules and material palette. Oversized
  headlines, full orange hero panels, marketing arrows, and slogans belong to
  the landing page.

## 2. Concrete visual rules

1. Use a paper canvas and paper sidebar in light mode. Separate regions with
   borders and spacing rather than a heavy dark sidebar.
2. Structure a page with a title, toolbar, and one main working surface. Related
   summaries share a continuous strip with internal dividers.
3. Use squared geometry: 0 px for tables/sections, 2 px for badges, 4 px for
   controls and overlays. Reserve circles for status dots and switch knobs.
4. Default primary action is ink on the button background with paper text.
   Orange marks the active location and small brand details. Do not make all
   buttons, selected tabs, statuses, and charts orange.
5. Show a number prominently only when it helps decide the next action. Every
   number has a label, unit where relevant, and an identifiable data source.
6. Use real names: `Keyspace`, `Queues`, `Streams`, `Consumer groups`, `Routing`,
   `Atomic operations`, and `Maintenance`. Avoid inventing bakery terminology
   for database concepts.

### Reject these outcomes in review

| Reject | Use instead |
|---|---|
| A grid of equally prominent rounded KPI cards | Three related engine summaries in one ruled band; pressure shown separately |
| Glass, blurred headers, gradients, glows, decorative grids, floating blobs | Opaque surfaces, 1 px rules, deliberate empty space |
| A large welcome hero on every operational page | 26 px title, short explanation, useful data immediately below |
| Pastel icon tiles, emoji navigation, a different icon family per view | Existing Lucide icons, 16 px, consistent stroke |
| Pill-shaped everything | Underlined tabs, rectangular buttons, quiet text statuses |
| Fake activity charts or “All systems operational” before data loads | Measured data or an explicit loading/unknown state |
| “Unlock powerful insights”, “Your data journey”, “Oopsie!” | “Inspect retained messages.” / “Could not load queues.” |
| A recolor of the existing cards without layout changes | New hierarchy, table density, field grouping, and responsive shell |

## 3. Color system

These are exact target values. Brand primitives come from the landing stylesheet;
supporting semantic colors are proposed console additions.

| Token | Light | Dark | Purpose |
|---|---|---|---|
| `--background` | `#f5f2e9` | `#20241e` | Page canvas |
| `--foreground` | `#27271f` | `#f5f2e9` | Main text |
| `--card`, `--popover` | `#fbf9f2` | `#252a23` | Editors, menus, dialogs |
| `--card-foreground`, `--popover-foreground` | `#27271f` | `#f5f2e9` | Text on those surfaces |
| `--secondary`, `--muted` | `#e6e8df` | `#30362c` | Table heads, neutral states, hover |
| `--secondary-foreground` | `#27271f` | `#f5f2e9` | Text on secondary |
| `--muted-foreground` | `#606156` | `#b9beae` | Help, metadata, labels |
| `--border` | `#cecec1` | `#515947` | Decorative rules and separators |
| `--input` | `#818373` | `#859079` | Visible control outlines |
| `--rule-strong` | `#27271f` | `#aab49b` | Main section rules |
| `--primary` | `#27271f` | `#f5f2e9` | Main action background |
| `--primary-foreground` | `#f5f2e9` | `#27271f` | Main action text |
| `--primary-hover` | `#46483c` | `#dfdfd1` | Explicit opaque hover |
| `--brand-orange` | `#ed742f` | `#ed742f` | Brand detail, active marker |
| `--brand-green` | `#c8daac` | `#c8daac` | Limited decorative highlight |
| `--accent` | `#eee4d7` | `#423526` | Selected/active subtle fill |
| `--accent-foreground` | `#27271f` | `#f5f2e9` | Selected text |
| `--link` | `#9a3b0b` | `#f4ac79` | Inline links, underlined |
| `--ring` | `#397aa0` | `#8dc4df` | Keyboard focus |
| `--success`, `--success-surface` | `#35532b` / `#e4edd9` | `#c8daac` / `#303e29` | Known successful/healthy state |
| `--warning`, `--warning-surface` | `#754c0b` / `#f6e8c6` | `#edcd83` / `#403722` | Degraded, stale, attention |
| `--destructive`, `--danger-surface` | `#9b332c` / `#f7e4df` | `#f1aaa0` / `#442a26` | Error text, destructive affordances |
| `--destructive-foreground` | `#fbf9f2` | `#20241e` | Filled destructive button text |
| `--info`, `--info-surface` | `#315b70` / `#e2edf1` | `#a6cddd` / `#263b44` | In-flight and informational state |

Sidebar aliases: background/foreground mirror the canvas; primary and primary
foreground mirror accent and accent foreground; accent mirrors muted; border
mirrors border; ring mirrors ring. Add a separate 3 px orange active rail.

Map success/warning/info/surface/link/brand additions into `@theme inline` before
using Tailwind classes. Do not leave a `bg-warning` class without a defined token.
Stop borrowing chart colors for semantic status. For existing chart aliases use
orange, olive, blue, brown, and gray; actual charts are deferred unless a real
supported dataset and operator question justify them.

Measured solid-color contrast ratios: ink/paper 13.44:1; muted/paper 5.61:1;
ink/orange 5.11:1; link/paper 6.24:1. **Paper text on orange is only 2.63:1:**
use dark ink if orange ever carries a button label. The proposed light status
text/surface pairs are all above 5.8:1. Decorative `--border` is too subtle to
identify form controls by itself; use `--input` for those outlines. These checks
cover token pairs, not a completed accessibility review of rendered components.
Recheck actual foreground/background combinations, including hover and dark mode.

Dark mode should resemble the landing page's olive-black demo section. Preserve
hierarchy and density. Do not apply a global brown tint or lower text opacity.
Resolve an explicit stored theme before OS preference, then keep one theme across
Connections, locked states, the shell, and portals. Set `color-scheme` to the
resolved theme. Persist only this harmless preference, never credentials.

## 4. Typography and geometry

Font stacks:

```css
--font-sans: "Helvetica Neue", Helvetica, Arial, sans-serif;
--font-mono: ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace;
--font-editorial: Georgia, "Times New Roman", serif;
```

No network font fetch. Do not claim JetBrains Mono is loaded when it is only
named in a fallback list. Use tabular numerals for quantities and monospace for
keys, IDs, offsets, endpoints, tokens being entered, and raw payloads.

| Role | Size / line height | Weight / tracking |
|---|---|---|
| Page title | 26 / 32 px | 600 / -0.035em |
| Narrow-screen page title | 24 / 30 px | 600 / -0.03em |
| Section title | 16 / 24 px | 600 / -0.015em |
| Body, labels, table cells | 14 / 20 px | 400; labels 500 |
| Button and navigation | 14 / 20 px | 500 |
| Metadata/help | 12 / 18 px | 400 |
| Table heading | 12 / 16 px | 500; sentence case |
| IDs and payload | 12 / 18 px | 400 mono |
| Primary engine count | 28 / 32 px | 500; tabular |
| Navigation group label | 11 / 16 px | 500 / 0.06em; uppercase |

11 px is limited to navigation group labels. No 10 px badges or operational
instructions. Avoid uppercase paragraphs and negative tracking on payload text.

Spacing scale: 4, 8, 12, 16, 24, 32, 48 px. 2 px is permitted for optical alignment
and focus offset. Default field gap 16 px; label-to-input 6 px; input-to-help 6 px;
section gap 32 px; toolbar-to-table 16 px. Table cells use 12 px horizontal padding.

| Dimension | Target |
|---|---|
| Desktop sidebar | 216 px including its right border |
| Context header | Minimum 56 px; grows when text wraps |
| Main page gutter | 32 px desktop, 24 px tablet, 16 px mobile |
| Content maximum width | 1440 px, aligned left in available main area |
| Navigation row | 36 px minimum height |
| Standard / compact button | 36 / 32 px height |
| Touch button | 44 px minimum interactive height |
| Input / select | 36 px; 44 px on coarse-pointer devices |
| Table heading / body row | 36 / 44 px minimum; allow wrapped content |
| Numeric compact row | 36 px only when it has no dense controls |
| Editor form width | Maximum 640 px |
| Standard dialog | 480 px; destructive 520 px; payload editor up to 720 px |
| Dialog viewport limit | `calc(100vw - 32px)` width; `calc(100dvh - 32px)` height |

Surface radius 0 px; control, popover, and dialog radius 4 px; status tag 2 px.
Remove existing `rounded-xl`, full-pill badge defaults, and computed radius
derivations that produce unwanted values. Use explicit radius aliases 2/4/4/4 px.
No shadows on tables, summary strips, buttons, or panels. Only overlays may use
`0 8px 24px rgb(39 39 31 / 14%)`. Scrim: `rgb(20 24 18 / 40%)`.

## 5. Shell and navigation

Desktop shell has a paper navigation column, an opaque context bar, and the
working area. Brand lockup sits above a connection switcher. Navigation groups:

| Group | Items, in order |
|---|---|
| Server | Overview |
| Data | Keyspace, Queues, Streams |
| Coordination | Consumer groups, Routing |
| Operations | Atomic operations, Maintenance |
| Footer | Connections, theme control, Lock all tokens |

Keep existing capability gates and route paths. Empty groups disappear. Active
navigation has a subtle fill, orange left rail, semibold label, and
`aria-current="page"`. Exactly one item is active, including the two Operations
routes. Use links for navigation and buttons for actions.

The context bar shows the selected profile label, endpoint in monospace, and a
text status. Its purpose is to answer “Which server am I acting on?” Version and
API contract belong in Overview metadata rather than the brand lockup.
Distinguish `Connected` from server readiness, persistence health, and audit
health. A session existing does not prove all subsystems are healthy.

Breakpoints:

- At 1200 px and above: 216 px sidebar, 32 px gutters, optional 2:1 detail columns.
- At 768–1199 px: navigation becomes a labelled Menu drawer; 24 px gutters;
  overview summary remains three columns if it fits. Stack detail sidebars when
  the working column would be narrower than 560 px.
- Below 768 px: Menu drawer, 16 px gutters, stacked summaries and forms. Toolbar
  wraps; primary action is visible. Endpoints wrap or reveal full text on focus.
  Scroll wide tables inside a labelled region; never make the whole page overflow.
- At 320 px and at 200% zoom: page title, connection identity, primary action,
  error messages, and dialog buttons remain reachable. Drawer closes after
  navigation and returns focus to its trigger when dismissed.

## 6. Component specifications

### PageHeader, Section, and SummaryStrip

`PageHeader` contains one h1, optional one-sentence description, and actions.
Use an optional breadcrumb on detail pages. Long decoded names wrap; full
canonical IDs are available separately for copying. Actions wrap below the
heading rather than narrowing it to a few characters.

`Section` is a heading followed by content with a top rule, no nested card.
`SummaryStrip` is a shared top/bottom border, equal columns, and internal rules.
Each cell has a label, a main quantity, and at most three secondary facts. A
summary link is a real focusable link, not a clickable div.

### Buttons, tabs, and menus

- Primary: ink fill, paper text; one per task region, ordinarily one per page.
- Secondary: transparent/paper fill, input-color border, ink text.
- Quiet: no resting border, subtle hover fill; keep destructive quiet actions
  red and labelled. Icon-only actions need accessible names and tooltips.
- Destructive: filled danger color is reserved for the final action in a
  confirmation dialog. Row menus may expose `Delete queue…` as red text.
- Disabled: muted surface and muted text, no hover; explain capability/audit
  restrictions alongside the action. Do not use opacity as the only state cue.
- Busy: preserve button width; spinner plus `Connecting…`, `Publishing…`, or
  another specific verb. Disable duplicate submissions; keep errors in context.
- Tabs: horizontal text labels over a bottom rule, 40 px minimum height, 2 px
  ink underline on active tab. Keep Radix keyboard behavior. Scroll the tab row
  independently on small screens; no pill container.
- Menus: 4 px radius, 4 px padding, 32 px minimum rows (44 px for touch),
  separators before destructive actions. Do not hide the main page action in a menu.

### Tables and pagination

The default resource view is a semantic table with a tinted head, horizontal
rules, no alternating saturated fills, no outer rounded card. Left-align names
and IDs; right-align counts, bytes, and offsets. Put units in headings or values
consistently. Names are links; row click may supplement that link but cannot
replace it. Keep copy and row action controls separate from navigation.

Make long identifiers selectable; a truncated display must have a keyboard
reachable full value. Copy buttons report success only after clipboard success.
Unknown is `—` with accessible “Unavailable”, never zero. Do not show pagination
totals or global sorting unless supported. Preserve opaque cursor Back/Next
behavior and weak-consistency notices. Any client-only filter must say
`Filter this page`; backend search uses only documented parameters.

### Forms and payloads

Use persistent labels and inline help, not placeholder labels. Group related
fields with headings and rules; optional settings may use a disclosure. Retain
encoding, size, TTL/lease/offset units and limits. Keep text, JSON, and Base64
views explicit; never silently rewrite binary data. Present payloads in a ruled
editor surface with an encoding tab bar, size metadata, and bounded scrolling.

Validation runs before submission where possible. Associate errors through
`aria-describedby`, mark invalid fields, and focus the first invalid field after
submission. Keep entered values after an error; clear sensitive values when
their existing lifecycle requires it. Never auto-persist tokens or payload drafts.

### Status and feedback

| Meaning | Presentation |
|---|---|
| Healthy, succeeded, ready message | Success dot/icon and text |
| Running, active delivery, in-flight | Info color and text |
| Queued, delayed, cancelled, needs token | Neutral text; do not imply failure |
| Stale, degraded, outcome unknown | Warning icon and explanatory text |
| Failed, audit unhealthy, persistence unhealthy | Danger icon and text |
| Data absent before a first successful read | Neutral `Checking…` or `Unknown` |

Use subtle rectangular tags only in tables where they improve scanning; prefer
plain status lines in headers. Do not rely on color alone or animate a healthy dot.
Every polling view needs these distinct states:

1. First load: static skeleton that matches final rows/sections and `aria-busy`.
2. Empty collection: “No queues yet.” plus a permitted creation action.
3. No search matches: “No keys match this prefix.” plus Clear filter.
4. Initial failure: actionable error and read retry, without invented data.
5. Background failure: retain last successful data, visibly mark it stale, show
   the last successful timestamp and retry. Never leave an old green state
   looking current.
6. Mutation pending: retain context and prevent duplicate actions.
7. Mutation success/failure: show the real returned result; distinguish partial,
   unknown, conflict, and success. An accepted maintenance job is `Queued`, not
   “Checkpoint complete”.

Success toasts are brief and optional when an inline result already confirms the
action. Errors and unknown outcomes remain inline until resolved or dismissed.
Use the existing safe error code and copyable request ID; never raw credentials
or stack traces. For `operation_in_doubt`, show reconciliation guidance and do
not offer a blind resubmit button. Apply existing retry policy to safe reads.

### Confirmation dialogs

Show action, connection label/endpoint, resource name, canonical ID, and concrete
consequence. Preserve exact typed-ID matching and freshly captured revision
checks. Do not prefill confirmation text. Use a specific final label such as
`Purge messages` rather than `Confirm`. Focus Cancel initially for destructive
actions; trap focus and return it to the triggering control. Keep the dialog and
error visible until the request has a confirmed outcome. Resource removal and
local profile removal must have different wording.

## 7. Reference screen compositions

### Connections

Use a standalone branded frame with 32 px desktop padding, maximum 1120 px,
centered horizontally. Put the brand and theme control in a simple top row.
Below the title, use a 400 px form column and a flexible saved-connections list,
separated by a rule and 32 px gap. Stack below 900 px. The saved list uses ruled
rows: profile name, endpoint, session state, last connection, Open/Reconnect, and
a secondary menu. Keep `Lock all tokens` visible.

Field copy: `Connection name`, `Management endpoint`, `Administrator token`.
Use this helper: “Profiles are saved on this device. Tokens stay in gateway
memory for this browser session and are never saved with a profile.” The default
endpoint remains `http://127.0.0.1:7380`. Empty state is a simple sentence with no
second mascot or dashed card. The form remains the visual anchor.

### Overview

At a 1440 × 900 viewport, sidebar is 216 px; main inner width is 1160 px after
32 px gutters. Context bar is 56 px. Page header follows with 32 px top padding.
Place server readiness, durability mode, uptime, and audit status in a wrapping
status line, followed by any blocking banner.

Below that, make one three-column summary strip: Keyspace, Queues, Streams.
Each column uses 20 px inset (a permitted summary-specific spacing exception).
Show entries/live bytes/expired/evicted; queue count/ready/in-flight/dead-letter;
stream count/partitions/retained bytes/groups respectively. Link each heading to
its real resource route. No decorative chart.

Next comes a 2:1 split: Recent jobs table on the left; Persistence and engines
definition list on the right. Follow with a compact Management pressure row
containing active tails, deliveries, claims, queued jobs, and running jobs.
All existing Overview facts remain available. A whole-server weak-consistency
note sits beside the refresh timestamp or under the status area.

### Queues list and detail

List: title and `Declare queue` action, optional supported filter, one table
with Name, Queue ID, Ready, In-flight. Show per-row names and counts before
secondary IDs. Any added durable flag must come from a supported response, not
an assumption. Pagination remains where currently supported, not invented.

Detail: breadcrumb, decoded queue name, copyable canonical ID, compact facts
including durability and dead-letter target, and separate Purge/Delete actions.
Preserve Overview, Messages, Publish, Deliveries, Consumers tabs. Messages is a
dense table; Publish is a bounded editor; Deliveries is an explicit working
session with receipt, lease, and acknowledgement actions. Viewing Messages must
never consume work. Do not reduce these tabs to a generic inspector.

### Other screens

| Screen | Composition and requirements |
|---|---|
| Keyspace | Keyspace selector and prefix search above table; Key, Entry ID, Size, Expires; entry inspector with encoding tabs; preserve create/update/delete and batch flows. Treat null TTL as no expiry only when the contract says so |
| Streams list | Name, Stream ID, Partitions, Records, Retained; declare action above table |
| Stream detail | Resource header and retention facts; Records, Append, Tail, Partitions tabs; represent partition and offset together; keep tail bounded with explicit start/stop and status |
| Consumer groups | Stream, Group, Generation, Members table; detail prioritizes offsets/lag table, then member snapshot and management session; resets get a separate confirmation flow |
| Routing | Name, Mode, Routes, Published, Unroutable, Router ID; detail uses definition list and routes table. Represent alternate router and topology with existing factual content, without a decorative graph |
| Atomic operations | Form and transaction preview in a 3:2 split, stacked on narrow screens; preserve operation modes, capability warnings, result, and atomicity constraints |
| Maintenance | Checkpoint availability in engine rows; jobs table below with kind, state, creation time, and supported actions; show queued/running/succeeded distinctly |
| Locked / expired session | Same branded frame and theme, profile identity, reason, and Reconnect/Connections action; preserve route context |
| Unknown profile | Branded recovery screen with clear message and Manage connections action; never a blank page |

## 8. Accessibility, motion, and content acceptance

- Target 4.5:1 for normal text and 3:1 for large text, essential control boundaries,
  and focus indicators. Verify both themes in the actual browser.
- Use a visible 2 px focus outline with 2 px offset. On dark-filled buttons in
  light mode add a paper separator so the focus treatment stays distinguishable.
- Provide a skip link, main/navigation landmarks, meaningful headings, labelled
  dialogs, table column headers, and names for all icon-only controls.
- Enter/Space activate buttons; links work with Enter; Radix tabs retain arrow
  navigation. Focus stays stable through polling and row refreshes.
- Live-tail updates must not flood assistive announcements. Announce start/stop,
  connection loss, and action outcomes through a restrained live region.
- Use 100–150 ms color/opacity transitions only. No hover lift, chart entrance,
  continuous status pulse, or ornamental skeleton shimmer. Respect reduced motion.
- Use exact counts, clear byte units, and explicit milliseconds/seconds labels.
  Relative timestamps expose an absolute timestamp and timezone on focus or in
  details. Distinguish retained records from total historical records.
- The user can identify the active server, current screen, next action, and any
  blocked operation without interpreting color or opening a tooltip.

## 9. Design completion criteria

The redesign is complete when all current workflows follow this system, rather
than only Overview. Review a populated table, payload editor, empty state,
connection error, stale read, locked profile, destructive dialog, and dark mode.
Compare the console with the landing page for palette, typography, rules, and
mascot treatment. It should read as one product while giving operational data
the space and density it needs.

The agent instruction (a local planning document under `docs/plans/`) defines
file ownership, staged implementation, behavior checks, and final evidence.
