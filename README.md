# herdr-sleeper — sleep idle resumable agents, wake them later

A Herdr plugin that sleeps idle agent panes after a user-chosen window and
wakes them — by answering `y` at the prompt left in the pane, by command, or (opt-in) on focus. It is
the standalone [`scripts/herdr-sleeper`](https://github.com/frdminc/herdr/tree/herdr-sleeper/scripts/herdr-sleeper)
reference implementation reborn as a plugin: the fail-closed safety rules
are unchanged, and the plugin adds what only a plugin can do (an in-process
idle watcher, an in-pane wake prompt, optional wake-on-focus, a sidebar
placeholder for slept panes, notifications, and repair of launch flags that
Herdr's own restore drops).

Grown from a study of the two existing community plugins — good ideas
adopted from both (see *Credits* at the end); the safety state machine,
argv replay filter, session-live checks, and config validation are ours and
carried over from the standalone script's three review rounds plus its
live-deployment iteration.

Building something similar, for Herdr or another multiplexer?
[LESSONS.md](LESSONS.md) collects the non-obvious traps we hit: pane and
session identity across restarts, the plugin runtime, typing into panes, idle
clocks, watcher lifecycle and testing against a real server. Each comes with
evidence. It is written to be useful whether or not you use this plugin.

> **Status:** [herdr-park](https://github.com/prabhatCH/herdr-park) is
> becoming the shared base for the three Herdr sleep plugins
> ([discussion](https://github.com/prabhatCH/herdr-park/issues/4)), and ideas
> from herdr-sleeper are being ported there as focused pull requests.
> herdr-sleeper still works and 0.1.2 is the current release, but new feature
> work is going to herdr-park.

## Install

```bash
herdr plugin install frdminc/herdr-sleeper
```

Requires Herdr ≥ 0.9.1 on macOS or Linux, and Python 3.9 or newer on the
PATH the Herdr server sees (stdlib only — no packages, no venv; config
parsing falls back to a built-in TOML subset below Python 3.11).

For local development:

```bash
git clone https://github.com/frdminc/herdr-sleeper
herdr plugin link <checkout>
```

## What it does

After an agent has been idle for a user-chosen window (`idle = "12h"` —
default 12 h), it exits the agent process (SIGTERM for claude and opencode:
both shut down cleanly and a composer draft is discarded, never submitted),
keeps the pane, reports a `sleeper` placeholder so the sidebar keeps its
row, leaves a wake stub in the pane (a "Resume? [y/N]" prompt), prefixes the
pane label with 💤, and posts a notification with the freed MB.

Wake paths:

1. **Answer `y` in the pane** — the stub asks "Resume? [y/N]"; `y` or `Y`
   plus Enter execs the agent back into its session after the same
   fail-closed checks and restores the pane label; the next watcher tick
   hands the agent its name back once the session is seen running. Anything
   else — a bare Enter, stray text, `pane run` input from an orchestrator — is
   not a wake request: the stub says once that it is staying asleep and keeps
   waiting. The prompt also names the `wake` command, because after a Herdr
   restart the saved screen is replayed with no stub behind it. Ctrl-C leaves
   a plain shell and prints the manual resume line.
2. **Command or sidebar action** — `herdr plugin action invoke
   djbclark.herdr-sleeper.wake` wakes the context pane (only that pane), or
   `wake-all` / `list` / `log` / `version` from anywhere. When the pane's stub
   is running, `wake` types `y` + Enter into it and waits up to 30 s for the
   session to come back before reporting success (a stub that exec'd but
   whose session Herdr has not reported yet counts as started). A stub left
   by v0.1.1 or earlier takes a bare Enter, and gets one.
3. **Focus the pane — opt-in.** With `wake_on_focus = true` (or
   `HERDR_SLEEPER_WAKE_ON_FOCUS=1`) the `pane.focused` hook wakes the pane
   through the same path. It is off by default: a focus cannot tell a click
   from a workspace switch, a restore or a scripted focus, so waking on it
   resumes agents you only walked past. Changed from v0.1.1, where focus woke
   by default and the stub woke on a bare Enter.

The idle clock is `state_change_seq` movement from the watcher's polling of
`agent list` (default 60 s) — no transcript mtime, no launchd, and the
clock restarts on any state change between polls. `done` (idle-but-unviewed)
accrues like `idle`, and a pane slept while `done` shows
`claude · sleeping · done`. Clocks persist in `idle.json` (wall clock), keyed
pane + terminal id + `state_change_seq` + session, so a watcher restart
keeps them; a Herdr restart, a damaged file, or any mismatch starts the
clock again (a later sleep, never an earlier one). Resets reach the file
before the tick sleeps anything. `scan` honours the same clocks (an
unclocked pane's stretch starts now), so it no longer sleeps everything at
once.

Supported kinds for sleeping: **claude** (original argv replayed on wake)
and **opencode** (`-s <session>`). Codex is deliberately absent — see the
comment on `PROFILES` in the script.

### Restore repair (v0.1.2)

Herdr's own restore relaunches each agent as its binary, a resume flag and
the session id, so every launch flag (`--model`, `--yolo` ...) is lost. The
watcher notices a pane whose session was recorded with richer flags in
another terminal (terminal ids change on every restore and on nothing else)
and relaunches it once with the recorded flags. **On by default**
(`[restore] repair_flags = false` turns it off), for every kind in the
`KINDS` table except codex, which must be named in `repair_kinds`.

Gates, all fail closed: pane not focused and not in `exclude` (the same list
that keeps a pane from sleeping), agent idle or done, session live nowhere
else, cwd as recorded, transcript on disk (claude), an empty input box for a
kind with a screen detector (claude). Two more guard against mistaking your
own relaunch for Herdr's:

- the agent's process id and start time are recorded when the restore is
  noticed; if the pane's agent has a different pid or start time when the
  repair is due, you relaunched it yourself: the marker is settled
  (`relaunched`, one `repair-settled` event) and the pane is left alone. A pid
  or start time that cannot be read means no repair;
- the agent must stay idle, through one unchanged `state_change_seq`, for 60 s
  before repair acts (never on the first idle tick).

Limit, stated plainly: if you exit the restored agent and relaunch it bare
*before* the watcher first notices the restore (up to one poll, 60 s by
default), the two launches are indistinguishable and the pane is repaired.

**Permission and sandbox bypass flags are not re-added by default.** Each
`KINDS` entry marks its bypass-class options (`--dangerously-skip-permissions`,
`--yolo`, `--allow-all*`, codex's `--dangerously-bypass-approvals-and-sandbox`,
`-a`/`--sandbox`, ...), and anything that widens what runs unasked: allowed tools, urls or paths
(`--add-dir`, copilot's `--allow-tool`/`--allow-url`/`--autopilot`, qwen's `--allowed-tools`, omp's
`--plan-yolo-into`/`--allow-home`, cursor's `--sandbox` with its value, `--auto-review`, agy's `--mode`) and, for
codex, `-c`/`--config` and `-p`/`--profile`, which can set the sandbox or approval policy. Both spellings
(`--opt value`, `--opt=value`) are covered. Repair restores the other recorded flags without them;
a pane whose record holds only bypass flags is left alone (the pending reason
says so). `[restore] repair_bypass = true` (env
`HERDR_SLEEPER_REPAIR_BYPASS`) puts them back too. The lists were read from each CLI's own top-level `--help`
where it is installed (claude, codex, hermes, cursor-agent, agy); copilot, qwen, omp and opencode were not on the
machine they were written on, so their extra entries are by name, and the comments in the `KINDS` table say which.
Nothing typed in the composer is logged: a draft is reported as `draft present (N chars)`, and `panes.json`
keeps only the replayable flags of an agent's argv, never a prompt or a refused option's value.

A pane that fails a gate is retried for `repair_window_minutes` (default 30),
then given up on. A repair is a second resume of the session: the price of
getting the flags back. `list` shows pending repairs. Other `[restore]` keys:
`repair_kinds`, `require_empty_composer` (env `HERDR_SLEEPER_REPAIR_FLAGS` and
so on). Per kind, the option tables are an allow-list: a prompt, `--`, a
one-shot, fork or secret-bearing option, or *any option not listed* for that
kind (long or short, with or without `=value`), or a CLI the plugin could not
read, refuses the replay and leaves the pane alone. Option values are never
written to `sleeper.log`, `events.jsonl` or notifications (names only); the
manual resume line printed to you keeps them, and its `sleeper.log` copy
masks them.

### Deploys (v0.1.2)

The watcher reloads itself: when the script's code stamp has changed on two
consecutive ticks, the file compiles, ends with its `__main__` block and
`herdr-sleeper version` runs cleanly, it re-execs in place (same pid, the
watcher lock handed over through an inherited fd). A broken script is logged
once and the old code keeps running. `startup` also stops a stale-path
`herdr-sleeper watch` that serves the same state dir (never one of another
Herdr session, and nothing at all when `lsof` cannot say).
`herdr plugin action invoke djbclark.herdr-sleeper.version` (or
`herdr-sleeper version`) prints one line: manifest version, script path, code
stamp, and the running watcher's pid and stamp (`current` or `STALE`). It is
read-only.

## Use

```bash
herdr plugin action invoke djbclark.herdr-sleeper.list
herdr plugin action invoke djbclark.herdr-sleeper.sleep-pane   # context pane
herdr plugin action invoke djbclark.herdr-sleeper.wake         # context pane
herdr plugin action invoke djbclark.herdr-sleeper.wake-all
herdr plugin action invoke djbclark.herdr-sleeper.log
herdr plugin action invoke djbclark.herdr-sleeper.version
```

Config: `<plugin config dir>/config.toml` — Herdr's
`~/.config/herdr/plugins/config/djbclark.herdr-sleeper/config.toml` (run
`herdr plugin list` to see it) — `idle` (duration), `agents` (list),
`exclude` (panes or names), `poll_seconds`, `notify`, `wake_on_focus`
(default false), and a `[restore]` table (see *Restore repair*);
overridable by `HERDR_SLEEPER_*` env (booleans take true/false/1/0), lowest
to highest precedence. State: `<plugin state dir>/` per Herdr session —
`sleeping.json` journal, `panes.json` crash-recovery snapshot, `idle.json`
idle clocks, `events.jsonl`, `sleeper.log`, `watcher.pid`/`watcher.lock`.
The state dir is created `0700` and the logs and pid file `0600` (they hold argv, cwd and flags).
Diagnostics go to stderr too, with option values masked in a manual resume line unless stderr is a terminal (a person
running `herdr-sleeper wake` gets the full command to paste; a hook, a Herdr action or the watcher does not). A session id
that is not a plain token (letters, numerals, `_ . : @ + = / -`) is refused everywhere: it is typed into the pane's shell.
A legacy standalone-script journal (`~/.local/state/herdr-sleeper/`) is
adopted once, on the default session's first startup. An older config —
v0.1.0's manual-run location
`~/.config/herdr/plugins/djbclark.herdr-sleeper/config.toml` or the
standalone `~/.config/herdr-sleeper/config.toml` — is translated once when
the plugin config is missing (`interval_minutes` dropped); when the plugin
config already exists it is never rewritten, and startup logs that the
older file is not read. An older config that cannot be translated keeps
sleeping off, and so does a plugin config with no `exclude` while an older
file still has one (merge it, or set `exclude = []`).

Keybindings work like any plugin action:

```toml
[[keys.command]]
key = "prefix+z"
type = "plugin_action"
command = "djbclark.herdr-sleeper.sleep-pane"
description = "sleep this agent pane"
```

## Safety rules (all fail closed)

Inherited verbatim from the standalone script; the deltas the plugin
introduces are marked.

1. Eligible only when: a supported kind with a session id (and, for claude,
   an on-disk transcript — nothing to resume otherwise); Herdr status
   `idle`/`done`; pane not focused; not excluded; session id not open in
   another pane; idle ≥ window; original argv safely replayable
   (`--fork-session`, `--print`, `--session-id`, positional prompts,
   anything after `--`, options of unknown arity → refuse).
2. Everything is re-checked after the journal write and immediately before
   the exit, including that `state_change_seq` and the session id have not
   moved; the exit targets the pane id, never the name; "exited" needs
   Herdr to drop the agent *and* process-info to show no agent process —
   unreadable process-info is unknown, not gone. *Delta:* the exit is a
   SIGTERM to the pane's identified agent process (refused when the pid
   cannot be identified) — a signal cannot append to a composer draft the
   way a typed `/exit` could, which is what the old positive-empty-composer
   check guarded against. That check still guards kinds exited by typing.
3. The record is written *before* the exit. If the agent is still there
   after the wait it stays `exit-requested`; the next tick reconciles
   (dropped only after two *consecutive* sightings with a real process, or
   when the pane runs a different session; `asleep` once verifiably gone;
   `displaced` when our own wake put another session there — by hand
   only). A record is never deleted because its pane vanished; the manual
   resume command is printed instead. A pane that moved (new pane id, same
   `terminal_id`) is followed.
4. *v0.1.1:* pane identity. Herdr re-allocates every terminal id when it
   restores a session and re-applies the saved manual label, so a changed
   `terminal_id` on a pane that still carries our 💤 label is a restart,
   not reuse: same pane id + 💤 label + a bare shell wakes, and the new
   terminal id is recorded only with a wake that succeeded. cwd is not
   identity evidence (workspace ids restart at max(restored)+1, so a fresh
   pane can get a recycled pane id in the same cwd — but never our label);
   a restored pane `cd`'d elsewhere is refused with "cd back". A new
   terminal without the label, or one running another session, is a reused
   pane id: the record is re-keyed `orphan:<first 8 of uuid>` with phase
   `orphaned` (one `orphaned` event), never woken automatically (focus,
   `wake-all` and `wake <orphan key or name>` refuse it, by key), and
   `list` prints its manual resume line. A legacy record with no terminal
   id needs the label as well. A record is dropped only when the *same*
   terminal now runs a different session (a deliberate replacement). The
   crash-recovery snapshot keeps an orphan copy the same way.
5. Wake refuses if the session id is live in any pane or any process that
   selects it (our own stub is recognised by its argv structure, never by
   substring), when that cannot be verified, when an `exit-requested`
   agent is still there (reconcile's two-sighting rule decides, not one
   focus), or when the same terminal moved to another cwd. *Delta:*
   opt-in focus-wakes are debounced by a per-pane lock (one click fires several
   `pane.focused` hooks) and give up after 10 s on a busy global lock (the
   next focus retries; a pane running another session is skipped silently);
   the in-pane stub resolves the agent binary and checks the transcript
   first, runs the replay filter and the liveness check again under the
   lock, and then marks the record `waking` with its pid rather than popping
   it (exec keeps the pid). Every other wake path refuses a `waking` record
   whose pid is alive; the watcher finishes it once the session runs, or
   puts it back to `asleep` (and re-claims the row) if the pid died; ctrl-c
   or a failed exec puts it back at once. An empty process list from Herdr
   is "unknown", never "no stub". A taken agent name is retried once as
   `wake-<pane>`; a start that answers before the session id is known is
   confirmed from `pane get`, never logged as a mismatch. `sleep-pane`
   refuses a pane whose record is kept for another session.
6. A non-object state file, malformed config, unknown keys, booleans where
   numbers go, conflicting env aliases, or a `nan`/negative/`inf` window
   disables sleeping (logged every tick) — `wake`/`list`/`log` keep
   working, and damaged state files are quarantined to
   `*.damaged-<timestamp>`, never overwritten. A file lock serialises
   overlapping runs; the journal is re-read under it before every write.
   On Python < 3.11 the config is read by a strict TOML subset parser that
   refuses (rather than misreads) anything tomllib would read differently.
7. *v0.1.1:* the watcher never gives up. Any exception costs one tick and
   backs off (poll × 2ⁿ, at most 10 min); one watcher per state dir, held
   by `watcher.lock`; SIGTERM (code change) finishes the transaction in hand
   first; `list`, `scan`, `sleep-pane` and every focus hook respawn a
   missing watcher, or replace a pre-v0.1.1 one that holds no lock (without
   inheriting the caller's `HERDR_SLEEPER_*` overrides). A recorded watcher
   pid is trusted only when its argv is `… herdr-sleeper watch` and, for a
   v0.1.1 record, it holds `watcher.lock`. One failing pane costs that pane,
   not the tick.

Known limits: SIGTERM leaves a small window between the final recheck and
the process handling it (only a native Herdr operation could close it);
the stub line is POSIX shell (`printf`, `env`), so nushell/pwsh panes get
focus/command wakes only.

## Tests

`python3 -m pytest tests/ -q` — 785 tests: the decision logic, the
sleep/wake state machine, pane identity and orphans, the watcher tick and
loop (idle clocks, backoff, the watcher lock, self-reload), the focus hook,
the stub and its `y` prompt, restore repair, the 3.9 TOML subset parser, and
legacy migration, against a fake `herdr` (one test execs real subprocesses
inside a pytest tmp dir to check the lock handover). Verified on Python 3.9
(Apple's `/usr/bin/python3`) and 3.14. Nothing here has been soaked against
a live Herdr server since v0.1.1.

## Credits

- [`dalogax/herdr-agent-hibernate`](https://github.com/dalogax/herdr-agent-hibernate) —
  the SIGTERM exit path, the seq-based idle clock, wake-on-focus with a
  per-pane lock, `terminal_id` identity, code-stamped watcher replacement,
  `agent_name_taken` handling ideas.
- `prabhatgmp/herdr-park` — the sidebar claim (`pane report-agent`), the
  in-pane wake stub, `notification show`, freed-MB reporting.
- [`scripts/herdr-sleeper`](https://github.com/frdminc/herdr/tree/herdr-sleeper/scripts/herdr-sleeper)
  (the standalone ancestor) — everything fail-closed.


## Upstream Herdr artifacts

1. [Discussion 631](https://github.com/herdrdev/herdr/discussions/631) — agent sleep / hibernate design thread (plugin announcement and follow-ups).
2. Standalone ancestor script on the fork: [`scripts/herdr-sleeper`](https://github.com/frdminc/herdr/tree/herdr-sleeper/scripts/herdr-sleeper).


## Related djbclark Herdr repos

| Repo | Role |
| --- | --- |
| [frdminc/herdr](https://github.com/frdminc/herdr) | Fork of [herdrdev/herdr](https://github.com/herdrdev/herdr) |
| [herdr-plain-line](https://github.com/frdminc/herdr-plain-line) | Local plugin: plain-English popup for tab/workspace rename and create |
| [herdr-sleeper](https://github.com/frdminc/herdr-sleeper) | Plugin: sleep idle agent panes, wake later |
| [herdr4paseo](https://github.com/frdminc/herdr4paseo) | Terminal UI to watch/steer a Paseo agent from a pane |
| [paseo4herdr](https://github.com/djbclark/paseo4herdr) | Paseo plugin mirroring OMP agents that run in Herdr |
