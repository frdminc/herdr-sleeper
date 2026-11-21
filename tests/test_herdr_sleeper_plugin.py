"""Unit tests for the herdr-sleeper plugin: pure decision logic plus the sleep/wake state
machine, the watcher tick, the focus hook, and the in-pane stub — against a fake `herdr`
(no Herdr, no Claude needed)."""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "herdr-sleeper"
sleeper = types.ModuleType("herdr_sleeper")
sleeper.__file__ = str(SCRIPT)  # a real interpreter sets __file__; exec() does not
exec(compile(SCRIPT.read_text(), str(SCRIPT), "exec"), sleeper.__dict__)

UUID = "20603f77-21a6-4a16-9f2b-77a87efbc665"
ORPHAN = "orphan:20603f77"
REAL_UUID_LIVE = sleeper.uuid_live_elsewhere  # captured before any fixture stubs it
REAL_HEAL = getattr(sleeper, "heal_watcher", None)
REAL_STOP_STRAYS = sleeper.stop_stray_watchers
REAL_SERVER_STARTED = sleeper.herdr_server_started
REAL_LSOF_PIDS = sleeper.lsof_pids


def term_at(when: float, counter: int = 0) -> str:
    """A Herdr terminal id (`term_<unix microseconds in 13 hex digits><counter>`) born at `when`."""
    return f"term_{int(when * 1e6):013x}{counter:x}"


# The Herdr server of the restore tests started half a minute ago. OLD_TERM was born in the previous server run, NEW_TERM
# is what its restore allocated seconds after the start, THIRD_TERM what a later server run's restore allocates.
SERVER_START = time.time() - 30
OLD_TERM = term_at(SERVER_START - 7200, 1)
NEW_TERM = term_at(SERVER_START + 5, 1)
LATER_START = SERVER_START + 7200             # a second server run (it is in the "future" of this process: only compared)
THIRD_TERM = term_at(LATER_START + 5, 1)


def agent(
    pane: str = "w1:p1",
    uuid: str | None = UUID,
    status: str = "idle",
    focused: bool = False,
    kind: str = "claude",
    name: str | None = "a",
    seq: int = 5,
    terminal: str = "term-1",
    cwd: str = "/tmp/proj",
) -> dict[str, Any]:
    a: dict[str, Any] = {"pane_id": pane, "agent_status": status, "focused": focused, "agent": kind,
                         "state_change_seq": seq, "cwd": cwd, "terminal_title_stripped": "Title",
                         "terminal_id": terminal, "workspace_id": "w1", "tab_id": "w1:t1"}
    if name:
        a["name"] = name
    if uuid:
        a["agent_session"] = {"value": uuid}
    return a


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """Isolated state dir + transcript root, with a transcript for UUID on disk."""
    d = tmp_path / "state"
    for name in ("STATE_DIR", "JOURNAL", "SNAPSHOT", "EVENTS", "LOGFILE", "LOCK", "WATCHER_PID",
                 "IDLE_FILE", "WATCHER_LOCK"):
        base = getattr(sleeper, name, None)
        if base is None:
            continue
        monkeypatch.setattr(sleeper, name, d / base.name if name != "STATE_DIR" else d)
    monkeypatch.setattr(sleeper, "LEGACY_STATE_DIR", tmp_path / "legacy")
    monkeypatch.setattr(sleeper, "CONFIG_FILE", tmp_path / "absent.toml")
    monkeypatch.setattr(sleeper, "LEGACY_CONFIG_FILES", [tmp_path / "legacy-config.toml"], raising=False)
    # never spawn a real watcher from a unit test; the heal path has its own tests
    monkeypatch.setattr(sleeper, "heal_watcher", lambda: None, raising=False)
    # nor ever signal a real process: startup's stray-watcher sweep has its own tests with a faked `ps`
    monkeypatch.setattr(sleeper, "stop_stray_watchers", list)
    # nor ask the OS who serves the socket: the restore tests run against a Herdr server that started SERVER_START
    monkeypatch.setattr(sleeper, "herdr_server_started", lambda: SERVER_START)
    monkeypatch.delenv(sleeper.WATCH_LOCK_FD_ENV, raising=False)
    if hasattr(sleeper, "STOP"):
        monkeypatch.setitem(sleeper.STOP, "requested", False)
    for var in ("HERDR_PLUGIN_CONTEXT_JSON", "HERDR_PANE_ID", "HERDR_PLUGIN_EVENT_JSON", "HERDR_SLEEPER_WAKE_ON_FOCUS"):
        monkeypatch.delenv(var, raising=False)
    root = tmp_path / "projects"
    (root / "proj").mkdir(parents=True)
    monkeypatch.setattr(sleeper, "TRANSCRIPTS", root)
    yield tmp_path
    for thread in threading.enumerate():  # a stub's marker thread stops once its prompt is answered; none outlives a test
        if thread.name == "stub-mark":
            thread.join(10)


def transcript(tmp_path: Path, uuid: str, age_hours: float, proj: str = "proj") -> Path:
    p = tmp_path / "projects" / proj / f"{uuid}.jsonl"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{}\n")
    stamp = time.time() - age_hours * 3600
    os.utime(p, (stamp, stamp))
    return p


PROC_START = 4_000_000.0   # the start time the fake reports for every agent process
EMPTY_SCREEN = "⏺ pong\n────────\n❯ \n────────\n  ◤ graft · 40 nodes\n"


class FakeHerdr:
    """Scripted `herdr` CLI: agents list, per-pane info, screen text, claims, and a call log."""

    def __init__(self, agents: list[dict[str, Any]], screen: str = EMPTY_SCREEN,
                 argv: list[str] | None = None) -> None:
        self.agents = agents
        self.screen: str | None = screen
        self.argv = ["--dangerously-skip-permissions"] if argv is None else argv
        self.calls: list[tuple[str, ...]] = []
        self.exit_leaves_agent = True      # a SIGTERM/prompt removes the agent from its pane
        self.start_uuid: str | None = None  # what agent start reports; None → the requested --resume uuid
        self.process_info_broken = False    # process-info returns an error envelope
        self.process_override: bool | None = None  # force process presence regardless of agent registration
        self.pidless = False              # report the agent process without a pid (argv still readable)
        self.stub_panes: set[str] = set()   # panes whose foreground is our wake stub
        self.stub_uuid = UUID               # the uuid argument our stub process carries
        self.stub_execs_on_enter = False    # a line the stub accepts resumes the stub's session
        self.stub_style = "new"             # "new": the Resume? [y/N] stub, execs on the line "y"; "legacy": on ""
        self.stub_lines: list[str] = []     # every line typed into a stub pane, as its input() would read it
        self.start_reports_session = True   # agent start answers with the session id (else it is learned later)
        self.start_session_appears = True   # ... and it then shows up in `pane get`
        self.taken_names: set[str] = set()  # agent names `agent start` rejects as taken
        self.empty_argv = False             # process-info lists the agent with no readable argv
        self.empty_foreground = False       # process-info answers with [] (Herdr could not see the job)
        self.start_updates_argv = False     # agent start changes the argv process-info reports (a real relaunch)
        self.signals: list[tuple[int, int]] = []
        self.execvps: list[tuple[str, list[str]]] = []
        self.binary: str | None = None      # the agent's executable when it is not the kind label (cursor-agent)
        self.proc_name: str | None = None   # the process name process-info reports (node for cursor-agent)
        self.prefix: list[str] = []         # arguments a launcher puts between the binary and the operator's own

    def stub_accepts(self, line: str) -> bool:
        """Would the stub of this flavour wake on this input line? The new one asks Resume? [y/N]; the legacy one
        (a pane slept before the prompt existed) wakes on any bare Enter and only echoes a "y" line."""
        return line == "y" if self.stub_style == "new" else line == ""

    def kill(self, pid: int, sig: int) -> None:
        self.signals.append((pid, sig))
        if self.exit_leaves_agent and sig == signal.SIGTERM:
            for a in self.agents:
                if a["pane_id"] in {p for p in self.stub_panes} or True:
                    if a.get("agent") and a.get("pid_hint") == pid:
                        a.pop("agent", None)
                        a.pop("agent_session", None)
            # the fake's processes carry pid 1 for the agent; drop any pane whose agent had no explicit pid
            for a in self.agents:
                if a.get("agent") and a.get("pid_hint", 1) == pid:
                    a.pop("agent", None)
                    a.pop("agent_session", None)

    def _procs(self, pane: str, a: dict[str, Any] | None) -> list[dict[str, Any]]:
        if pane in self.stub_panes:
            return [{"argv0": "python3", "name": "python3",
                     "argv": ["python3", str(SCRIPT), "stub", pane, self.stub_uuid], "pid": 99}]
        if self.empty_foreground:
            return []
        present = (a is not None and bool(a.get("agent"))) if self.process_override is None else self.process_override
        if not present:
            return [{"argv0": "zsh", "name": "zsh", "argv": ["-zsh"], "pid": 50}]   # an idle pane lists its shell
        kind = (a or {}).get("agent") or "claude"
        binary = self.binary or kind
        proc = {"argv0": binary, "name": self.proc_name or binary,
                "argv": [] if self.empty_argv else [binary, *self.prefix, *self.argv]}
        if not self.pidless:
            proc["pid"] = a.get("pid_hint", 1)
        return [proc]

    def __call__(self, *args: str, check: bool = True, timeout: float = 0) -> dict[str, Any]:
        self.calls.append(args)
        by_pane = {a["pane_id"]: a for a in self.agents}
        if args[:2] == ("agent", "list"):
            return {"agents": self.agents}
        if args[:2] == ("pane", "list"):
            return {"panes": [{"pane_id": a["pane_id"], "terminal_id": a.get("terminal_id"),
                               "cwd": a.get("cwd"), "label": a.get("label")} for a in self.agents]}
        if args[:2] == ("agent", "get"):
            a = by_pane.get(args[2])
            return {"agent": a} if a else {}
        if args[:2] == ("pane", "get"):
            a = by_pane.get(args[2])
            if a is None:
                if check:
                    raise sleeper.SleeperError("no such pane")
                return {}
            pane = {"pane_id": args[2], "label": a.get("label"), "cwd": a.get("cwd"),
                    "terminal_id": a.get("terminal_id")}
            if a.get("agent"):
                pane["agent"] = a["agent"]
                pane["agent_session"] = a.get("agent_session")
            return {"pane": pane}
        if args[:2] == ("pane", "process-info"):
            pane = args[args.index("--pane") + 1]
            if self.process_info_broken:
                return {}
            return {"process_info": {"foreground_processes": self._procs(pane, by_pane.get(pane))}}
        if args[:2] == ("agent", "start"):
            pane = args[args.index("--pane") + 1]
            if args[2] in self.taken_names:
                raise sleeper.SleeperError(f"herdr agent start failed: {{'code': 'agent_name_taken', 'name': '{args[2]}'}}")
            uuid = self.start_uuid or resume_uuid(args[args.index("--") + 1:])
            kind = args[args.index("--kind") + 1]
            if self.start_updates_argv:
                self.argv = list(args[args.index("--") + 1:])
            a = by_pane[pane]
            a.update(agent=kind, agent_session={"value": uuid} if self.start_session_appears else None)
            if not self.start_session_appears:
                a.pop("agent_session")
            a["started_name"] = args[2]
            self.stub_panes.discard(pane)
            return {"agent": {"agent_session": {"value": uuid}}} if self.start_reports_session else {"agent": {}}
        if args[:2] == ("agent", "rename"):
            by_pane[args[2]]["name"] = args[3]
            return {"agent": {"name": args[3]}}
        if args[:2] == ("pane", "report-agent"):
            by_pane[args[2]]["agent"] = sleeper.CLAIM_AGENT
            return {}
        if args[:2] == ("pane", "release-agent"):
            a = by_pane[args[2]]
            if a.get("agent") == sleeper.CLAIM_AGENT:
                a.pop("agent", None)
            return {}
        if args[:2] in (("pane", "run"), ("pane", "rename"), ("pane", "report-metadata"),
                        ("pane", "send-keys"), ("notification", "show"), ("agent", "prompt")):
            if args[:2] == ("pane", "rename"):
                assert len(args) > 3, "bare `pane rename <pane>` is a usage error in real herdr"
                by_pane[args[2]]["label"] = None if args[3] == "--clear" else args[3]
            if args[:2] == ("pane", "send-keys") and args[2] in self.stub_panes:
                lines = key_lines(args[3:])
                self.stub_lines.extend(lines)
                if self.stub_execs_on_enter and any(self.stub_accepts(line) for line in lines):
                    by_pane[args[2]].update(agent="claude", agent_session={"value": self.stub_uuid})
                    self.stub_panes.discard(args[2])
            return {}
        raise AssertionError(f"unexpected herdr call {args}")


def key_lines(keys: Any) -> list[str]:
    """`send-keys` tokens as the lines a tty hands to input(): single characters accumulate, `enter` ends a line."""
    lines: list[str] = []
    cur = ""
    for key in keys:
        if key == "enter":
            lines.append(cur)
            cur = ""
        else:
            cur += key
    return lines


def resume_uuid(tokens: Any) -> str:
    """The session id in the resume tokens of an `agent start` (any kind's form)."""
    for i, t in enumerate(tokens):
        if t.startswith("--resume="):
            return str(t.split("=", 1)[1])
        if t in ("--resume", "-s", "--session", "--conversation", "--thread", "resume") and i + 1 < len(tokens):
            return str(tokens[i + 1])
    raise AssertionError(f"no resume token in {tokens}")


@pytest.fixture
def fake(state: Path, monkeypatch: pytest.MonkeyPatch) -> FakeHerdr:
    f = FakeHerdr([agent()])
    monkeypatch.setattr(sleeper, "herdr", f)
    monkeypatch.setattr(sleeper, "herdr_screen", lambda pane: f.screen)
    monkeypatch.setattr(sleeper, "uuid_live_elsewhere", lambda uuid, except_pane=None, cwd=None, ignore_pids=None: None)
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    monkeypatch.setattr(sleeper, "EXIT_WAIT_SECONDS", 2)
    monkeypatch.setattr(sleeper.os, "kill", f.kill)
    monkeypatch.setattr(sleeper, "process_started", lambda pid: PROC_START)   # ps is not asked
    monkeypatch.setattr(sleeper, "REPAIR_IDLE_DWELL_SECONDS", 0)               # the dwell has its own tests
    transcript(state, UUID, 30)
    return f


def journal() -> dict[str, Any]:
    return sleeper.read_json(sleeper.JOURNAL)


def events() -> list[str]:
    p = sleeper.EVENTS
    return [json.loads(l)["event"] for l in p.read_text().splitlines()] if p.exists() else []


def stub_run(fake: FakeHerdr) -> str:
    return next(c[3] for c in fake.calls if c[:2] == ("pane", "run"))


# ------------------------------------------------------------- pure helpers

def test_replayable_argv_strips_session_selectors() -> None:
    ok = sleeper.replayable_argv
    assert ok(["--dangerously-skip-permissions", "--resume", "abc"]) == (["--dangerously-skip-permissions"], None)
    assert ok(["-r", "abc", "--model", "opus"]) == (["--model", "opus"], None)
    assert ok(["--resume=abc", "-c", "--continue"]) == ([], None)
    assert ok(["--resume", "--model", "opus"]) == (["--model", "opus"], None)  # bare --resume opens a picker
    assert ok([]) == ([], None)
    assert ok(["--add-dir", "/x", "--verbose"]) == (["--add-dir", "/x", "--verbose"], None)
    assert ok(["--unknown-flag", "value"])[0] is None and "unrecognised" in ok(["--unknown-flag", "value"])[1]
    assert ok(["--model"])[0] is None  # value option missing its value


@pytest.mark.parametrize(
    "args, needle",
    [
        (["--fork-session"], "--fork-session"),
        (["-p", "hi"], "-p"),
        (["--session-id", "x"], "--session-id"),
        (["--verbose", "publish the release"], "positional"),
        (["--dangerously-skip-permissions", "--", "x"], "`--`"),
    ],
)
def test_replayable_argv_refuses_dangerous(args: list[str], needle: str) -> None:
    argv, why = sleeper.replayable_argv(args)
    assert argv is None and why and needle in why


def test_parse_duration() -> None:
    assert sleeper.parse_duration("12h") == 12
    assert sleeper.parse_duration("90m") == 1.5
    assert sleeper.parse_duration("1d") == 24
    assert sleeper.parse_duration("3600s") == 1
    assert sleeper.parse_duration("2") == 2
    with pytest.raises(sleeper.ConfigError):
        sleeper.parse_duration("soon")


CLAUDE_SCREEN = """
❯ earlier question
⏺ pong
✻ Worked for 1s · done 5:23 PM
────────
❯ {composer}
────────
  ◤ graft · 40 nodes / 63 edges · ✓ synced
  ⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents
"""


def test_composer_state_fails_closed() -> None:
    cs = sleeper.composer_state
    assert cs(CLAUDE_SCREEN.format(composer=""))[0] == "empty"
    assert cs(CLAUDE_SCREEN.format(composer="half-typed thought")) == ("draft", "draft present (18 chars)")   # the text itself is never reported
    multiline = CLAUDE_SCREEN.format(composer="") .replace("────────\n  ◤", "  second line of a draft\n────────\n  ◤", 1)
    assert cs(multiline)[0] == "draft"
    assert cs("❯ \n  ?\n────────\n")[0] == "draft"          # one-character draft is still a draft
    assert cs("❯ \n")[0] == "unknown"                    # bare glyph, nothing rendered below it
    assert cs(None)[0] == "unknown"                       # unreadable screen
    assert cs("djbclark@mac:~$ \n")[0] == "unknown"       # bare shell: no composer to vouch for
    assert cs("⏺ done\n✻ Worked\n")[0] == "unknown"       # transcript visible, composer scrolled off
    assert cs("⏺ done\n> quoted line\n")[0] == "unknown"  # a bare ">" is not the composer
    assert cs("❯ \n─── a rule I am drafting\n────────\n  ⏵⏵ bypass\n")[0] == "draft"


def test_manual_command_is_shell_safe() -> None:
    cmd = sleeper.manual_command({"cwd": "/tmp/my dir", "argv": ["--model", "it's"], "uuid": "u", "kind": "claude"})
    assert cmd == "cd '/tmp/my dir' && claude --model 'it'\"'\"'s' --resume u"


def test_manual_command_never_prints_refused_argv() -> None:
    cmd = sleeper.manual_command({"cwd": "/p", "argv": ["--fork-session", "--model", "x"], "uuid": "u", "kind": "claude"})
    assert cmd == "cd /p && claude --resume u"


def test_wake_name_falls_back_to_pane() -> None:
    assert sleeper.wake_name({"name": None, "pane_id": "w26:p2"}) == "wake-w26-p2"
    assert sleeper.wake_name({"name": "lichess", "pane_id": "w24:p1"}) == "lichess"


# ---------------------------------------------------------------- config

def test_load_config_precedence(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = state / "config.toml"
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    for var in ("HERDR_SLEEPER_IDLE_HOURS", "HERDR_SLEEPER_IDLE", "HERDR_SLEEPER_EXCLUDE",
                "HERDR_SLEEPER_AGENTS", "HERDR_SLEEPER_POLL_SECONDS", "HERDR_SLEEPER_NOTIFY",
                "HERDR_SLEEPER_WAKE_ON_FOCUS", "HERDR_SLEEPER_REPAIR_KINDS", "HERDR_SLEEPER_REQUIRE_EMPTY_COMPOSER"):
        monkeypatch.delenv(var, raising=False)
    values, source = sleeper.load_config()
    assert values == {"idle_hours": 12.0, "agents": ["claude", "opencode"], "exclude": [],
                      "poll_seconds": 60, "notify": True, "wake_on_focus": False,
                      "repair_flags": True, "repair_bypass": False, "repair_window_minutes": 30.0,
                      "repair_kinds": [k for k in sleeper.KINDS if k != "codex"], "require_empty_composer": True}
    assert set(source.values()) == {"default"}

    cfg.write_text('idle = "90m"\nexclude = "orc"\npoll_seconds = 30\nnotify = false\n')
    values, source = sleeper.load_config()
    assert values["idle_hours"] == 1.5 and values["exclude"] == ["orc"]   # a string is one name, not letters
    assert values["poll_seconds"] == 30 and values["notify"] is False
    assert source["idle_hours"] == str(cfg)

    monkeypatch.setenv("HERDR_SLEEPER_IDLE", "2d")
    monkeypatch.setenv("HERDR_SLEEPER_EXCLUDE", "a b")
    values, source = sleeper.load_config()
    assert values["idle_hours"] == 48 and values["exclude"] == ["a", "b"]
    assert source["idle_hours"] == "$HERDR_SLEEPER_IDLE"


def test_wake_on_focus_is_an_opt_in_boolean(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = state / "config.toml"
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    assert sleeper.load_config()[0]["wake_on_focus"] is False            # focus-wake is opt-in
    cfg.write_text("wake_on_focus = true\n")
    values, source = sleeper.load_config()
    assert values["wake_on_focus"] is True and source["wake_on_focus"] == str(cfg)
    cfg.write_text('wake_on_focus = "yes"\n')
    with pytest.raises(sleeper.ConfigError, match="wake_on_focus must be a boolean"):
        sleeper.load_config()
    cfg.write_text("wake_on_focus = true\n")
    monkeypatch.setenv("HERDR_SLEEPER_WAKE_ON_FOCUS", "0")               # the environment still wins
    assert sleeper.load_config()[0]["wake_on_focus"] is False


@pytest.mark.parametrize(
    "text",
    ["idle_hours = = 3\n", "idle_hours = nan\n", "idle_hours = -1\n", "exclude = [1, 2]\n", "poll_seconds = 0\n",
     "bogus = 1\n", "idle_hours = false\n", "poll_seconds = true\n", 'agents = ["codex"]\n', "notify = 1\n"],
)
def test_load_config_rejects_bad_values(state: Path, monkeypatch: pytest.MonkeyPatch, text: str) -> None:
    cfg = state / "config.toml"
    cfg.write_text(text)
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    with pytest.raises(sleeper.ConfigError):
        sleeper.load_config()


def test_env_alias_conflict_and_units(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "CONFIG_FILE", state / "absent.toml")
    monkeypatch.setenv("HERDR_SLEEPER_IDLE_HOURS", "90m")
    monkeypatch.delenv("HERDR_SLEEPER_IDLE", raising=False)
    assert sleeper.load_config()[0]["idle_hours"] == 1.5
    monkeypatch.setenv("HERDR_SLEEPER_IDLE", "2h")
    with pytest.raises(sleeper.ConfigError):
        sleeper.load_config()


def test_cli_exclude_adds_to_config_exclude(fake: FakeHerdr, state: Path,
                                            monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    cfg = state / "config.toml"
    cfg.write_text('exclude = ["a"]\nidle_hours = 0.0\n')
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    monkeypatch.setattr(sleeper, "list_agents", lambda: fake.agents)
    assert sleeper.main(["scan", "--exclude", "zzz"]) == 0
    assert "excluded" in capsys.readouterr().out and journal() == {}


def test_scan_refuses_with_broken_config(state: Path, monkeypatch: pytest.MonkeyPatch,
                                         capsys: pytest.CaptureFixture[str]) -> None:
    cfg = state / "config.toml"
    cfg.write_text("idle_hours = = 3\n")
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    assert sleeper.main(["scan"]) == 2
    assert "broken config" in capsys.readouterr().err


# ------------------------------------------------------------- assessment

def test_assess_sleeps_when_window_elapsed(state: Path) -> None:
    transcript(state, UUID, 30)
    since = time.time() - 13 * 3600
    ok, _reason, idle = sleeper.assess(agent(), [agent()], 12, {}, idle_since=since)
    assert ok and idle is not None and idle > 12


def test_transcript_for_picks_newest_copy(state: Path) -> None:
    old = transcript(state, UUID, 40, proj="older")
    new = transcript(state, UUID, 0.1, proj="newer")
    assert sleeper.transcript_for(UUID) == new and sleeper.transcript_for(UUID) != old


def test_assess_requires_transcript_for_claude(state: Path) -> None:
    ok, reason, _ = sleeper.assess(agent(), [agent()], 12, {}, idle_since=time.time() - 100 * 3600)
    assert not ok and "no transcript" in reason


def test_assess_opencode_needs_no_transcript(state: Path) -> None:
    since = time.time() - 13 * 3600
    a = agent(kind="opencode", uuid="sess-1")
    ok, _reason, _ = sleeper.assess(a, [a], 12, {}, idle_since=since)
    assert ok


@pytest.mark.parametrize(
    "kw, needle",
    [
        ({"status": "working"}, "status working"),
        ({"status": "blocked"}, "status blocked"),
        ({"focused": True}, "focused"),
        ({"focused": None}, "focus unknown"),
        ({"uuid": None}, "no session uuid"),
        ({"kind": "codex"}, "not supported"),
    ],
)
def test_assess_rejections(state: Path, kw: dict[str, Any], needle: str) -> None:
    transcript(state, UUID, 30)
    a = agent(**kw)
    ok, reason, _ = sleeper.assess(a, [a], 12, {}, idle_since=time.time() - 100 * 3600)
    assert not ok and needle in reason


def test_assess_scan_rows_and_gates(state: Path) -> None:
    transcript(state, UUID, 30)
    a, b = agent(pane="w1:p1"), agent(pane="w2:p1", status="working")
    assert "w2:p1" in sleeper.assess(a, [a, b], 12, {})[1]
    assert sleeper.assess(a, [a], 12, {}, {"a"})[1] == "excluded"
    assert sleeper.assess(a, [a], 12, {}, {"w1:p1"})[1] == "excluded"
    assert "journal" in sleeper.assess(a, [a], 12, {"w1:p1": {"phase": "asleep"}})[1]
    assert "no transcript" in sleeper.assess(agent(uuid="other"), [agent(uuid="other")], 12, {})[1]


# ------------------------------------------------------ sleep state machine

def test_sleep_happy_path_signals_and_leaves_stub(fake: FakeHerdr) -> None:
    slept, outcome = sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    assert slept and outcome == "slept"
    assert fake.signals == [(1, signal.SIGTERM)]                     # the exit is a signal, not a typed command
    j = journal()["w1:p1"]
    assert j["uuid"] == UUID and j["kind"] == "claude" and j["phase"] == "asleep"
    assert j["argv"] == ["--dangerously-skip-permissions"] and j["terminal_id"] == "term-1"
    assert ("agent", "prompt", "w1:p1", "/exit") not in fake.calls
    run = stub_run(fake)
    assert str(SCRIPT) in run and "stub" in run and "w1:p1" in run and UUID in run
    assert "exec " not in run                                        # the pane's shell must survive beneath the stub
    assert any(c[:2] == ("pane", "rename") and c[3].startswith("💤") for c in fake.calls)
    assert any(c[:2] == ("pane", "report-agent") for c in fake.calls)  # sidebar claim
    assert fake.agents[0]["agent"] == sleeper.CLAIM_AGENT
    assert any(c[:2] == ("notification", "show") for c in fake.calls)
    assert events() == ["slept"]


def test_sleep_discards_draft_via_signal(fake: FakeHerdr) -> None:
    """A draft no longer blocks sleeping: SIGTERM discards it instead of the old
    /exit-appends-to-draft hazard. (The composer check still guards prompt-exit kinds.)"""
    fake.screen = CLAUDE_SCREEN.format(composer="unsent")
    slept, outcome = sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    assert slept and outcome == "slept" and journal()["w1:p1"]["phase"] == "asleep"


def test_sleep_refuses_when_state_moved_since_scan(fake: FakeHerdr) -> None:
    fake.agents[0]["state_change_seq"] = 6  # scan saw 5
    slept, outcome = sleeper.sleep_agent(agent(seq=5), 12, set(), dry_run=False)
    assert not slept and "state changed" in outcome
    assert not fake.signals and journal() == {}


def test_sleep_refuses_unreplayable_argv(fake: FakeHerdr) -> None:
    fake.argv = ["--fork-session"]
    slept, outcome = sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    assert not slept and "--fork-session" in outcome and journal() == {} and not fake.signals


def test_sleep_refuses_when_process_unidentifiable(fake: FakeHerdr) -> None:
    fake.pidless = True  # process-info answers, argv is readable, but no pid to signal
    slept, outcome = sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    assert not slept and "not identifiable" in outcome and journal() == {} and not fake.signals


def test_dry_run_runs_every_check_but_acts_on_nothing(fake: FakeHerdr) -> None:
    slept, outcome = sleeper.sleep_agent(agent(), 12, set(), dry_run=True)
    assert not slept and outcome.startswith("would sleep")
    assert not fake.signals and journal() == {}
    fake.agents[0]["state_change_seq"] = 6
    assert "state changed" in sleeper.sleep_agent(agent(seq=5), 12, set(), dry_run=True)[1]


def test_exit_timeout_keeps_handle_as_exit_requested(fake: FakeHerdr) -> None:
    fake.exit_leaves_agent = False  # SIGTERM delivered but the agent lingers
    slept, outcome = sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    assert not slept and "uncertain" in outcome
    assert journal()["w1:p1"]["phase"] == "exit-requested"
    assert events() == ["exit-uncertain"]


def test_exit_with_unreadable_process_info_stays_uncertain(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    # process-info works until the signal is sent, then becomes unreadable: Herdr drops the agent
    # but we cannot positively see the process go -> exit-requested, no pane run, handle kept
    real_kill = fake.kill

    def flaky(pid: int, sig: int) -> None:
        real_kill(pid, sig)
        fake.process_info_broken = True

    monkeypatch.setattr(sleeper.os, "kill", flaky)
    slept, outcome = sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    assert not slept and "uncertain" in outcome
    assert journal()["w1:p1"]["phase"] == "exit-requested"
    assert not any(c[:2] == ("pane", "run") for c in fake.calls)


def test_reconcile_settles_exit_requested(fake: FakeHerdr) -> None:
    fake.exit_leaves_agent = False
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    # case 1: the agent is still there next tick → first sighting only counts, second drops
    sleeper.reconcile(fake.agents)
    assert journal()["w1:p1"]["seen_running"] == 1
    sleeper.reconcile(fake.agents)
    assert journal() == {}
    # case 2: it did leave after the timeout → becomes asleep
    fake.exit_leaves_agent = False
    fake.agents = [agent()]
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    fake.agents[0].pop("agent"); fake.agents[0].pop("agent_session")
    sleeper.reconcile(fake.agents)
    assert journal()["w1:p1"]["phase"] == "asleep"


def test_reconcile_drops_entry_when_pane_runs_another_session(fake: FakeHerdr) -> None:
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    fake.agents[0].update(agent="claude", agent_session={"value": "other"})
    sleeper.reconcile(fake.agents)
    assert journal() == {} and events()[-1] == "reconciled"


# ------------------------------------------------------- wake state machine

def slept_entry(fake: FakeHerdr) -> dict[str, Any]:
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    return journal()["w1:p1"]


def test_wake_happy_path_restores_label(fake: FakeHerdr) -> None:
    fake.agents[0]["label"] = "mylabel"
    entry = slept_entry(fake)
    assert fake.agents[0]["label"] == "💤 mylabel"
    assert sleeper.wake_entry(entry)
    start = next(c for c in fake.calls if c[:2] == ("agent", "start"))
    assert start[-2:] == ("--resume", UUID) and "--dangerously-skip-permissions" in start
    assert any(c[:2] == ("pane", "release-agent") for c in fake.calls)   # claim dropped before the start
    assert fake.agents[0]["label"] == "mylabel" and fake.agents[0]["agent"] == "claude"
    assert journal() == {} and events()[-1] == "woke"


def test_wake_clears_label_that_did_not_exist_before_sleep(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    assert fake.agents[0]["label"] == "💤 Title" and entry["label"] is None
    assert sleeper.wake_entry(entry)
    assert fake.agents[0]["label"] is None


def test_wake_filters_snapshot_argv(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    entry["argv"] = ["--fork-session"]  # a raw snapshot argv that sleep never vetted
    assert not sleeper.wake_entry(entry) and "w1:p1" in journal()
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)


def test_read_json_rejects_non_object_root(state: Path) -> None:
    sleeper.STATE_DIR.mkdir(parents=True)
    sleeper.JOURNAL.write_text("[]\n")
    with pytest.raises(sleeper.DamagedState):
        sleeper.read_json(sleeper.JOURNAL)


def test_wake_quarantines_damaged_journal_before_snapshot_recovery(fake: FakeHerdr, capsys: pytest.CaptureFixture[str]) -> None:
    sleeper.merge_snapshot(fake.agents)
    fake.agents[0].pop("agent"); fake.agents[0].pop("agent_session")
    sleeper.JOURNAL.write_text("[]\n")
    assert sleeper.cmd_wake(types.SimpleNamespace(target="a", all=False)) == 0
    damaged = list(sleeper.STATE_DIR.glob("sleeping.json.damaged-*"))
    assert len(damaged) == 1 and damaged[0].read_text() == "[]\n"
    assert "damaged journal moved to" in capsys.readouterr().err


def test_reconcile_needs_a_real_process_before_dropping(fake: FakeHerdr) -> None:
    fake.exit_leaves_agent = False
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    fake.process_override = False  # agent list says present, process-info says no process
    sleeper.reconcile(fake.agents); sleeper.reconcile(fake.agents); sleeper.reconcile(fake.agents)
    assert journal()["w1:p1"]["phase"] == "exit-requested" and "seen_running" not in journal()["w1:p1"]


def test_reconcile_sightings_must_be_consecutive(fake: FakeHerdr) -> None:
    fake.exit_leaves_agent = False
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    sleeper.reconcile(fake.agents)                      # True
    fake.process_override = None; fake.process_info_broken = True
    sleeper.reconcile(fake.agents)                      # unknown -> streak resets
    fake.process_info_broken = False
    sleeper.reconcile(fake.agents)                      # True again: only one in a row
    assert "w1:p1" in journal()


def test_displaced_session_survives_reconcile(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.start_uuid = "fresh-uuid"
    assert not sleeper.wake_entry(entry)                # pane now runs fresh-uuid; entry kept with wake_got
    sleeper.merge_snapshot(fake.agents)
    sleeper.reconcile(fake.agents)
    j = journal()["w1:p1"]
    assert j["uuid"] == UUID and j["phase"] == "displaced" and j["argv"] == ["--dangerously-skip-permissions"]
    assert not sleeper.wake_entry(j)                    # manual-only from here


def test_wake_after_server_restart_wakes_the_restored_pane(fake: FakeHerdr) -> None:
    """A1: every Herdr restart re-allocates terminal ids; the restored pane keeps its 💤 label
    (restore re-applies manual labels), so same pane id + 💤 label + bare shell is the same pane."""
    entry = slept_entry(fake)
    fake.agents[0]["terminal_id"] = "term-2"   # restored by a restarted server: new terminal, same pane, same cwd
    fake.agents[0].pop("agent", None)          # restart cleared our claim; a bare shell is left
    assert fake.agents[0]["label"].startswith(sleeper.SLEEP_MARK)
    assert sleeper.wake_entry(entry) and journal() == {}
    assert "identity re-established" in sleeper.LOGFILE.read_text()


def test_restamp_is_written_only_when_the_wake_succeeds(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """F2: a refused wake must not leave the new terminal id on record (the same-terminal drop rule
    would then fire on a terminal that was never proven ours)."""
    entry = slept_entry(fake)
    fake.agents[0]["terminal_id"] = "term-2"
    fake.agents[0].pop("agent", None)
    monkeypatch.setattr(sleeper, "uuid_live_elsewhere", lambda uuid, except_pane=None, cwd=None: "pid 42")
    assert not sleeper.wake_entry(entry)
    assert journal()["w1:p1"]["terminal_id"] == "term-1"


def test_new_terminal_without_the_sleep_label_is_reuse(fake: FakeHerdr) -> None:
    """F1: a fresh pane that got a recycled pane id (workspace ids restart at max(restored)+1) in the
    same cwd carries no 💤 label: orphan, never resume into it."""
    entry = slept_entry(fake)
    fake.agents[0].update(terminal_id="term-2", label=None)   # same cwd, new terminal, no label
    fake.agents[0].pop("agent", None)
    assert not sleeper.wake_entry(entry)
    assert ORPHAN in journal() and not any(c[:2] == ("agent", "start") for c in fake.calls)


def test_restored_pane_moved_to_another_cwd_is_refused_not_orphaned(fake: FakeHerdr,
                                                                   capsys: pytest.CaptureFixture[str]) -> None:
    """F1: cwd is a wake precondition, not identity evidence: a restored pane cd'd elsewhere says "cd back"."""
    entry = slept_entry(fake)
    fake.agents[0].update(terminal_id="term-2", cwd="/somewhere/else")   # 💤 label kept by restore
    fake.agents[0].pop("agent", None)
    assert not sleeper.wake_entry(entry)
    sleeper.reconcile(fake.agents)
    assert "w1:p1" in journal() and "cd back" in sleeper.LOGFILE.read_text()


def test_legacy_entry_without_terminal_id_needs_the_sleep_label(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    j = journal(); j["w1:p1"]["terminal_id"] = None; sleeper.write_json(sleeper.JOURNAL, j)
    fake.agents[0].pop("agent", None)
    fake.agents[0]["label"] = "something the user set"
    assert not sleeper.wake_entry({**entry, "terminal_id": None})
    assert ORPHAN in journal()
    fake.agents[0]["label"] = "💤 Title"
    j = journal(); j["w1:p1"] = {**j.pop(ORPHAN), "phase": "asleep"}; sleeper.write_json(sleeper.JOURNAL, j)
    assert sleeper.wake_entry(journal()["w1:p1"]) and journal() == {}


def test_wake_after_restart_compares_cwd_by_realpath(fake: FakeHerdr, state: Path) -> None:
    real = state / "real"; real.mkdir()
    link = state / "link"; link.symlink_to(real)
    fake.agents[0]["cwd"] = str(real)
    sleeper.sleep_agent(agent(cwd=str(real)), 12, set(), dry_run=False)
    entry = journal()["w1:p1"]
    fake.agents[0].update(terminal_id="term-2", cwd=str(link))   # the precondition compares realpaths
    fake.agents[0].pop("agent", None)
    assert sleeper.wake_entry(entry) and journal() == {}


def test_wake_on_new_terminal_in_other_cwd_orphans_entry(fake: FakeHerdr) -> None:
    """A1/A2: a new terminal that does not carry our 💤 label -> a reused pane id; re-keyed, never dropped."""
    entry = slept_entry(fake)
    fake.agents[0].update(terminal_id="term-2", cwd="/somewhere/else", label=None)
    fake.agents[0].pop("agent", None)
    assert not sleeper.wake_entry(entry)
    j = journal()
    assert "w1:p1" not in j and j[ORPHAN]["phase"] == "orphaned" and j[ORPHAN]["uuid"] == UUID
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)
    assert events().count("orphaned") == 1


def test_wake_refuses_recycled_pane_by_cwd(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.agents[0]["cwd"] = "/somewhere/else"
    assert not sleeper.wake_entry(entry) and "w1:p1" in journal()
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)


def test_wake_refuses_when_pane_gone_and_keeps_entry(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.agents = []
    assert not sleeper.wake_entry(entry) and "w1:p1" in journal()


def test_wake_refuses_when_argv_unknown(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    entry["argv"] = None
    assert not sleeper.wake_entry(entry)
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)


def test_wake_refuses_when_session_live_elsewhere(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    entry = slept_entry(fake)
    monkeypatch.setattr(sleeper, "uuid_live_elsewhere", lambda uuid, except_pane=None, cwd=None: "pid 42")
    assert not sleeper.wake_entry(entry) and "w1:p1" in journal()


def test_wake_refuses_when_liveness_unknown(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    entry = slept_entry(fake)

    def boom(uuid: str, except_pane: str | None = None, cwd: str | None = None) -> None:
        raise sleeper.SleeperError("ps failed")

    monkeypatch.setattr(sleeper, "uuid_live_elsewhere", boom)
    with pytest.raises(sleeper.SleeperError):
        sleeper.wake_entry(entry)
    assert "w1:p1" in journal()


def test_wake_clears_entry_when_same_session_already_running(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.agents[0].update(agent="claude", agent_session={"value": UUID})  # came back by other means
    assert sleeper.wake_entry(entry) and journal() == {}


def test_wake_keeps_entry_when_agent_shown_but_no_process(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.agents[0].update(agent="claude", agent_session={"value": UUID})
    fake.process_info_broken = True
    assert not sleeper.wake_entry(entry) and "w1:p1" in journal()


def test_wake_keeps_entry_on_session_mismatch(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.start_uuid = "fresh-uuid"
    assert not sleeper.wake_entry(entry)
    assert journal()["w1:p1"]["wake_got"] == "fresh-uuid" and events()[-1] == "wake-mismatch"


def test_wake_with_stub_running_answers_y_instead_of_starting(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.stub_panes.add("w1:p1")
    assert sleeper.wake_entry(entry)
    keys = [c for c in fake.calls if c[:2] == ("pane", "send-keys")]
    assert keys == [("pane", "send-keys", "w1:p1", "y", "enter")]     # a bare Enter no longer wakes the stub
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)   # the stub execs the agent itself


def test_wake_sends_a_bare_enter_to_a_stub_left_by_an_older_version(fake: FakeHerdr) -> None:
    """A pane slept before this change runs a stub that wakes on a bare Enter and ignores a "y" line."""
    entry = slept_entry(fake)
    assert entry["stub_confirm"] is True                               # new sleeps record the prompting stub
    entry.pop("stub_confirm")
    fake.stub_panes.add("w1:p1")
    assert sleeper.wake_entry(entry)
    keys = [c for c in fake.calls if c[:2] == ("pane", "send-keys")]
    assert keys == [("pane", "send-keys", "w1:p1", "enter")]


def test_wake_from_snapshot_persists_before_trying(fake: FakeHerdr, capsys: pytest.CaptureFixture[str]) -> None:
    sleeper.merge_snapshot(fake.agents)
    fake.agents[0].pop("agent"); fake.agents[0].pop("agent_session")   # asleep, but no journal (crash)
    assert sleeper.cmd_wake(types.SimpleNamespace(target="a", all=False)) == 0
    assert (fake.agents[0].get("agent_session") or {}).get("value") == UUID
    assert "recovering" in capsys.readouterr().out


def test_snapshot_keeps_sleeping_panes(fake: FakeHerdr) -> None:
    sleeper.merge_snapshot(fake.agents)
    fake.agents[0].pop("agent"); fake.agents[0].pop("agent_session")   # now asleep: not in agent list
    sleeper.merge_snapshot([])
    assert "w1:p1" in sleeper.read_json(sleeper.SNAPSHOT)
    fake.agents[0].update(agent="claude", agent_session={"value": "different"})
    sleeper.merge_snapshot(fake.agents)
    assert sleeper.read_json(sleeper.SNAPSHOT)["w1:p1"]["uuid"] == "different"


# ------------------------------------------------------------------ stub

def test_stub_wakes_by_exec_after_y(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                        capsys: pytest.CaptureFixture[str]) -> None:
    entry = slept_entry(fake)
    fake.stub_panes.add("w1:p1")
    fake.agents[0].pop("agent", None)  # claimed placeholder is not a real agent for this path
    monkeypatch.setattr("builtins.input", lambda _prompt="": "y")
    monkeypatch.setattr(sleeper.os, "chdir", lambda _p: None)  # cwd check compares realpaths; entry cwd is /tmp/proj
    monkeypatch.setattr(sleeper.os, "getcwd", lambda: entry["cwd"])
    monkeypatch.setattr(sleeper.shutil, "which", lambda b: f"/opt/bin/{b}")
    monkeypatch.setattr(sleeper.subprocess, "Popen", lambda *a, **k: pytest.fail("no helper process carries the uuid"))
    execvps: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: execvps.append((b, a)))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert execvps == [("/opt/bin/claude", ["claude", "--dangerously-skip-permissions", "--resume", UUID])]
    row = journal()["w1:p1"]   # G1: kept as `waking` (same pid after exec) until reconcile sees the session
    assert row["phase"] == "waking" and row["waking_pid"] == os.getpid() and events()[-1] == "woke"
    assert any(c[:2] == ("pane", "release-agent") for c in fake.calls)


def prepare_stub(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, entry: dict[str, Any]) -> list[tuple[str, list[str]]]:
    """Make cmd_stub runnable in-process for `entry`'s pane: nothing is exec'd, the pane's cwd matches. Returns
    the list that records each execvp."""
    fake.stub_panes.add("w1:p1")
    fake.agents[0].pop("agent", None)
    monkeypatch.setattr(sleeper.os, "getcwd", lambda: entry["cwd"])
    monkeypatch.setattr(sleeper.shutil, "which", lambda b: f"/opt/bin/{b}")
    execvps: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: execvps.append((b, a)))
    return execvps


def test_stub_drops_typeahead_right_before_the_exec(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """A `y`+Enter that `wake` typed while the stub waited on the lock must not reach the agent's stdin."""
    entry = slept_entry(fake)
    order: list[Any] = []
    prepare_stub(fake, monkeypatch, entry)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: order.append("execvp"))
    monkeypatch.setattr(sleeper.sys, "stdin", types.SimpleNamespace(fileno=lambda: 7))
    monkeypatch.setattr(sleeper.termios, "tcflush", lambda fd, how: order.append(("tcflush", fd, how)))
    monkeypatch.setattr("builtins.input", lambda _prompt="": "y")
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert order == [("tcflush", 7, sleeper.termios.TCIFLUSH), "execvp"]


def test_stub_still_execs_when_the_typeahead_flush_fails(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    entry = slept_entry(fake)
    execvps = prepare_stub(fake, monkeypatch, entry)    # stdin under pytest has no fileno(): the flush raises

    def broken(fd: int, how: int) -> None:
        raise OSError("not a tty")

    monkeypatch.setattr(sleeper.termios, "tcflush", broken)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "y")
    assert sleeper.cmd_stub("w1:p1", UUID) == 0 and len(execvps) == 1


def test_stub_marks_its_own_row_as_prompting(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """A row written by a watcher still on old code has no `stub_confirm`; the stub that now runs is the authority."""
    entry = slept_entry(fake)
    entry.pop("stub_confirm")
    sleeper.write_json(sleeper.JOURNAL, {"w1:p1": entry})
    prepare_stub(fake, monkeypatch, entry)
    monkeypatch.setattr("builtins.input", lambda _prompt="": (_ for _ in ()).throw(EOFError()))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert journal()["w1:p1"]["stub_confirm"] is True and journal()["w1:p1"]["uuid"] == UUID


def test_stub_skips_the_mark_when_the_lock_stays_busy(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                                     capsys: pytest.CaptureFixture[str]) -> None:
    entry = slept_entry(fake)
    entry.pop("stub_confirm")
    sleeper.write_json(sleeper.JOURNAL, {"w1:p1": entry})
    prepare_stub(fake, monkeypatch, entry)
    monkeypatch.setattr(sleeper, "STUB_MARK_ATTEMPT_SECONDS", 0.3)  # the wait for the lock, short
    monkeypatch.setattr("builtins.input", lambda _prompt="": (_ for _ in ()).throw(EOFError()))
    with sleeper.locked():
        assert sleeper.cmd_stub("w1:p1", UUID) == 0                  # still prompts and returns; no exception
        for thread in threading.enumerate():                         # the retry stops with the prompt
            if thread.name == "stub-mark":
                thread.join(10)
        assert "stub_confirm" not in journal()["w1:p1"]               # the lock was ours throughout: no mark could land
    assert "Resume? [y/N]" in capsys.readouterr().out


def test_stub_prompt_line_drops_control_characters_from_the_title(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                                                 capsys: pytest.CaptureFixture[str]) -> None:
    entry = slept_entry(fake)
    sleeper.write_json(sleeper.JOURNAL, {"w1:p1": {**entry, "title": "evil\r\x1b[2Kfake"}})
    prepare_stub(fake, monkeypatch, entry)
    monkeypatch.setattr("builtins.input", lambda _prompt="": (_ for _ in ()).throw(EOFError()))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    out = capsys.readouterr().out
    assert "\r" not in out and "\x1b" not in out and "evil[2Kfake" in out


@pytest.mark.parametrize("failure", [OSError("EIO"), UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad"),
                                     RuntimeError("input(): lost sys.stdin")])
def test_stub_leaves_a_shell_when_input_fails(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                              capsys: pytest.CaptureFixture[str], failure: Exception) -> None:
    entry = slept_entry(fake)
    prepare_stub(fake, monkeypatch, entry)
    monkeypatch.setattr("builtins.input", lambda _prompt="": (_ for _ in ()).throw(failure))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert "resume with:" in capsys.readouterr().out and any(c[:2] == ("pane", "release-agent") for c in fake.calls)


def wake_typed_into_the_prompt(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, marked: bool) -> list[tuple[str, list[str]]]:
    """Round trip: `wake_entry` types into the pane and the stub's input() reads what the fake delivered.

    The first input() call is the moment a wake command arrives (as a real `herdr-sleeper wake` would, under the
    lock). `marked=False` is the row an old watcher wrote, before the new stub marked it."""
    entry = slept_entry(fake)
    if not marked:
        entry.pop("stub_confirm")
        sleeper.write_json(sleeper.JOURNAL, {"w1:p1": entry})
    execvps = prepare_stub(fake, monkeypatch, entry)
    asked: list[str] = []

    def terminal(_prompt: str = "") -> str:
        assert len(asked) < 3, "the stub is looping on input()"
        if not fake.stub_lines:
            with sleeper.locked():
                assert sleeper.wake_entry(journal()["w1:p1"]) == sleeper.DELEGATED
        asked.append(fake.stub_lines.pop(0))
        return asked[-1]

    monkeypatch.setattr("builtins.input", terminal)
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert asked == ["y"]                                            # the keys wake_entry typed, as a line
    return execvps


@pytest.mark.parametrize("marked", [True, False], ids=["new row", "row from a watcher on old code"])
def test_wake_keys_round_trip_through_the_stub_prompt(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, marked: bool) -> None:
    execvps = wake_typed_into_the_prompt(fake, monkeypatch, marked)
    assert execvps == [("/opt/bin/claude", ["claude", "--dangerously-skip-permissions", "--resume", UUID])]
    assert journal()["w1:p1"]["phase"] == "waking" and journal()["w1:p1"]["stub_confirm"] is True


@pytest.mark.parametrize("style,marked,execs", [
    ("new", True, True),       # the prompting stub is answered with y + Enter
    ("legacy", False, True),  # a stub from before the prompt wakes on a bare Enter
    ("new", False, False),    # what the stub's self-mark exists to prevent: a bare Enter at the prompt is ignored
    ("legacy", True, False),  # and `y` + Enter at an old stub is only echoed
])
def test_wake_entry_keys_wake_only_the_stub_flavour_they_are_meant_for(
        fake: FakeHerdr, style: str, marked: bool, execs: bool) -> None:
    entry = slept_entry(fake)
    if not marked:
        entry.pop("stub_confirm")
    fake.agents[0].pop("agent", None)
    fake.stub_panes.add("w1:p1")
    fake.stub_style = style
    fake.stub_execs_on_enter = True
    assert sleeper.wake_entry(entry) == sleeper.DELEGATED
    assert fake.stub_lines == (["y"] if marked else [""])
    assert bool(fake.agents[0].get("agent")) is execs                # the fake stub exec'd the agent back (or did not)
    assert ("w1:p1" in fake.stub_panes) is not execs


def test_stub_leaves_shell_on_interrupt(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                        capsys: pytest.CaptureFixture[str]) -> None:
    entry = slept_entry(fake)
    monkeypatch.setattr("builtins.input", lambda _prompt="": (_ for _ in ()).throw(KeyboardInterrupt()))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert "w1:p1" in journal()  # entry kept; the printed manual command still points at it
    assert "resume with:" in capsys.readouterr().out


def test_stub_refuses_when_session_live_elsewhere(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                                  capsys: pytest.CaptureFixture[str]) -> None:
    entry = slept_entry(fake)
    monkeypatch.setattr("builtins.input", lambda _prompt="": "y")
    monkeypatch.setattr(sleeper.os, "getcwd", lambda: entry["cwd"])   # the liveness check is what refuses here
    monkeypatch.setattr(sleeper.shutil, "which", lambda b: f"/opt/bin/{b}")
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("double resume"))
    monkeypatch.setattr(sleeper, "uuid_live_elsewhere", lambda uuid, except_pane=None, cwd=None: "pane w9:p9")
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert "w9:p9" in capsys.readouterr().out and "w1:p1" in journal()


# ------------------------------------------------------------- focus hook

def focus_env(monkeypatch: pytest.MonkeyPatch, pane: str, opt_in: bool = True) -> None:
    """The pane.focused hook's event. Wake-on-focus is off by default, so the tests that exercise the
    focus path opt in through the environment (the `state` fixture clears it between tests)."""
    monkeypatch.setenv("HERDR_PLUGIN_EVENT_JSON", json.dumps(
        {"event": "pane_focused", "data": {"type": "pane_focused", "pane_id": pane, "workspace_id": "w1"}}))
    if opt_in:
        monkeypatch.setenv("HERDR_SLEEPER_WAKE_ON_FOCUS", "1")


def test_on_focus_wakes_sleeping_pane(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    slept_entry(fake)
    focus_env(monkeypatch, "w1:p1")
    assert sleeper.cmd_on_focus() == 0
    assert any(c[:2] == ("agent", "start") for c in fake.calls) and journal() == {}


def test_on_focus_ignores_ordinary_panes(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    focus_env(monkeypatch, "w9:p9")  # nothing of ours there: no wake, no lock contention, fast exit
    assert sleeper.cmd_on_focus() == 0
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)


def test_on_focus_debounces_one_click(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    slept_entry(fake)
    focus_env(monkeypatch, "w1:p1")
    lock = sleeper.STATE_DIR / ".w1_p1.wakelock"  # pane.replace(":", "_") inside the leading dot
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("held")  # a sibling hook from the same click holds the per-pane lock
    assert sleeper.cmd_on_focus() == 0
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)


def test_on_focus_does_nothing_by_default(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """Visiting a workspace focuses its last pane; that must not resume a session nobody asked for."""
    slept_entry(fake)
    fake.calls.clear()
    focus_env(monkeypatch, "w1:p1", opt_in=False)
    assert sleeper.cmd_on_focus() == 0
    assert not any(c[:2] in (("agent", "start"), ("pane", "send-keys")) for c in fake.calls)
    assert "w1:p1" in journal() and journal()["w1:p1"]["phase"] == "asleep"


@pytest.mark.parametrize("setting,woken", [("false", False), ("true", True)])
def test_on_focus_follows_the_config_key(fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch,
                                         setting: str, woken: bool) -> None:
    slept_entry(fake)
    cfg = state / "config.toml"
    cfg.write_text(f"wake_on_focus = {setting}\n")
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    focus_env(monkeypatch, "w1:p1", opt_in=False)
    assert sleeper.cmd_on_focus() == 0
    assert any(c[:2] == ("agent", "start") for c in fake.calls) is woken


# ---------------------------------------------------------------- watcher

def test_watch_tick_sleeps_only_after_window(fake: FakeHerdr, state: Path,
                                             monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = state / "config.toml"
    cfg.write_text('idle = "1h"\n')
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    clocks: dict[str, Any] = {}
    clock = {"t": 1_000_000.0}
    monkeypatch.setattr(sleeper.time, "time", lambda: clock["t"])
    config, _src = sleeper.load_config()

    sleeper.watch_tick(clocks, config)
    assert journal() == {} and clocks["w1:p1"]["since"] == 1_000_000.0   # clock starts, nothing sleeps yet

    clock["t"] += 1800
    sleeper.watch_tick(clocks, config)
    assert journal() == {}                                       # half an hour is not an hour

    fake.agents[0]["state_change_seq"] = 6                        # activity between polls restarts the clock
    clock["t"] += 1800
    sleeper.watch_tick(clocks, config)
    assert journal() == {} and clocks["w1:p1"]["since"] == 1_003_600.0

    clock["t"] += 3700
    sleeper.watch_tick(clocks, config)
    assert journal()["w1:p1"]["phase"] == "asleep"                # now a full quiet hour has passed


def test_watch_tick_honors_excludes_and_kinds(fake: FakeHerdr, state: Path,
                                              monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = state / "config.toml"
    cfg.write_text('idle = "0s"\nexclude = ["a"]\n')
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    config, _src = sleeper.load_config()
    clocks: dict[str, Any] = {}
    sleeper.watch_tick(clocks, config)
    assert journal() == {} and clocks == {}                       # excluded before the clock even starts

    fake.agents[0].pop("name")
    sleeper.watch_tick(clocks, config)
    assert journal()["w1:p1"]["phase"] == "asleep"


# -------------------------------------------------------------- migration

def test_migration_adopts_legacy_journal(state: Path) -> None:
    legacy = state / "legacy"
    (legacy).mkdir(parents=True, exist_ok=True)
    (legacy / "sleeping.json").write_text(json.dumps({
        "w9:p1": {"pane_id": "w9:p1", "uuid": "legacy-uuid", "cwd": "/x",
                  "argv": ["--dangerously-skip-permissions"], "phase": "asleep"},
        "bad": "not an entry",
    }))
    sleeper.migrate_legacy()
    j = journal()
    assert j["w9:p1"]["uuid"] == "legacy-uuid" and j["w9:p1"]["kind"] == "claude"
    assert "bad" not in j and events() == ["migrated"]


def test_migration_is_once_and_opt_in(fake: FakeHerdr) -> None:
    (sleeper.STATE_DIR).mkdir(parents=True, exist_ok=True)
    sleeper.JOURNAL.write_text(json.dumps({"w1:p1": {"pane_id": "w1:p1", "uuid": "u", "kind": "claude"}}))
    sleeper.migrate_legacy()  # journal already exists → no touch
    assert journal()["w1:p1"]["uuid"] == "u" and events() == []


# ===================================================================== v0.1.1
# One block per fix-list item (A = session loss / double resume / silent stop,
# B = broken or misleading, C = low). Each test failed against v0.1.0.

class Ps:
    """A scripted `ps -eo pid=,args=` / `ps -p PID -o args=` for liveness and watcher checks."""

    def __init__(self, lines: list[str], fail: bool = False) -> None:
        self.lines, self.fail = lines, fail

    def __call__(self, cmd: list[str], **kw: Any) -> Any:
        if self.fail:
            raise OSError("ps: no such file")
        return types.SimpleNamespace(stdout="\n".join(self.lines) + "\n", returncode=0, stderr="")


def hold_flock(path: Path) -> Any:
    import fcntl
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = path.open("a+")
    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fh


# ------------------------------------------------------------- A2 orphans

def test_reconcile_orphans_reused_pane_id_running_another_session(fake: FakeHerdr) -> None:
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    fake.agents[0].update(agent="claude", agent_session={"value": "other"}, terminal_id="term-2")
    sleeper.reconcile(fake.agents)
    j = journal()
    assert "w1:p1" not in j and j[ORPHAN]["phase"] == "orphaned" and j[ORPHAN]["uuid"] == UUID
    assert j[ORPHAN]["orphaned_from"] == "w1:p1"
    sleeper.reconcile(fake.agents); sleeper.reconcile(fake.agents)
    assert events().count("orphaned") == 1 and ORPHAN in journal()   # one event, then silence


def test_reconcile_orphans_reused_bare_pane_in_other_cwd(fake: FakeHerdr) -> None:
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    fake.agents[0].pop("agent"); fake.agents[0].update(terminal_id="term-2", cwd="/elsewhere", label=None)
    sleeper.reconcile(fake.agents)
    assert ORPHAN in journal() and "w1:p1" not in journal()


def test_reconcile_keeps_entry_when_restart_kept_cwd(fake: FakeHerdr) -> None:
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    fake.agents[0].pop("agent"); fake.agents[0]["terminal_id"] = "term-2"   # restart: same pane, same cwd
    sleeper.reconcile(fake.agents)
    assert journal()["w1:p1"]["phase"] == "asleep"


def test_legacy_entry_without_terminal_id_is_orphaned_not_dropped(fake: FakeHerdr) -> None:
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    j = journal(); j["w1:p1"]["terminal_id"] = None; sleeper.write_json(sleeper.JOURNAL, j)
    fake.agents[0].update(agent="claude", agent_session={"value": "other"})
    sleeper.reconcile(fake.agents)
    assert ORPHAN in journal()  # same terminal cannot be proven: keep the handle


def test_orphans_are_never_auto_woken(fake: FakeHerdr, capsys: pytest.CaptureFixture[str],
                                      monkeypatch: pytest.MonkeyPatch) -> None:
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    fake.agents[0].update(agent="claude", agent_session={"value": "other"}, terminal_id="term-2")
    sleeper.reconcile(fake.agents)
    fake.agents[0].pop("agent"); fake.agents[0].pop("agent_session")
    focus_env(monkeypatch, "w1:p1")
    assert sleeper.cmd_on_focus() == 0
    assert sleeper.cmd_wake(types.SimpleNamespace(target=None, all=True)) == 0
    assert sleeper.cmd_wake(types.SimpleNamespace(target=ORPHAN, all=False)) == 1
    assert not any(c[:2] == ("agent", "start") for c in fake.calls) and ORPHAN in journal()
    capsys.readouterr()
    assert sleeper.cmd_list(types.SimpleNamespace()) == 0
    out = capsys.readouterr().out
    assert ORPHAN in out and "orphaned" in out and f"--resume {UUID}" in out


def test_hand_made_orphan_key_is_treated_as_orphan(fake: FakeHerdr, capsys: pytest.CaptureFixture[str]) -> None:
    sleeper.STATE_DIR.mkdir(parents=True, exist_ok=True)
    sleeper.write_json(sleeper.JOURNAL, {ORPHAN: {"pane_id": ORPHAN, "uuid": UUID, "kind": "claude",
                                                  "phase": "asleep", "cwd": "/p", "argv": []}})
    assert sleeper.cmd_wake(types.SimpleNamespace(target=None, all=True)) == 0
    sleeper.reconcile(fake.agents)
    assert not any(c[:2] in (("agent", "start"), ("pane", "get")) for c in fake.calls) and ORPHAN in journal()


def test_snapshot_keeps_an_orphan_copy_of_a_reused_pane(fake: FakeHerdr) -> None:
    sleeper.merge_snapshot(fake.agents)
    fake.agents[0].update(agent_session={"value": "different"}, terminal_id="term-2")
    sleeper.merge_snapshot(fake.agents)
    snap = sleeper.read_json(sleeper.SNAPSHOT)
    assert snap["w1:p1"]["uuid"] == "different" and snap[ORPHAN]["uuid"] == UUID


# ----------------------------------------------------- A3 / A4 watcher survives

def test_watch_tick_skips_claude_without_session(fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = state / "config.toml"; cfg.write_text('idle = "0s"\n'); monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    fake.agents[0].pop("agent_session")
    fake.agents.append({**agent(pane="w2:p1", name="b"), "agent_session": None})
    config, _ = sleeper.load_config()
    sleeper.watch_tick({}, config)  # v0.1.0: KeyError / AttributeError -> watcher gone
    assert journal() == {}


def run_loop(monkeypatch: pytest.MonkeyPatch, tick: Any, ticks: int) -> list[float]:
    """Drive watch_loop for `ticks` ticks of a scripted watch_tick; returns the requested sleeps."""
    sleeps: list[float] = []
    calls = {"n": 0}

    def fake_tick(clocks: dict[str, Any], config: dict[str, Any]) -> None:
        calls["n"] += 1
        if calls["n"] > ticks:
            sleeper.STOP["requested"] = True
            return
        tick(calls["n"])

    monkeypatch.setattr(sleeper, "watch_tick", fake_tick)
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(sleeper.signal, "signal", lambda *a: None)
    sleeper.watch_loop()
    assert calls["n"] == ticks + 1
    return sleeps


def test_watch_loop_survives_an_arbitrary_exception(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    def tick(n: int) -> None:
        if n == 1:
            raise KeyError("agent_session")

    run_loop(monkeypatch, tick, 3)
    assert "tick-error: KeyError" in sleeper.LOGFILE.read_text()


def test_watch_loop_never_exits_and_backs_off(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    def tick(n: int) -> None:
        raise sleeper.SleeperError("herdr agent list timed out")

    sleeps = run_loop(monkeypatch, tick, 15)  # v0.1.0 exited for good after 10
    assert sum(sleeps) > 15 * 60 and max(sleeps) <= 1.0     # interruptible 1s steps ...
    assert "backing off" in sleeper.LOGFILE.read_text()


def test_watch_loop_waits_for_the_watcher_lock(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                               capsys: pytest.CaptureFixture[str]) -> None:
    """C/F20: one watcher per state dir, enforced by a lock rather than a pid read-back race."""
    fh = hold_flock(sleeper.WATCHER_LOCK)
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    monkeypatch.setattr(sleeper, "watch_tick", lambda *a: pytest.fail("a second watcher ticked"))
    sleeper.watch_loop()
    fh.close()
    assert "another watcher" in capsys.readouterr().out


def test_heal_spawns_a_watcher_only_when_none_runs(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    spawned: list[dict[str, str]] = []
    monkeypatch.setattr(sleeper.subprocess, "Popen",
                        lambda argv, **kw: spawned.append(kw["env"]) or types.SimpleNamespace(pid=4242))
    monkeypatch.setenv("HERDR_SLEEPER_IDLE", "0s")
    real_heal = REAL_HEAL
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([]))   # no recorded watcher process
    real_heal()
    assert len(spawned) == 1 and "HERDR_SLEEPER_IDLE" not in spawned[0]   # a heal never inherits test overrides
    fh = hold_flock(sleeper.WATCHER_LOCK)
    real_heal()
    fh.close()
    assert len(spawned) == 1


def test_list_scan_sleep_pane_and_focus_heal_the_watcher(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    healed: list[int] = []
    monkeypatch.setattr(sleeper, "heal_watcher", lambda: healed.append(1))
    monkeypatch.setattr(sleeper, "list_agents", lambda: fake.agents)
    sleeper.cmd_list(types.SimpleNamespace())
    sleeper.cmd_scan(types.SimpleNamespace(dry_run=True, exclude=None))
    sleeper.cmd_sleep_pane(types.SimpleNamespace(pane="w1:p1", dry_run=True))
    focus_env(monkeypatch, "w9:p9"); sleeper.cmd_on_focus()
    assert len(healed) == 4


# ---------------------------------------------------- A5 structural stub match

def test_uuid_live_elsewhere_skips_only_a_real_stub(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "list_agents", lambda: [])
    stub = f"123 /usr/bin/python3 /x/plugins/herdr-sleeper/herdr-sleeper stub w1:p1 {UUID} /state"
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([stub]))
    assert REAL_UUID_LIVE(UUID, except_pane="w1:p1") is None
    sneaky = f"124 claude --resume {UUID} --add-dir /x/plugins/herdr-sleeper stub"
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([stub, sneaky]))
    assert REAL_UUID_LIVE(UUID, except_pane="w1:p1") == "pid 124"
    prompt = f"125 claude --resume {UUID} -- look at the herdr-sleeper stub bug"
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([prompt]))
    assert REAL_UUID_LIVE(UUID) == "pid 125"


def test_uuid_live_elsewhere_continue_cwd_and_ps_failure(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "list_agents", lambda: [])
    monkeypatch.setattr(sleeper.subprocess, "run", Ps(["7 claude --continue"]))
    monkeypatch.setattr(sleeper, "process_cwd", lambda pid: "/tmp/proj")
    assert "--continue" in REAL_UUID_LIVE(UUID, cwd="/tmp/proj")
    monkeypatch.setattr(sleeper, "process_cwd", lambda pid: None)
    with pytest.raises(sleeper.SleeperError):
        REAL_UUID_LIVE(UUID, cwd="/tmp/proj")
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([], fail=True))
    with pytest.raises(sleeper.SleeperError):
        REAL_UUID_LIVE(UUID)
    monkeypatch.setattr(sleeper, "list_agents", lambda: [agent(pane="w5:p5")])
    assert REAL_UUID_LIVE(UUID, except_pane="w1:p1") == "pane w5:p5"


def test_stub_running_is_structural(fake: FakeHerdr) -> None:
    fake.stub_panes.add("w1:p1")
    assert sleeper.stub_running("w1:p1", UUID) is True
    assert sleeper.stub_running("w1:p1", "another-uuid") is False
    fake.stub_panes.clear()
    fake.argv = ["--add-dir", "/p/herdr-sleeper", "stub"]   # a claude whose argv merely mentions us
    assert sleeper.stub_running("w1:p1", UUID) is False


# --------------------------------------------------------- A6 wake target

def test_wake_action_without_target_wakes_the_context_pane(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    fake.agents = [agent(pane="w1:p1", name=None), {**agent(pane="w2:p1", name="b", uuid="u2", terminal="term-b"),
                                                     "pid_hint": 2}]
    transcript(Path(str(sleeper.TRANSCRIPTS.parent)), "u2", 30)
    sleeper.sleep_agent(fake.agents[0], 12, set(), dry_run=False)
    sleeper.sleep_agent(fake.agents[1], 12, set(), dry_run=False)
    assert sleeper.find_journal_entry(None) is None
    monkeypatch.setenv("HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"focused_pane_id": "w2:p1"}))
    assert sleeper.cmd_wake(types.SimpleNamespace(target=None, all=False)) == 0
    starts = [c for c in fake.calls if c[:2] == ("agent", "start")]
    assert len(starts) == 1 and "w2:p1" in starts[0] and "w1:p1" in journal()


def test_wake_without_target_or_context_refuses(fake: FakeHerdr, capsys: pytest.CaptureFixture[str]) -> None:
    slept_entry(fake)
    assert sleeper.cmd_wake(types.SimpleNamespace(target=None, all=False)) == 2
    assert "w1:p1" in journal()


# ------------------------------------------------------------ A7 stub order

def stub_ready(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, lines: list[str | None] | None = None) -> dict[str, Any]:
    entry = slept_entry(fake)
    fake.stub_panes.add("w1:p1")
    feed = iter(lines if lines is not None else ["y"])

    def answer(_prompt: str = "") -> str:
        line = next(feed)   # a StopIteration here means the stub asked for more input than the test allowed
        if line is None:
            raise EOFError
        return line

    monkeypatch.setattr("builtins.input", answer)
    monkeypatch.setattr(sleeper.os, "getcwd", lambda: entry["cwd"])
    monkeypatch.setattr(sleeper.shutil, "which", lambda b: f"/opt/bin/{b}")
    return entry


def test_stub_exec_failure_reinserts_the_entry_and_reclaims(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                                            capsys: pytest.CaptureFixture[str]) -> None:
    stub_ready(fake, monkeypatch)

    def boom(b: str, a: list[str]) -> None:
        raise FileNotFoundError(b)

    monkeypatch.setattr(sleeper.os, "execvp", boom)
    fake.calls.clear()
    assert sleeper.cmd_stub("w1:p1", UUID) == 1
    row = journal()["w1:p1"]
    assert row["uuid"] == UUID and row["phase"] == "asleep" and "waking_pid" not in row
    assert events()[-1] == "wake-failed"
    assert any(c[:2] == ("pane", "report-agent") for c in fake.calls)   # the sidebar row is claimed back
    assert "manual" in capsys.readouterr().out


def test_stub_refuses_when_agent_not_on_path(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                             capsys: pytest.CaptureFixture[str]) -> None:
    stub_ready(fake, monkeypatch)
    monkeypatch.setattr(sleeper.shutil, "which", lambda b: None)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("exec without a binary"))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert "w1:p1" in journal() and "PATH" in capsys.readouterr().out


def test_stub_filters_recovered_argv(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_ready(fake, monkeypatch)
    j = journal(); j["w1:p1"]["argv"] = ["--resume", "other-uuid", "--fork-session"]; sleeper.write_json(sleeper.JOURNAL, j)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("exec of unvetted argv"))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0 and "w1:p1" in journal()


def test_stub_refuses_an_unknown_argv_instead_of_inventing_one(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                                               capsys: pytest.CaptureFixture[str]) -> None:
    stub_ready(fake, monkeypatch)
    j = journal(); j["w1:p1"]["argv"] = None; sleeper.write_json(sleeper.JOURNAL, j)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("exec with an invented argv"))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert "w1:p1" in journal() and "refusing to invent" in capsys.readouterr().out


def test_stub_refuses_when_the_pane_moved_to_another_cwd(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                                         capsys: pytest.CaptureFixture[str]) -> None:
    stub_ready(fake, monkeypatch)
    monkeypatch.setattr(sleeper.os, "getcwd", lambda: "/somewhere/else")
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("exec from the wrong directory"))
    fake.calls.clear()
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    out = capsys.readouterr().out
    assert "w1:p1" in journal() and journal()["w1:p1"]["phase"] == "asleep"   # the handle is kept
    assert "slept-in" in out and "manual:" in out
    assert any(c[:2] == ("pane", "release-agent") for c in fake.calls)         # the shell is the user's again


def test_stub_leaves_the_row_alone_when_it_changed_while_the_prompt_waited(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    stub_ready(fake, monkeypatch)
    j = journal(); j["w1:p1"]["phase"] = "waking"; j["w1:p1"]["waking_pid"] = 1; sleeper.write_json(sleeper.JOURNAL, j)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("second resume of a waking row"))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert journal()["w1:p1"]["phase"] == "waking" and "changed while waiting" in capsys.readouterr().out


def test_stub_rechecks_under_the_lock(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_ready(fake, monkeypatch)
    calls: list[str | None] = []

    def live(uuid: str, except_pane: str | None = None, cwd: str | None = None) -> str | None:
        calls.append(except_pane)
        return "pane w9:p9" if len(calls) > 1 else None   # a focus-wake raced us after the first look

    monkeypatch.setattr(sleeper, "uuid_live_elsewhere", live)
    real_locked = sleeper.locked

    class spy(real_locked):  # type: ignore[misc,valid-type]
        def __enter__(self) -> Any:
            calls.append("lock")
            return super().__enter__()

    monkeypatch.setattr(sleeper, "locked", spy)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("double resume"))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert "lock" in calls and calls.index("lock") < len(calls) - 1 and "w1:p1" in journal()


# ---------------------------------------------------- A8 exit-requested focus

def test_focus_during_exit_requested_does_not_clear_on_one_sighting(fake: FakeHerdr) -> None:
    fake.exit_leaves_agent = False
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    entry = journal()["w1:p1"]
    assert entry["phase"] == "exit-requested"
    assert not sleeper.wake_entry(entry, via="focus")
    assert journal()["w1:p1"]["phase"] == "exit-requested"


# ---------------------------------------------------------- A9 idle clocks

def test_idle_clock_survives_a_watcher_restart(fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = state / "config.toml"; cfg.write_text('idle = "1h"\n'); monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    config, _ = sleeper.load_config()
    clock = {"t": 1_000_000.0}
    monkeypatch.setattr(sleeper.time, "time", lambda: clock["t"])
    sleeper.watch_tick(sleeper.load_clocks(), config)
    clock["t"] += 3000
    sleeper.watch_tick(sleeper.load_clocks(), config)   # a new watcher process: clocks come from idle.json
    clock["t"] += 700
    sleeper.watch_tick(sleeper.load_clocks(), config)
    assert journal()["w1:p1"]["phase"] == "asleep"


def test_idle_clock_restarts_when_terminal_or_seq_differs(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    config = {**sleeper.DEFAULTS, "idle_hours": 1.0}
    monkeypatch.setattr(sleeper.time, "time", lambda: 1_000_000.0)
    for rec in ({"terminal_id": "term-OLD", "seq": 5, "since": 1.0},   # server restart / reused pane id
                {"terminal_id": "term-1", "seq": 4, "since": 1.0},      # activity
                {"terminal_id": "term-1", "seq": 5, "since": 2e9}):     # clock in the future
        clocks = {"w1:p1": dict(rec)}
        sleeper.watch_tick(clocks, config)
        assert clocks["w1:p1"]["since"] == 1_000_000.0 and journal() == {}


def test_damaged_idle_file_starts_clocks_now(state: Path) -> None:
    sleeper.STATE_DIR.mkdir(parents=True)
    sleeper.IDLE_FILE.write_text("[]\n")
    assert sleeper.load_clocks() == {}
    assert list(sleeper.STATE_DIR.glob("idle.json.damaged-*"))
    sleeper.IDLE_FILE.write_text(json.dumps({"w1:p1": {"since": "yesterday"}, "w2:p1": {"since": 5.0, "seq": 1,
                                                                                         "terminal_id": "t"}}))
    assert list(sleeper.load_clocks()) == ["w2:p1"]


# ----------------------------------------------------------- A10 watcher id

def test_watcher_alive_matches_argv_tokens(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper.os, "kill", lambda pid, sig: None)
    monkeypatch.setattr(sleeper.subprocess, "run", Ps(["cargo watch -x test"]))
    assert not sleeper.watcher_alive({"pid": 77})
    monkeypatch.setattr(sleeper.subprocess, "run", Ps(["/usr/bin/python3 /x/herdr-sleeper watch"]))
    assert sleeper.watcher_alive({"pid": 77})   # a pre-v0.1.1 record (no lock) is judged by argv alone


def test_watcher_alive_requires_the_lock_for_a_v011_record(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """F5: a reused pid that is another session's watcher must not pass for ours."""
    monkeypatch.setattr(sleeper.os, "kill", lambda pid, sig: None)
    monkeypatch.setattr(sleeper.subprocess, "run", Ps(["/usr/bin/python3 /x/herdr-sleeper watch"]))
    assert not sleeper.watcher_alive({"pid": 77, "lock": True})
    fh = hold_flock(sleeper.WATCHER_LOCK)
    assert sleeper.watcher_alive({"pid": 77, "lock": True})
    fh.close()


def test_heal_replaces_a_pre_v011_watcher(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """G3: v0.1.0 holds no watcher.lock; a heal (any focus, list, scan) replaces it, as startup would."""
    sleeper.STATE_DIR.mkdir(parents=True)
    sleeper.WATCHER_PID.write_text(json.dumps({"pid": 77, "stamp": "v0.1.0-stamp"}))
    signals: list[tuple[int, int]] = []
    gone = {"v": False}

    def kill(pid: int, sig: int) -> None:
        if sig == signal.SIGTERM:
            gone["v"] = True
        if sig == 0 and gone["v"]:
            raise ProcessLookupError(pid)
        signals.append((pid, sig))

    monkeypatch.setattr(sleeper.os, "kill", kill)
    monkeypatch.setattr(sleeper.subprocess, "run", Ps(["/usr/bin/python3 /orca/herdr-sleeper watch"]))
    spawned: list[int] = []
    monkeypatch.setattr(sleeper.subprocess, "Popen", lambda *a, **k: spawned.append(1) or types.SimpleNamespace(pid=88))
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    REAL_HEAL()
    assert gone["v"] and spawned == [1]


def test_ensure_watcher_never_signals_an_unverified_pid(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sleeper.STATE_DIR.mkdir(parents=True)
    sleeper.WATCHER_PID.write_text(json.dumps({"pid": 77, "stamp": "old"}))
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(sleeper.os, "kill", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(sleeper.subprocess, "run", Ps(["fswatch /tmp"]))   # pid 77 was reused
    monkeypatch.setattr(sleeper.subprocess, "Popen", lambda *a, **k: types.SimpleNamespace(pid=88))
    assert "spawned" in sleeper.ensure_watcher()
    assert (77, signal.SIGTERM) not in signals


# --------------------------------------------------------------- B items

def test_sleep_pane_on_the_focused_pane_sleeps(fake: FakeHerdr) -> None:
    fake.agents[0]["focused"] = True
    slept, outcome = sleeper.sleep_agent(agent(focused=True), 12, set(), dry_run=False, allow_focused=True)
    assert slept and outcome == "slept"
    assert not sleeper.sleep_agent(agent(pane="w1:p1", focused=True), 12, set(), dry_run=True)[0]


def test_stub_line_carries_no_control_bytes(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/s/herdr.sock")
    monkeypatch.setenv("HERDR_CLIENT_SOCKET_PATH", "/s/herdr-client.sock")
    slept_entry(fake)
    run = stub_run(fake)
    assert all(ord(c) >= 32 and ord(c) != 127 for c in run)            # nothing a line editor reads as keys
    assert "\\033[?1049l" in run and " env HERDR_SOCKET_PATH=/s/herdr.sock " in f" {run} "
    assert "HERDR_CLIENT_SOCKET_PATH" not in run                       # C/F19: the CLI routes by HERDR_SOCKET_PATH


def test_stub_prompt_names_the_pane_and_a_route_that_survives_a_restart(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    entry = stub_ready(fake, monkeypatch)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: None)
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    out = capsys.readouterr().out
    assert f"Sleeping: a · {entry['title']}. Resume? [y/N]" in out
    # Herdr's restore replays this text with no stub running: it must name something that still works
    assert f"{sleeper.SELF} wake a" in out


@pytest.mark.parametrize("answer", ["y", "Y", " y "])
def test_stub_wakes_on_y(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    stub_ready(fake, monkeypatch, [answer])
    execs: list[str] = []
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: execs.append(b))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert execs == ["/opt/bin/claude"]


@pytest.mark.parametrize("answer", ["", "   ", "n", "N", "yes", "yy", "ok", "do the thing", "y please"])
def test_stub_stays_asleep_on_anything_but_y(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, answer: str) -> None:
    """Enter alone is what helm keys, Collie and send-keys deliver by accident; none of it may wake."""
    stub_ready(fake, monkeypatch, [answer, answer, None])               # the stub is then left with EOF
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("woke without a y"))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert journal()["w1:p1"]["phase"] == "asleep" and "waking_pid" not in journal()["w1:p1"]
    assert "woke" not in events()


def test_stub_reprints_the_hint_once_then_waits_for_y(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                                      capsys: pytest.CaptureFixture[str]) -> None:
    stub_ready(fake, monkeypatch, ["do the thing", "", "", "n", "Y"])
    execs: list[str] = []
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: execs.append(b))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    out = capsys.readouterr().out
    assert execs == ["/opt/bin/claude"]                                  # woke on the Y, not before
    assert out.count("staying asleep") == 1 and "ignored 'do the thing'" in out


def test_stub_wake_restores_label_and_reconcile_hands_the_name_back(fake: FakeHerdr,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    """B4 + F7: the name comes back when reconcile finalises the `waking` row; no detached helper
    whose argv carries the uuid (it read as a live session to every wake for 90 s)."""
    fake.agents[0]["label"] = "mylabel"
    stub_ready(fake, monkeypatch)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: None)
    sleeper.cmd_stub("w1:p1", UUID)
    assert fake.agents[0]["label"] == "mylabel"
    fake.stub_panes.discard("w1:p1")                                   # the stub's process became claude (exec)
    fake.agents[0].update(agent="claude", agent_session={"value": UUID}, name=None)   # the exec'd agent, unnamed
    sleeper.reconcile(fake.agents)
    assert fake.agents[0]["name"] == "a" and journal() == {} and events()[-1] == "reconciled"


def test_reconcile_logs_a_name_it_could_not_give_back(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    stub_ready(fake, monkeypatch)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: None)
    sleeper.cmd_stub("w1:p1", UUID)
    fake.stub_panes.discard("w1:p1")                                   # the stub's process became claude (exec)
    fake.agents[0].update(agent="claude", agent_session={"value": UUID}, name=None)
    real = fake.__call__
    def rename_refused(*a: str, **k: Any) -> Any:
        if a[:2] == ("agent", "rename"):
            raise sleeper.SleeperError("herdr agent rename failed: agent_name_taken")
        return real(*a, **k)

    monkeypatch.setattr(sleeper, "herdr", rename_refused)
    sleeper.reconcile(fake.agents)
    assert journal() == {} and "could not give the name" in sleeper.LOGFILE.read_text()


def test_already_running_clear_path_restores_label(fake: FakeHerdr) -> None:
    fake.agents[0]["label"] = "mylabel"
    entry = slept_entry(fake)
    fake.agents[0].update(agent="claude", agent_session={"value": UUID})
    assert sleeper.wake_entry(entry) and fake.agents[0]["label"] == "mylabel"


def test_wake_via_stub_is_confirmed_before_reporting_success(fake: FakeHerdr, capsys: pytest.CaptureFixture[str]) -> None:
    slept_entry(fake)
    fake.agents[0].pop("agent", None)
    fake.stub_panes.add("w1:p1")
    assert sleeper.cmd_wake(types.SimpleNamespace(target="w1:p1", all=False)) == 1   # stub refused: not a success
    assert "wake-delegated-unconfirmed" in events()
    fake.stub_execs_on_enter = True
    assert sleeper.cmd_wake(types.SimpleNamespace(target="w1:p1", all=False)) == 0
    assert "woke 1/1" in capsys.readouterr().out


def test_boolean_env_overrides(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERDR_SLEEPER_NOTIFY", "false")
    monkeypatch.setenv("HERDR_SLEEPER_WAKE_ON_FOCUS", "0")
    values, _ = sleeper.load_config()
    assert values["notify"] is False and values["wake_on_focus"] is False
    monkeypatch.setenv("HERDR_SLEEPER_NOTIFY", "Yes")
    assert sleeper.load_config()[0]["notify"] is True
    monkeypatch.setenv("HERDR_SLEEPER_NOTIFY", "maybe")
    with pytest.raises(sleeper.ConfigError):
        sleeper.load_config()


def test_a_bad_wake_on_focus_value_is_an_error_and_the_focus_hook_does_nothing(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    slept_entry(fake)
    focus_env(monkeypatch, "w1:p1", opt_in=False)
    monkeypatch.setenv("HERDR_SLEEPER_WAKE_ON_FOCUS", "maybe")
    with pytest.raises(sleeper.ConfigError):
        sleeper.load_config()
    fake.calls.clear()
    assert sleeper.cmd_on_focus() == 0                                        # fail closed: no wake, no keys
    assert not any(c[:2] in (("agent", "start"), ("pane", "send-keys")) for c in fake.calls)
    assert journal()["w1:p1"]["phase"] == "asleep"


def test_wake_retries_once_with_the_pane_name_when_the_name_is_taken(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.taken_names = {"a"}
    assert sleeper.wake_entry(entry) and journal() == {}
    assert fake.agents[0]["started_name"] == "wake-w1-p1"
    fake.taken_names = {"a", "wake-w1-p1"}
    entry = slept_entry(fake)
    assert not sleeper.wake_entry(entry) and "w1:p1" in journal() and events()[-1] == "wake-failed"


def test_start_answering_before_the_session_is_known_is_not_a_mismatch(fake: FakeHerdr) -> None:
    entry = slept_entry(fake)
    fake.start_reports_session = False
    assert sleeper.wake_entry(entry) and journal() == {}             # learned from `pane get` shortly after
    entry = slept_entry(fake)
    fake.start_session_appears = False
    assert not sleeper.wake_entry(entry)
    assert "wake-mismatch" not in events() and events()[-1] == "wake-unconfirmed"
    assert journal()["w1:p1"]["wake_pending"] is True
    fake.agents[0]["agent_session"] = {"value": "fresh"}              # it turned out to be another session
    sleeper.reconcile(fake.agents)
    assert journal()["w1:p1"]["phase"] == "displaced"                 # kept, by hand only


def test_sigterm_lets_the_transaction_finish(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    sleeper.on_term(signal.SIGTERM, None)          # v0.1.0: sys.exit(0) right here, mid-transaction
    assert sleeper.STOP["requested"] is True
    sleeper.sleep_agent(agent(), 12, set(), dry_run=False)
    assert journal()["w1:p1"]["phase"] == "asleep"
    config = {**sleeper.DEFAULTS, "idle_hours": 0.0}
    fake.agents.append({**agent(pane="w2:p1", name="b", terminal="term-b"), "pid_hint": 2})
    sleeper.watch_tick({}, config)                 # a stopping watcher starts no new transaction
    assert "w2:p1" not in journal()


def test_agent_process_without_readable_argv_is_unknown(fake: FakeHerdr) -> None:
    fake.empty_argv = True
    assert sleeper.agent_process("w1:p1", "claude")[0] is None


def test_legacy_config_is_translated_once(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    legacy = state / "legacy-config.toml"
    legacy.write_text('idle_hours = 12\nexclude = ["orc", "orc-meta"]   # orchestrators\ninterval_minutes = 30\n')
    cfg = state / "cfg" / "config.toml"
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    sleeper.migrate_legacy_config()
    values, _ = sleeper.load_config()
    assert values["idle_hours"] == 12 and values["exclude"] == ["orc", "orc-meta"]
    assert "interval_minutes =" not in cfg.read_text() and events() == ["config-migrated"]
    cfg.write_text('idle = "2h"\n')
    sleeper.migrate_legacy_config()                 # an existing config is never rewritten ...
    assert cfg.read_text() == 'idle = "2h"\n'
    assert "is not read" in sleeper.LOGFILE.read_text()   # ... but the stray file is pointed out


def test_legacy_config_with_unknown_keys_is_not_migrated(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (state / "legacy-config.toml").write_text("idle_hours = 12\nsurprise = 1\n")
    cfg = state / "cfg" / "config.toml"
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    sleeper.migrate_legacy_config()
    assert not cfg.exists()


# --------------------------------------------------------------- C items

@pytest.fixture
def no_tomllib(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "tomllib", None)   # the 3.9 path, on every interpreter


def test_toml_subset_parses_the_documented_config(state: Path, monkeypatch: pytest.MonkeyPatch, no_tomllib: None) -> None:
    cfg = state / "config.toml"
    cfg.write_text('# comment\nidle = "90m"  # trailing\nexclude = ["pane#1", "orc"]\npoll_seconds = 30\n'
                   'notify = false\nagents = []\n')
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    values, _ = sleeper.load_config()
    assert values["idle_hours"] == 1.5 and values["exclude"] == ["pane#1", "orc"] and values["notify"] is False


@pytest.mark.parametrize("text", ['idle = "1h"\nidle = "2h"\n', 'exclude = ["a\\"b"]\n', 'exclude = ["a", 1]\n',
                                  "[table]\n", 'idle = "1h\n', "exclude = [\n"])
def test_toml_subset_refuses_what_it_cannot_parse_exactly(state: Path, monkeypatch: pytest.MonkeyPatch,
                                                          no_tomllib: None, text: str) -> None:
    cfg = state / "config.toml"
    cfg.write_text(text)
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    with pytest.raises(sleeper.ConfigError):
        sleeper.load_config()


def test_context_pane_ignores_a_leaked_pane_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERDR_PANE_ID", "w3:p1")                       # inherited from an outer session
    monkeypatch.setenv("HERDR_PLUGIN_CONTEXT_JSON", json.dumps({"workspace_id": "w1"}))
    assert sleeper.context_pane() is None
    monkeypatch.delenv("HERDR_PLUGIN_CONTEXT_JSON")
    assert sleeper.context_pane() == "w3:p1"                           # a manual run has no context to prefer


def test_ambiguous_name_refuses_with_candidates(fake: FakeHerdr) -> None:
    fake.agents = [agent(pane="w1:p1"), {**agent(pane="w2:p1", uuid="u2", terminal="term-b"), "pid_hint": 2}]
    transcript(Path(str(sleeper.TRANSCRIPTS.parent)), "u2", 30)
    sleeper.sleep_agent(fake.agents[0], 12, set(), dry_run=False)
    sleeper.sleep_agent(fake.agents[1], 12, set(), dry_run=False)
    with pytest.raises(sleeper.SleeperError, match="w1:p1.*w2:p1"):
        sleeper.find_journal_entry("a")
    assert sleeper.find_journal_entry("w2:p1")[0] == "w2:p1"


def test_done_status_survives_into_the_sleeping_label(fake: FakeHerdr) -> None:
    fake.agents[0]["agent_status"] = "done"
    sleeper.sleep_agent(agent(status="done"), 12, set(), dry_run=False)
    assert any(c[:2] == ("pane", "report-metadata") and "claude · sleeping · done" in c for c in fake.calls)


def test_focus_hook_gives_up_on_a_long_held_lock(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    slept_entry(fake)
    fh = hold_flock(sleeper.LOCK)
    focus_env(monkeypatch, "w1:p1")
    assert sleeper.cmd_on_focus() == 0                  # bounded wait, then the next focus retries
    fh.close()
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)
    assert "busy" in sleeper.LOGFILE.read_text()


# ================================================================ fix pass
# Second adversarial round on v0.1.1: F = review2-fable.md, G = review2-grok.md.

def test_cmd_wake_refuses_a_hand_made_orphan_key_with_a_pane_body(fake: FakeHerdr,
                                                                  capsys: pytest.CaptureFixture[str]) -> None:
    """F3: the key decides, not the body: copying an entry under `orphan:` must not wake it into
    whatever pane now has the body's pane id."""
    entry = slept_entry(fake)
    j = journal(); j[ORPHAN] = j.pop("w1:p1"); sleeper.write_json(sleeper.JOURNAL, j)   # pane_id still w1:p1
    fake.agents[0].pop("agent", None)
    assert sleeper.cmd_wake(types.SimpleNamespace(target=ORPHAN, all=False)) == 1
    assert sleeper.cmd_wake(types.SimpleNamespace(target="a", all=False)) == 1
    assert not sleeper.wake_entry(entry, key=ORPHAN)
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)
    assert f"--resume {UUID}" in capsys.readouterr().out


def test_sleep_refuses_to_overwrite_a_kept_handle(fake: FakeHerdr) -> None:
    """F4: sleep-pane skips assess; it must still never write over another session's handle."""
    sleeper.STATE_DIR.mkdir(parents=True, exist_ok=True)
    kept = {"pane_id": "w1:p1", "uuid": "kept-uuid", "kind": "claude", "phase": "displaced", "wake_got": UUID}
    sleeper.write_json(sleeper.JOURNAL, {"w1:p1": kept})
    slept, outcome = sleeper.sleep_agent(agent(), 12, set(), dry_run=False, allow_focused=True)
    assert not slept and "kept handle" in outcome and journal()["w1:p1"] == kept and not fake.signals


def test_watch_tick_survives_an_exception_in_one_pane(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """F6: one pane raising a non-SleeperError must not starve the panes after it or the clock write."""
    fake.agents = [agent(pane="w1:p1"), {**agent(pane="w2:p1", name="b", terminal="term-b", uuid="u2"), "pid_hint": 2}]
    transcript(Path(str(sleeper.TRANSCRIPTS.parent)), "u2", 30)
    real = sleeper.sleep_agent

    def flaky(a: dict[str, Any], *args: Any, **kw: Any) -> Any:
        if a["pane_id"] == "w1:p1":
            raise TypeError("pane get returned a list")
        return real(a, *args, **kw)

    monkeypatch.setattr(sleeper, "sleep_agent", flaky)
    sleeper.watch_tick({}, {**sleeper.DEFAULTS, "idle_hours": 0.0})
    assert journal()["w2:p1"]["phase"] == "asleep" and "TypeError" in sleeper.LOGFILE.read_text()
    assert "w1:p1" in json.loads(sleeper.IDLE_FILE.read_text())


def test_stub_refuses_without_a_transcript(fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch,
                                           capsys: pytest.CaptureFixture[str]) -> None:
    """F8: same gate as wake_entry: nothing to resume -> no exec, entry kept."""
    stub_ready(fake, monkeypatch)
    for t in (state / "projects").glob("*/*.jsonl"):
        t.unlink()
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("exec without a transcript"))
    assert sleeper.cmd_stub("w1:p1", UUID) == 0
    assert journal()["w1:p1"]["phase"] == "asleep" and "transcript" in capsys.readouterr().out


def test_scan_honours_the_persisted_idle_clock(fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch,
                                               capsys: pytest.CaptureFixture[str]) -> None:
    """F9: `scan` (no --dry-run) used to sleep everything at once, window ignored."""
    cfg = state / "config.toml"; cfg.write_text('idle = "1h"\n'); monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    monkeypatch.setattr(sleeper, "list_agents", lambda: fake.agents)
    sleeper.cmd_scan(types.SimpleNamespace(dry_run=False, exclude=None))
    assert journal() == {} and "idle 0.0h < 1h" in capsys.readouterr().out   # no clock: it starts now
    sleeper.STATE_DIR.mkdir(parents=True, exist_ok=True)
    sleeper.write_json(sleeper.IDLE_FILE, {"w1:p1": {"terminal_id": "term-1", "seq": 5, "uuid": UUID,
                                                     "since": time.time() - 2 * 3600}})
    sleeper.cmd_scan(types.SimpleNamespace(dry_run=False, exclude=None))
    assert journal()["w1:p1"]["phase"] == "asleep"


def test_idle_clock_restarts_when_the_session_differs(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """F9: the clock record carries the uuid: a claude replaced by another claude does not inherit it."""
    monkeypatch.setattr(sleeper.time, "time", lambda: 1_000_000.0)
    clocks = {"w1:p1": {"terminal_id": "term-1", "seq": 5, "uuid": "previous-session", "since": 1.0}}
    sleeper.watch_tick(clocks, {**sleeper.DEFAULTS, "idle_hours": 1.0})
    assert clocks["w1:p1"]["since"] == 1_000_000.0 and clocks["w1:p1"]["uuid"] == UUID and journal() == {}


def test_clock_reset_is_persisted_before_any_sleep(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """G6: a focus pops the clock; a crash before the end-of-tick write used to replay the old `since`."""
    fake.agents = [agent(pane="w1:p1", focused=True), {**agent(pane="w2:p1", name="b", terminal="term-b",
                                                              uuid="u2"), "pid_hint": 2}]
    transcript(Path(str(sleeper.TRANSCRIPTS.parent)), "u2", 30)
    sleeper.STATE_DIR.mkdir(parents=True, exist_ok=True)
    old = {"terminal_id": "term-1", "seq": 5, "uuid": UUID, "since": 1.0}
    seen: list[dict[str, Any]] = []

    def watch(a: dict[str, Any], *args: Any, **kw: Any) -> Any:
        seen.append(json.loads(sleeper.IDLE_FILE.read_text()))
        return False, "refused: test"

    monkeypatch.setattr(sleeper, "sleep_agent", watch)
    sleeper.watch_tick({"w1:p1": dict(old)}, {**sleeper.DEFAULTS, "idle_hours": 0.0})
    assert seen and "w1:p1" not in seen[0]   # the reset was on disk before the first sleep transaction


def test_focus_on_a_displaced_pane_is_silent(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """F10: the user keeps clicking into a pane running another session; no lock, no log line per click."""
    sleeper.STATE_DIR.mkdir(parents=True, exist_ok=True)
    sleeper.write_json(sleeper.JOURNAL, {"w1:p1": {"pane_id": "w1:p1", "uuid": "kept", "kind": "claude",
                                                   "phase": "displaced", "wake_got": UUID}})
    monkeypatch.setattr(sleeper, "load_config", lambda: pytest.fail("a displaced pane took the slow path"))
    focus_env(monkeypatch, "w1:p1")
    assert sleeper.cmd_on_focus() == 0 and not sleeper.LOGFILE.exists()


def test_await_stub_wake_reports_a_slow_session_as_started(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                                           capsys: pytest.CaptureFixture[str]) -> None:
    """F11: the stub exec'd (row `waking`, its pid alive) but Herdr has not attached the session yet."""
    slept_entry(fake)
    j = journal(); j["w1:p1"].update(phase="waking", waking_pid=4242); sleeper.write_json(sleeper.JOURNAL, j)
    monkeypatch.setattr(sleeper, "waking_alive", lambda e: True)
    assert sleeper.await_stub_wake(journal()["w1:p1"]) is True
    assert "not yet reported" in sleeper.LOGFILE.read_text() and "wake-delegated-unconfirmed" not in events()


# ------------------------------------------------------ G1/G2/G4 stub window

def test_ctrl_c_after_enter_puts_the_row_back(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch,
                                              capsys: pytest.CaptureFixture[str]) -> None:
    """G1: SIGINT between the journal write and the exec used to drop the only listed handle."""
    stub_ready(fake, monkeypatch)
    real = fake.__call__

    def interrupted(*a: str, **kw: Any) -> Any:
        if a[:2] == ("pane", "release-agent"):
            raise KeyboardInterrupt
        return real(*a, **kw)

    monkeypatch.setattr(sleeper, "herdr", interrupted)
    monkeypatch.setattr(sleeper.os, "execvp", lambda b, a: pytest.fail("exec after ctrl-c"))
    assert sleeper.cmd_stub("w1:p1", UUID) == 1
    row = journal()["w1:p1"]
    assert row["phase"] == "asleep" and "waking_pid" not in row and "manual" in capsys.readouterr().out


def test_a_waking_row_with_a_live_pid_is_never_woken_again(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """G1/G2: focus, wake, wake-all and snapshot recovery all go through wake_entry or see the row."""
    slept_entry(fake)
    fake.agents[0].pop("agent", None)
    j = journal(); j["w1:p1"].update(phase="waking", waking_pid=4242); sleeper.write_json(sleeper.JOURNAL, j)
    monkeypatch.setattr(sleeper, "waking_alive", lambda e: True)
    assert not sleeper.wake_entry(journal()["w1:p1"])
    assert sleeper.cmd_wake(types.SimpleNamespace(target=None, all=True)) == 1
    assert sleeper.cmd_wake(types.SimpleNamespace(target="a", all=False)) == 1
    focus_env(monkeypatch, "w1:p1"); sleeper.cmd_on_focus()
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)
    monkeypatch.setattr(sleeper, "waking_alive", lambda e: None)   # cannot tell -> same refusal
    assert not sleeper.wake_entry(journal()["w1:p1"])
    monkeypatch.setattr(sleeper, "waking_alive", lambda e: False)  # the stub died: an ordinary asleep row
    assert sleeper.wake_entry(journal()["w1:p1"]) and journal() == {}


def test_reconcile_restores_asleep_when_the_waking_stub_died(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    slept_entry(fake)
    fake.agents[0].pop("agent", None)
    j = journal(); j["w1:p1"].update(phase="waking", waking_pid=4242); sleeper.write_json(sleeper.JOURNAL, j)
    monkeypatch.setattr(sleeper, "waking_alive", lambda e: True)
    sleeper.reconcile(fake.agents)
    assert journal()["w1:p1"]["phase"] == "waking"           # still going: leave it
    monkeypatch.setattr(sleeper, "waking_alive", lambda e: False)
    fake.calls.clear()
    sleeper.reconcile(fake.agents)
    row = journal()["w1:p1"]
    assert row["phase"] == "asleep" and "waking_pid" not in row
    assert any(c[:2] == ("pane", "report-agent") for c in fake.calls)   # the sidebar row is claimed back


def test_reconcile_marks_a_waking_row_displaced_by_another_session(fake: FakeHerdr) -> None:
    slept_entry(fake)
    j = journal(); j["w1:p1"].update(phase="waking", waking_pid=4242); sleeper.write_json(sleeper.JOURNAL, j)
    fake.agents[0].update(agent="claude", agent_session={"value": "someone-else"})
    sleeper.reconcile(fake.agents)
    assert journal()["w1:p1"]["phase"] == "displaced" and journal()["w1:p1"]["wake_got"] == "someone-else"


def test_waking_alive_matches_the_stub_or_the_resumed_session(monkeypatch: pytest.MonkeyPatch) -> None:
    e = {"pane_id": "w1:p1", "uuid": UUID, "waking_pid": 4242}
    monkeypatch.setattr(sleeper.os, "kill", lambda pid, sig: None)
    for line, want in ((f"/usr/bin/python3 /x/herdr-sleeper stub w1:p1 {UUID} /s", True),
                       (f"claude --dangerously-skip-permissions --resume {UUID}", True),   # after the exec
                       ("vim notes.txt", False)):                                         # pid reused
        monkeypatch.setattr(sleeper.subprocess, "run", Ps([line]))
        assert sleeper.waking_alive(e) is want
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([], fail=True))
    assert sleeper.waking_alive(e) is None

    def dead(pid: int, sig: int) -> None:
        raise ProcessLookupError(pid)

    monkeypatch.setattr(sleeper.os, "kill", dead)
    assert sleeper.waking_alive(e) is False
    assert sleeper.waking_alive({**e, "waking_pid": None}) is False


def test_empty_foreground_list_is_unknown(fake: FakeHerdr) -> None:
    """G2: Herdr answers [] when it could not see the job; an idle shell still lists the shell."""
    entry = slept_entry(fake)
    fake.agents[0].pop("agent", None)
    fake.empty_foreground = True
    assert sleeper.agent_process("w1:p1", "claude")[0] is None
    assert sleeper.stub_running("w1:p1", UUID) is None
    assert not sleeper.wake_entry(entry) and "w1:p1" in journal()
    assert not any(c[:2] == ("agent", "start") for c in fake.calls)


def test_exec_failure_after_the_row_was_refilled_orphans_the_session(fake: FakeHerdr,
                                                                    monkeypatch: pytest.MonkeyPatch) -> None:
    """G4: never `setdefault`: if the pane's row now names another session, ours goes to an orphan key."""
    stub_ready(fake, monkeypatch)

    def boom(b: str, a: list[str]) -> None:
        j = journal(); j["w1:p1"] = {"pane_id": "w1:p1", "uuid": "newer", "kind": "claude", "phase": "asleep"}
        sleeper.write_json(sleeper.JOURNAL, j)
        raise PermissionError(b)

    monkeypatch.setattr(sleeper.os, "execvp", boom)
    assert sleeper.cmd_stub("w1:p1", UUID) == 1
    j = journal()
    assert j["w1:p1"]["uuid"] == "newer" and j[ORPHAN]["uuid"] == UUID and j[ORPHAN]["phase"] == "orphaned"


# --------------------------------------------------------------- G5 config

def test_plugin_config_without_exclude_refuses_while_a_legacy_one_has_it(state: Path,
                                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = state / "cfg" / "config.toml"; cfg.parent.mkdir(); cfg.write_text('idle = "12h"\n')
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    (state / "legacy-config.toml").write_text('exclude = ["orc", "orc-meta"]\ninterval_minutes = 30\n')
    with pytest.raises(sleeper.ConfigError, match="merge"):
        sleeper.load_config()
    monkeypatch.setenv("HERDR_SLEEPER_EXCLUDE", "orc")   # an explicit exclude settles it
    assert sleeper.load_config()[0]["exclude"] == ["orc"]
    monkeypatch.delenv("HERDR_SLEEPER_EXCLUDE")
    cfg.write_text('idle = "12h"\nexclude = []\n')        # so does an explicit empty list
    assert sleeper.load_config()[0]["exclude"] == []
    (state / "legacy-config.toml").write_text("interval_minutes = 30\n")
    cfg.write_text('idle = "12h"\n')                      # a legacy file without excludes is no reason to refuse
    assert sleeper.load_config()[0]["exclude"] == []


def test_focus_delegated_to_the_stub_finishes_the_row(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """A focus wake handed to the pane's stub confirms it like `wake` does (outside the global lock):
    the `waking` row is finished and the name handed back now, not a watcher tick later."""
    slept_entry(fake)
    fake.agents[0].pop("agent", None)
    fake.stub_panes.add("w1:p1")
    fake.stub_execs_on_enter = True
    j = journal(); j["w1:p1"].update(phase="waking", waking_pid=os.getpid())   # what the stub writes on Enter
    sleeper.write_json(sleeper.JOURNAL, {"w1:p1": {**j["w1:p1"], "phase": "asleep"}})
    real = fake.__call__

    def stub_marks_waking(*a: str, **kw: Any) -> Any:
        if a[:2] == ("pane", "send-keys") and any(fake.stub_accepts(line) for line in key_lines(a[3:])):
            sleeper.write_json(sleeper.JOURNAL, j)
        return real(*a, **kw)

    sleeper.herdr = stub_marks_waking  # restored by the fixture's monkeypatch of `herdr`
    focus_env(monkeypatch, "w1:p1")
    assert sleeper.cmd_on_focus() == 0
    assert journal() == {} and fake.agents[0]["name"] == "a"


# ============================================================ restore repair
# Herdr's restore relaunches a live claude as plain `claude --resume <uuid>`, dropping every launch flag.
# merge_snapshot keeps the flags it recorded before the restart (plus a `repair` marker), and
# repair_restored relaunches the pane once with them, under the wake gates.

FLAGS = ["--dangerously-skip-permissions", "--model", "haiku"]


def record(flags: list[str] | None = None, terminal: str = OLD_TERM, seen: str = "2026-10-08T10:00:00",
           pane: str = "w1:p1", uuid: str = UUID, **extra: Any) -> None:
    """Seed panes.json with what an earlier tick recorded for a live claude launched with `flags`."""
    snap = sleeper.read_json(sleeper.SNAPSHOT)
    snap[pane] = {"kind": "claude", "uuid": uuid, "name": "a", "cwd": "/tmp/proj", "title": "Title",
                  "argv": [*(FLAGS if flags is None else flags), "--resume", uuid],
                  "terminal_id": terminal, "seen_at": seen, **extra}
    sleeper.write_json(sleeper.SNAPSHOT, snap)


def restore(fake: FakeHerdr, terminal: str = NEW_TERM, argv: list[str] | None = None) -> None:
    """What a Herdr restart does to the pane: a new terminal id, claude relaunched with --resume alone."""
    fake.agents[0]["terminal_id"] = terminal
    fake.argv = ["--resume", UUID] if argv is None else argv


def repair_config(**over: Any) -> dict[str, Any]:
    return {**sleeper.DEFAULTS, **sleeper.RESTORE_DEFAULTS, "notify": False, "repair_kinds": list(sleeper.KINDS),
            "repair_bypass": True, **over}   # bypass flags on here: most tests restore FLAGS whole


def snap_row(pane: str = "w1:p1") -> dict[str, Any]:
    return sleeper.read_json(sleeper.SNAPSHOT)[pane]


def tick(config: dict[str, Any] | None = None) -> None:
    sleeper.watch_tick({}, config or repair_config())


def starts(fake: FakeHerdr) -> list[tuple[str, ...]]:
    return [c for c in fake.calls if c[:2] == ("agent", "start")]


# --- config: the [restore] table

RESTORE_BAD = [
    "[restore]\nrepair_flags = 1\n", '[restore]\nrepair_flags = "yes"\n', "[restore]\nrepair_window_minutes = 0\n",
    "[restore]\nrepair_window_minutes = -5\n", "[restore]\nrepair_window_minutes = true\n",
    "[restore]\nrepair_window_minutes = nan\n", "[restore]\nrepair_window_minutes = 2000\n",
    '[restore]\nrepair_window_minutes = "soon"\n', "[restore]\nbogus = 1\n", '[restore]\nidle = "1h"\n',
    "repair_flags = true\n", "restore = 3\n",
]


@pytest.mark.parametrize("subset", [False, True])
def test_restore_table_in_config(state: Path, monkeypatch: pytest.MonkeyPatch, subset: bool) -> None:
    if subset:
        monkeypatch.setattr(sleeper, "tomllib", None)   # the 3.9 reader
    cfg = state / "config.toml"
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    cfg.write_text('idle = "1h"\n\n[restore]\n# keep it short\nrepair_flags = false\nrepair_window_minutes = 5\n')
    values, source = sleeper.load_config()
    assert values["repair_flags"] is False and values["repair_window_minutes"] == 5.0 and values["idle_hours"] == 1
    assert source["repair_flags"] == str(cfg) and source["idle_hours"] == str(cfg)
    monkeypatch.setenv("HERDR_SLEEPER_REPAIR_FLAGS", "true")
    monkeypatch.setenv("HERDR_SLEEPER_REPAIR_WINDOW_MINUTES", "90")
    values, source = sleeper.load_config()
    assert values["repair_flags"] is True and values["repair_window_minutes"] == 90.0
    assert source["repair_flags"] == "$HERDR_SLEEPER_REPAIR_FLAGS"


@pytest.mark.parametrize("subset", [False, True])
@pytest.mark.parametrize("text", RESTORE_BAD)
def test_restore_table_rejects_bad_values(state: Path, monkeypatch: pytest.MonkeyPatch, subset: bool, text: str) -> None:
    if subset:
        monkeypatch.setattr(sleeper, "tomllib", None)
    cfg = state / "config.toml"
    cfg.write_text(text)
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    with pytest.raises(sleeper.ConfigError):
        sleeper.load_config()


def test_env_rejects_a_bad_repair_boolean(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "CONFIG_FILE", state / "absent.toml")
    monkeypatch.setenv("HERDR_SLEEPER_REPAIR_FLAGS", "maybe")
    with pytest.raises(sleeper.ConfigError):
        sleeper.load_config()


@pytest.mark.parametrize("text", ["[restore]\n[restore]\n", "[[restore]]\n", "restore.repair_flags = true\n",
                                  "[restore.x]\n", "[restore]\nrepair_flags = true\nrepair_flags = false\n",
                                  'restore = 1\n[restore]\n'])
def test_toml_subset_refuses_what_it_cannot_read_exactly(state: Path, text: str) -> None:
    cfg = state / "c.toml"
    cfg.write_text(text)
    with pytest.raises(sleeper.ConfigError):
        sleeper._toml_subset(cfg)


def test_toml_subset_reads_one_level_of_tables(state: Path) -> None:
    cfg = state / "c.toml"
    cfg.write_text('notify = false\n[restore]\nrepair_flags = true  # on\n')
    assert sleeper._toml_subset(cfg) == {"notify": False, "restore": {"repair_flags": True}}


def test_repair_keys_are_not_migrated_from_older_configs() -> None:
    assert not {"repair_flags", "repair_window_minutes"} & sleeper.MIGRATABLE_KEYS


# --- the recorded flags survive the restore (merge_snapshot / richer_record)

def test_merge_snapshot_keeps_the_richer_record_for_a_bare_restored_pane(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    sleeper.merge_snapshot(fake.agents)
    row = snap_row()
    assert row["argv"] == FLAGS                       # not overwritten by the bare relaunch
    assert row["terminal_id"] == OLD_TERM and row["seen_at"] == "2026-10-08T10:00:00"   # the observation it is
    marker = row["repair"]
    assert marker["state"] == "pending" and marker["terminal_id"] == NEW_TERM and marker["from_terminal"] == OLD_TERM
    sleeper.merge_snapshot(fake.agents)                                    # idempotent: the window does not restart
    assert snap_row()["repair"]["first_seen_ts"] == marker["first_seen_ts"]
    assert snap_row()["argv"] == FLAGS


def test_bare_claude_in_the_same_terminal_is_recorded_as_it_is(fake: FakeHerdr) -> None:
    """No restart (same terminal id): someone ran it bare on purpose."""
    record(terminal=OLD_TERM)
    fake.agents[0]["terminal_id"] = OLD_TERM
    fake.argv = ["--resume", UUID]
    sleeper.merge_snapshot(fake.agents)
    assert snap_row()["argv"] == [] and "repair" not in snap_row()


def test_merge_snapshot_without_repair_records_the_live_argv(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    sleeper.merge_snapshot(fake.agents, repair=False)
    assert snap_row()["argv"] == [] and "repair" not in snap_row()


def test_merge_snapshot_does_not_use_a_record_kept_under_another_pane(fake: FakeHerdr) -> None:
    """Replaces the old "matches by session, not pane id" test, which pinned the weak rule: a record of the session under
    ANOTHER pane's key is what an operator's bare resume in a new pane or tab looks like once the old pane is closed."""
    record(pane="w9:p9")                       # recorded under a pane id that is not the live one
    restore(fake)
    sleeper.merge_snapshot(fake.agents)
    assert snap_row()["argv"] == [] and "repair" not in snap_row()    # recorded as it is: nothing to restore
    assert snap_row("w9:p9")["argv"] == [*FLAGS, "--resume", UUID]   # the old row is left alone, as it was seeded


def test_merge_snapshot_process_info_hiccup_keeps_a_pending_repair(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    sleeper.merge_snapshot(fake.agents)
    fake.process_info_broken = True
    sleeper.merge_snapshot(fake.agents)
    assert snap_row()["argv"] == FLAGS and snap_row()["repair"]["state"] == "pending"


def test_merge_snapshot_drops_a_pending_marker_when_the_pane_is_gone(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    sleeper.merge_snapshot(fake.agents)
    fake.agents[0].pop("agent")
    fake.agents[0].pop("agent_session")
    sleeper.merge_snapshot(fake.agents)
    assert "repair" not in snap_row() and snap_row()["argv"] == FLAGS   # the handle stays


def test_find_journal_entry_does_not_recover_the_repair_marker(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    sleeper.merge_snapshot(fake.agents)
    hit = sleeper.find_journal_entry("a")
    assert hit and hit[2] and "repair" not in hit[1] and hit[1]["argv"] == FLAGS


@pytest.mark.parametrize(
    "live, recorded, terminal, found",
    [
        (["--resume", UUID], FLAGS, NEW_TERM, True),
        (["--dangerously-skip-permissions", "--resume", UUID], FLAGS, NEW_TERM, True),    # a strict subset
        ([f"--resume={UUID}"], FLAGS, NEW_TERM, True),
        (["--model", "opus", "--resume", UUID], FLAGS, NEW_TERM, False),                  # another value: not a subset
        ([*FLAGS, "--resume", UUID], FLAGS, NEW_TERM, False),                              # nothing was lost
        (["--resume", UUID], [], NEW_TERM, False),                                         # nothing to restore
        (["--resume", UUID], ["-p", "x"], NEW_TERM, False),                                # recorded argv is not replayable
        (["--resume", UUID], ["--fork-session"], NEW_TERM, False),
        (["--resume", UUID, "do the thing"], FLAGS, NEW_TERM, False),                      # live positional input
        (["--dangerously-skip-permissions"], FLAGS, NEW_TERM, False),                      # live does not select the session
        (["--resume", "other-uuid"], FLAGS, NEW_TERM, False),
        (["--resume", UUID], FLAGS, OLD_TERM, False),                                      # the terminal that recorded it
        (["--resume", UUID], FLAGS, None, False),                                          # terminal unknown
    ],
)
def test_richer_record_rules(state: Path, live: list[str], recorded: list[str], terminal: str | None, found: bool) -> None:
    record(flags=recorded)
    hit = sleeper.richer_record(sleeper.read_json(sleeper.SNAPSHOT), UUID, live, terminal, pane="w1:p1")
    assert (hit is not None) is found


def test_richer_record_needs_the_latest_observation_to_be_the_rich_one(state: Path) -> None:
    record(seen="2026-10-08T10:00:00")
    record(pane="w9:p9", flags=[], terminal="term-0", seen="2026-10-08T09:00:00")        # older and bare: irrelevant
    snap = sleeper.read_json(sleeper.SNAPSHOT)
    assert sleeper.richer_record(snap, UUID, ["--resume", UUID], NEW_TERM, pane="w1:p1")[0] == "w1:p1"
    record(pane="w9:p9", flags=[], terminal="term-0", seen="2026-10-08T11:00:00")        # later and bare: on purpose
    snap = sleeper.read_json(sleeper.SNAPSHOT)
    assert sleeper.richer_record(snap, UUID, ["--resume", UUID], NEW_TERM, pane="w1:p1") is None
    record(pane="w9:p9", flags=[], terminal="term-0", seen=None)                         # no timestamp: unknown
    snap = sleeper.read_json(sleeper.SNAPSHOT)
    assert sleeper.richer_record(snap, UUID, ["--resume", UUID], NEW_TERM, pane="w1:p1")[0] == "w1:p1"


# --- the repair itself (watch_tick)

def test_restore_repair_relaunches_a_bare_restored_pane_with_its_flags(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    fake.agents[0]["label"] = "mylabel"
    record()
    restore(fake)
    at_kill: dict[str, Any] = {}
    real_kill = fake.kill

    def kill(pid: int, sig: int) -> None:
        at_kill.update(journal()["w1:p1"])     # the handle is on disk before the exit
        real_kill(pid, sig)

    monkeypatch.setattr(sleeper.os, "kill", kill)
    tick()
    assert at_kill["phase"] == "exit-requested" and at_kill["restore_repair"] is True and at_kill["argv"] == FLAGS
    assert fake.signals == [(1, signal.SIGTERM)]
    (start,) = starts(fake)
    assert start[2] == "a" and start[start.index("--") + 1:] == (*FLAGS, "--resume", UUID)   # name and flags
    assert journal() == {} and fake.agents[0]["label"] == "mylabel"
    assert events() == ["repaired"]                                          # one event line per repair
    assert not any(c[:2] == ("pane", "send-keys") for c in fake.calls)       # a repair never types into a pane
    last = json.loads(sleeper.EVENTS.read_text().splitlines()[-1])
    assert last["flags"] == ["--dangerously-skip-permissions", "--model"] and last["from_terminal"] == OLD_TERM and last["terminal"] == NEW_TERM
    assert snap_row()["repair"]["state"] == "repaired"


def test_restore_repair_notifies_when_notifications_are_on(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    tick(repair_config(notify=True))
    assert any(c[:2] == ("notification", "show") for c in fake.calls)


def test_restore_repair_is_one_attempt_per_session_and_terminal(fake: FakeHerdr) -> None:
    """The fake keeps reporting a bare argv after the start: a second tick must not relaunch again, which is also
    what leaves a pane the operator started bare by hand alone after its one repair."""
    record()
    restore(fake)
    tick()
    tick()
    tick()
    assert len(fake.signals) == 1 and len(starts(fake)) == 1
    row = snap_row()
    assert row["argv"] == [] and row["terminal_id"] == NEW_TERM and row["repair"]["state"] == "repaired"


def test_restore_repair_runs_again_after_the_next_restart(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    fake.start_updates_argv = True              # a real relaunch: the process carries the flags again
    record()
    restore(fake)
    tick()
    tick()
    assert snap_row()["argv"] == FLAGS and snap_row()["terminal_id"] == NEW_TERM
    monkeypatch.setattr(sleeper, "herdr_server_started", lambda: LATER_START)   # a second server run ...
    restore(fake, THIRD_TERM)                     # ... whose restore allocated this terminal
    tick()
    assert len(fake.signals) == 2 and starts(fake)[1][starts(fake)[1].index("--") + 1:] == (*FLAGS, "--resume", UUID)
    assert events() == ["repaired", "repaired"] and snap_row()["repair"]["terminal_id"] == THIRD_TERM


def test_restore_repair_does_not_touch_a_pane_that_was_not_restored(fake: FakeHerdr) -> None:
    record(terminal=OLD_TERM)
    fake.argv = ["--resume", UUID]
    tick()
    assert fake.signals == [] and events() == [] and "repair" not in snap_row()


def test_restore_repair_does_not_act_on_a_record_kept_under_another_pane(fake: FakeHerdr) -> None:
    """Replaces "matches by session, not pane id": that rule repaired the operator's deliberate bare resume in a new pane."""
    record(pane="w9:p9")                        # the record lives under another pane key
    restore(fake)
    tick()
    tick()
    assert fake.signals == [] and starts(fake) == [] and events() == [] and "repair" not in snap_row()


GATES = {
    "focused": (lambda f, s, mp: f.agents[0].update(focused=True),
                lambda f, s, mp: f.agents[0].update(focused=False), "focused"),
    "working": (lambda f, s, mp: f.agents[0].update(agent_status="working"),
                lambda f, s, mp: f.agents[0].update(agent_status="idle"), "status working"),
    "draft": (lambda f, s, mp: setattr(f, "screen", "⏺ pong\n────────\n❯ half-typed\n────────\n  ◤ graft\n"),
              lambda f, s, mp: setattr(f, "screen", EMPTY_SCREEN), "composer draft"),
    "unreadable": (lambda f, s, mp: setattr(f, "screen", None),
                   lambda f, s, mp: setattr(f, "screen", EMPTY_SCREEN), "composer unknown"),
    "other-pane": (lambda f, s, mp: f.agents.append(agent(pane="w1:p2", name="b", terminal="term-9")),
                   lambda f, s, mp: f.agents.pop(), "also open in w1:p2"),
    "cwd": (lambda f, s, mp: f.agents[0].update(cwd="/elsewhere"),
            lambda f, s, mp: f.agents[0].update(cwd="/tmp/proj"), "cd back"),
    "transcript": (lambda f, s, mp: (s / "projects" / "proj" / f"{UUID}.jsonl").unlink(),
                   lambda f, s, mp: transcript(s, UUID, 30), "no transcript"),
    "live-process": (lambda f, s, mp: mp.setattr(sleeper, "uuid_live_elsewhere", lambda *a, **k: "pid 9"),
                     lambda f, s, mp: mp.setattr(sleeper, "uuid_live_elsewhere", lambda *a, **k: None), "live in pid 9"),
    "unverifiable": (lambda f, s, mp: mp.setattr(sleeper, "uuid_live_elsewhere", lambda *a, **k: (_ for _ in ()).throw(
                         sleeper.SleeperError("ps failed"))),
                     lambda f, s, mp: mp.setattr(sleeper, "uuid_live_elsewhere", lambda *a, **k: None), "ps failed"),
    "journal": (lambda f, s, mp: sleeper.write_json(sleeper.JOURNAL, {"w9:p9": {
                    "pane_id": "w9:p9", "uuid": UUID, "kind": "claude", "phase": "asleep"}}),
                lambda f, s, mp: sleeper.write_json(sleeper.JOURNAL, {}), "sleep journal"),
}


@pytest.mark.parametrize("gate", [g for g in GATES if g != "other-pane"])
def test_restore_repair_waits_on_every_gate_then_repairs(gate: str, fake: FakeHerdr, state: Path,
                                                         monkeypatch: pytest.MonkeyPatch) -> None:
    record()
    restore(fake)
    block, unblock, needle = GATES[gate]
    block(fake, state, monkeypatch)
    tick()
    tick()
    assert fake.signals == [] and starts(fake) == [] and "w1:p1" not in journal()
    row = snap_row()
    assert row["repair"]["state"] == "pending" and needle in row["repair"]["reason"]
    assert row["argv"] == FLAGS                         # the flags survive the wait
    unblock(fake, state, monkeypatch)
    tick()
    assert len(fake.signals) == 1 and events() == ["repaired"] and snap_row()["repair"]["state"] == "repaired"


def test_a_second_live_pane_of_the_session_holds_the_repair_back_and_its_bare_row_then_cancels_it(
        fake: FakeHerdr) -> None:
    """With the record matched only under the pane's own key, the second pane is not repaired itself (it holds no record);
    while it is open the session is "also open" and the repair waits; once it is gone its bare row is the newest
    observation of the session, which reads as "someone ran it bare on purpose": nothing is repaired. Fail safe."""
    record()
    restore(fake)
    fake.agents.append(agent(pane="w1:p2", name="b", terminal="term-9"))
    tick()
    assert fake.signals == [] and starts(fake) == []
    assert snap_row()["repair"]["state"] == "pending" and "also open in w1:p2" in snap_row()["repair"]["reason"]
    assert "repair" not in snap_row("w1:p2") and snap_row("w1:p2")["argv"] == []
    fake.agents.pop()
    tick()
    assert fake.signals == [] and starts(fake) == [] and "repair" not in snap_row()


def test_restore_repair_gives_up_after_the_window(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    record()
    restore(fake)
    fake.agents[0]["focused"] = True
    clock = {"t": 1_000_000.0}
    monkeypatch.setattr(sleeper.time, "time", lambda: clock["t"])
    tick()                                                  # noticed, and held back
    clock["t"] += 29 * 60
    tick()
    assert snap_row()["repair"]["state"] == "pending"
    clock["t"] += 2 * 60                                    # 31 minutes after it was first noticed
    tick()
    assert snap_row()["repair"]["state"] == "gave-up" and events() == ["repair-gave-up"]
    fake.agents[0]["focused"] = False
    tick()
    tick()
    assert fake.signals == [] and events() == ["repair-gave-up"]            # settled: never retried
    assert snap_row()["argv"] == []                         # and the live argv is recorded as it is


def test_restore_repair_window_is_configurable(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    record()
    restore(fake)
    fake.agents[0]["focused"] = True
    clock = {"t": 1_000_000.0}
    monkeypatch.setattr(sleeper.time, "time", lambda: clock["t"])
    config = repair_config(repair_window_minutes=2.0)
    tick(config)
    clock["t"] += 90
    tick(config)
    assert snap_row()["repair"]["state"] == "pending"
    clock["t"] += 60
    tick(config)
    assert snap_row()["repair"]["state"] == "gave-up"


def test_restore_repair_off_leaves_the_snapshot_and_the_pane_alone(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    tick(repair_config(repair_flags=False))
    assert fake.signals == [] and starts(fake) == []
    assert snap_row()["argv"] == [] and "repair" not in snap_row()


def test_restore_repair_does_not_depend_on_the_managed_agents(fake: FakeHerdr) -> None:
    """`agents` says what may be put to sleep; repair has its own `repair_kinds` (v0.1.2)."""
    record()
    restore(fake)
    tick(repair_config(agents=["opencode"]))
    assert len(fake.signals) == 1 and events() == ["repaired"]


def test_restore_repair_kinds_narrow_it(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    tick(repair_config(repair_kinds=["codex"]))
    assert fake.signals == [] and "repair" not in snap_row()
    record()                                    # the tick above recorded the bare relaunch: start again
    tick(repair_config(repair_kinds=[]))
    assert fake.signals == [] and "repair" not in snap_row()
    record()
    tick(repair_config(repair_kinds=["claude", "codex"]))
    assert len(fake.signals) == 1 and events() == ["repaired"]


@pytest.mark.parametrize("exclude", [["a"], ["w1:p1"]])
def test_restore_repair_honours_the_exclude_list_like_sleep_does(fake: FakeHerdr, exclude: list[str]) -> None:
    """v0.1.2 review M1: an excluded pane (by name or pane id) is never signalled, restored or not."""
    record()
    restore(fake)
    tick(repair_config(exclude=exclude))
    tick(repair_config(exclude=exclude))
    assert fake.signals == [] and starts(fake) == [] and events() == []


def test_restore_repair_covers_opencode_with_its_own_flags(fake: FakeHerdr) -> None:
    fake.agents[0]["agent"] = "opencode"
    record(kind="opencode", flags=["--auto", "--model", "a/b"], seen="2026-10-08T10:00:00")
    snap = sleeper.read_json(sleeper.SNAPSHOT)
    snap["w1:p1"]["argv"] = ["--auto", "--model", "a/b", "--session", UUID]
    sleeper.write_json(sleeper.SNAPSHOT, snap)
    restore(fake, argv=["--session", UUID])
    tick()
    (start,) = starts(fake)
    assert start[start.index("--") + 1:] == ("--auto", "--model", "a/b", "-s", UUID)
    assert events() == ["repaired"]


def test_restore_repair_waits_while_the_watcher_is_stopping(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    sleeper.STOP["requested"] = True
    tick()
    assert fake.signals == [] and snap_row()["repair"]["state"] == "pending"


def test_restore_repair_exit_uncertain_keeps_the_handle_and_never_retries(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    record()
    restore(fake)
    fake.exit_leaves_agent = False
    monkeypatch.setattr(sleeper, "EXIT_WAIT_SECONDS", 0.05)
    tick()
    assert events() == ["repair-failed"] and starts(fake) == []
    assert journal()["w1:p1"]["phase"] == "exit-requested" and journal()["w1:p1"]["restore_repair"] is True
    assert snap_row()["repair"]["state"] == "failed"
    capsys.readouterr()
    sleeper.cmd_list(types.SimpleNamespace())
    assert "exit-requested+repair" in capsys.readouterr().out               # `list` shows the half-done repair
    tick()
    tick()
    assert len(fake.signals) == 1 and starts(fake) == []                    # one attempt, whatever happened


def test_restore_repair_start_that_lands_elsewhere_keeps_the_entry(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    fake.start_uuid = "someone-else"
    tick()
    assert events() == ["wake-mismatch", "repair-failed"]
    j = journal()["w1:p1"]
    assert j["uuid"] == UUID and j["wake_got"] == "someone-else" and j["restore_repair"] is True
    assert snap_row()["repair"]["state"] == "failed"


def test_restore_repair_signal_failure_withdraws_the_record_and_retries(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    record()
    restore(fake)

    def boom(pid: int, sig: int) -> None:
        raise ProcessLookupError("gone")

    monkeypatch.setattr(sleeper.os, "kill", boom)
    tick()
    assert journal() == {} and starts(fake) == []
    assert snap_row()["repair"]["state"] == "pending" and "signal failed" in snap_row()["repair"]["reason"]
    monkeypatch.setattr(sleeper.os, "kill", fake.kill)
    tick()
    assert events() == ["repaired"]


def test_a_repair_that_stopped_after_the_exit_is_finished_by_a_wake(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """The journal record written before the exit is a complete handle: a failed (or crashed) start leaves an
    asleep row that carries the recorded flags, and any wake replays them."""
    record()
    restore(fake)
    real_wake = sleeper.wake_entry

    def boom(entry: dict[str, Any], via: str = "command", key: str | None = None) -> bool:
        raise sleeper.SleeperError("herdr went away")

    monkeypatch.setattr(sleeper, "wake_entry", boom)
    tick()
    assert events() == ["repair-failed"] and starts(fake) == []
    assert journal()["w1:p1"]["phase"] == "asleep" and journal()["w1:p1"]["argv"] == FLAGS
    assert real_wake(journal()["w1:p1"]) is True
    (start,) = starts(fake)
    assert start[start.index("--") + 1:] == (*FLAGS, "--resume", UUID) and journal() == {}


def test_a_repaired_pane_is_not_slept_in_the_same_tick(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    tick(repair_config(idle_hours=0.0))
    assert events() == ["repaired"] and "w1:p1" not in journal()


def test_restore_repair_ignores_the_panes_own_process_but_not_a_stray_one(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """The real liveness check sees the bare claude we are about to replace; it must not count as 'live elsewhere'
    (while a second process on the same session still does)."""
    record()
    restore(fake)
    monkeypatch.setattr(sleeper, "uuid_live_elsewhere", REAL_UUID_LIVE)
    stray: list[str] = []

    def ps(cmd: list[str], **kw: Any) -> Any:
        own = [] if fake.signals else [f"1 claude --resume {UUID}"]
        return types.SimpleNamespace(stdout="\n".join([*own, *stray]) + "\n", returncode=0, stderr="")

    monkeypatch.setattr(sleeper.subprocess, "run", ps)
    stray.append(f"2 claude --dangerously-skip-permissions --resume {UUID}")
    tick()
    assert fake.signals == [] and "live in pid 2" in snap_row()["repair"]["reason"]
    stray.clear()
    tick()
    assert len(fake.signals) == 1 and events() == ["repaired"]


def test_uuid_live_elsewhere_can_ignore_the_agent_about_to_be_replaced(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "list_agents", list)
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([f"77 claude --dangerously-skip-permissions --resume {UUID}"]))
    assert REAL_UUID_LIVE(UUID, except_pane="w1:p1") == "pid 77"
    assert REAL_UUID_LIVE(UUID, except_pane="w1:p1", ignore_pids={77}) is None
    assert REAL_UUID_LIVE(UUID, except_pane="w1:p1", ignore_pids={78}) == "pid 77"


def test_list_shows_a_pane_waiting_for_repair(fake: FakeHerdr, capsys: pytest.CaptureFixture[str]) -> None:
    record()
    restore(fake)
    fake.agents[0]["focused"] = True
    tick()
    capsys.readouterr()
    assert sleeper.cmd_list(types.SimpleNamespace()) == 0
    out = capsys.readouterr().out
    assert "repair-pending" in out and "--model haiku" in out and "focused" in out and "w1:p1" in out


def test_list_is_unchanged_when_nothing_waits_for_repair(fake: FakeHerdr, capsys: pytest.CaptureFixture[str]) -> None:
    assert sleeper.cmd_list(types.SimpleNamespace()) == 0
    assert capsys.readouterr().out == "nothing is sleeping\n"


# ====================================================== restore repair, every kind
# Herdr relaunches every restorable kind as binary + resume flag + id (src/agent_resume.rs `plan()`).
# KINDS is the one table for that: the resume form, what a replay strips and refuses, the composer rule.

# kind, binary, launch flags the operator typed, Herdr's resume tokens, ours on wake, launcher prefix
KIND_CASES = [
    ("claude", "claude", ["--dangerously-skip-permissions", "--model", "haiku"], ["--resume", UUID],
     ["--resume", UUID], []),
    ("codex", "codex", ["--dangerously-bypass-approvals-and-sandbox", "-m", "gpt-5"], ["resume", UUID],
     ["resume", UUID], []),
    ("copilot", "copilot", ["--yolo", "--model", "gpt-5"], [f"--resume={UUID}"], [f"--resume={UUID}"], []),
    ("cursor", "cursor-agent", ["--yolo", "--model", "sonnet"], ["--resume", UUID], ["--resume", UUID],
     ["--use-system-ca", "/x/versions/1/index.js"]),
    ("hermes", "hermes", ["--yolo", "--tui"], ["--resume", UUID], ["--resume", UUID], []),
    ("opencode", "opencode", ["--auto", "--model", "a/b"], ["--session", UUID], ["-s", UUID], []),
    ("omp", "omp", ["--auto-approve", "--model=opus"], [f"--resume={UUID}"], [f"--resume={UUID}"], []),
    ("qwen", "qwen", ["--yolo", "--model", "qwen3"], ["--resume", UUID], ["--resume", UUID], []),
    ("agy", "agy", ["--dangerously-skip-permissions", "--model", "x"], ["--conversation", UUID],
     ["--conversation", UUID], []),
]
KIND_IDS = [c[0] for c in KIND_CASES]
DRAFT_SCREEN = "⏺ pong\n────────\n❯ half typed\n────────\n  ◤ graft · 40 nodes\n"
RECAP_SCREEN = "⏺ a recap line\n"   # claude's composer is not visible: `unknown`


def record_kind(kind: str, argv: list[str], terminal: str = OLD_TERM, seen: str = "2026-10-08T10:00:00",
                pane: str = "w1:p1", uuid: str = UUID, **extra: Any) -> None:
    snap = sleeper.read_json(sleeper.SNAPSHOT)
    snap[pane] = {"kind": kind, "uuid": uuid, "name": "a", "cwd": "/tmp/proj", "title": "Title", "argv": argv,
                  "terminal_id": terminal, "seen_at": seen, **extra}
    sleeper.write_json(sleeper.SNAPSHOT, snap)


def setup_kind(fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str],
               prefix: list[str], restored: bool = True) -> None:
    """A pane of this kind that was recorded with `flags` and, with `restored`, came back from a Herdr restart
    relaunched the way Herdr does it (resume tokens only)."""
    fake.agents[0]["agent"] = kind
    fake.binary, fake.prefix = (None if binary == kind else binary), prefix
    fake.proc_name = "node" if prefix else None
    record_kind(kind, [*flags, *theirs])
    if restored:
        restore(fake, argv=theirs)


def test_every_kind_herdr_restores_has_a_table_entry() -> None:
    herdr_restores = {"claude", "codex", "copilot", "devin", "droid", "qodercli", "qwen", "grok", "kimi",
                      "mastracode", "pi", "omp", "hermes", "opencode", "kilo", "cursor", "agy", "letta"}
    assert set(sleeper.KINDS) == herdr_restores
    for never in ("gemini", "cline", "kiro", "amp", "maki", "muse"):
        assert never not in sleeper.KINDS


@pytest.mark.parametrize("kind, uuid, expected", [
    ("claude", UUID, ["--resume", UUID]),
    ("codex", UUID, ["resume", UUID]),
    ("copilot", UUID, [f"--resume={UUID}"]),
    ("devin", UUID, ["--resume", UUID]),
    ("droid", UUID, ["--resume", UUID]),
    ("qodercli", UUID, ["--resume", UUID]),
    ("qwen", UUID, ["--resume", UUID]),
    ("grok", UUID, ["--resume", UUID]),
    ("kimi", UUID, ["--session", UUID]),
    ("mastracode", UUID, ["--thread", UUID]),
    ("pi", "/s/x.jsonl", ["--session", "/s/x.jsonl"]),
    ("omp", UUID, [f"--resume={UUID}"]),
    ("hermes", UUID, ["--resume", UUID]),
    ("opencode", UUID, ["-s", UUID]),          # Herdr says --session; the short form is what sleeping always used
    ("kilo", UUID, ["--session", UUID]),
    ("cursor", UUID, ["--resume", UUID]),
    ("agy", UUID, ["--conversation", UUID]),
    ("letta", "conv-1", ["--conversation", "conv-1"]),
    ("letta", "default:agent-9", ["--conversation", "default", "--agent", "agent-9"]),
])
def test_resume_argv_is_the_form_herdr_uses(kind: str, uuid: str, expected: list[str]) -> None:
    assert sleeper.resume_argv(kind, uuid) == expected
    assert sleeper.resume_argv(kind, uuid, ["--x"]) == ["--x", *expected]      # flags first, the resume tokens last


def test_resume_argv_for_a_kind_herdr_never_relaunches_is_none() -> None:
    assert sleeper.resume_argv("gemini", UUID) is None and sleeper.resume_argv("letta", "default:") is None


def test_the_binary_is_not_always_the_kind() -> None:
    assert sleeper.kind_binary("cursor") == "cursor-agent" and sleeper.kind_binary("codex") == "codex"
    assert sleeper.kind_binary("unknown-kind") == "unknown-kind"
    assert sleeper.manual_command({"kind": "cursor", "uuid": UUID, "cwd": "/p", "argv": ["--yolo"],
                                   "restore_repair": True}) == f"cd /p && cursor-agent --yolo --resume {UUID}"


@pytest.mark.parametrize("kind, argv, expected", [
    ("claude", ["--dangerously-skip-permissions", "--resume", UUID], ["--dangerously-skip-permissions"]),
    ("codex", ["resume", UUID, "--dangerously-bypass-approvals-and-sandbox", "-m", "gpt-5"],
     ["--dangerously-bypass-approvals-and-sandbox", "-m", "gpt-5"]),
    ("codex", ["--search", "resume", "--last"], ["--search"]),
    ("codex", ["-c", "model=o3", "--oss"], ["-c", "model=o3", "--oss"]),
    ("copilot", [f"--resume={UUID}", "--yolo", "--model", "gpt-5"], ["--yolo", "--model", "gpt-5"]),
    ("copilot", ["--resume", UUID, "--allow-tool", "shell(git)"], ["--allow-tool", "shell(git)"]),
    ("copilot", ["--continue", "--autopilot"], ["--autopilot"]),
    ("cursor", ["--yolo", "--resume", UUID, "--model", "sonnet"], ["--yolo", "--model", "sonnet"]),
    ("cursor", ["--resume", "--trust"], ["--trust"]),                       # a bare --resume opens a picker
    ("cursor", ["--continue", "--plan", "--workspace", "/w"], ["--plan", "--workspace", "/w"]),
    ("hermes", ["--resume", UUID, "--yolo", "--tui"], ["--yolo", "--tui"]),
    ("hermes", ["-r", UUID, "-m", "x", "-c"], ["-m", "x"]),
    ("hermes", ["--continue", "my session", "--ignore-rules"], ["--ignore-rules"]),
    ("opencode", ["--session", UUID, "--auto", "-m", "a/b"], ["--auto", "-m", "a/b"]),
    ("opencode", ["-s", UUID, "-c", "--mini"], ["--mini"]),
    ("omp", [f"--resume={UUID}", "--model=opus", "--auto-approve"], ["--model=opus", "--auto-approve"]),
    ("omp", ["-r", UUID, "--thinking", "high"], ["--thinking", "high"]),
    ("qwen", ["--resume", UUID, "--yolo", "-m", "qwen3"], ["--yolo", "-m", "qwen3"]),
    ("qwen", ["-c", "--approval-mode", "plan"], ["--approval-mode", "plan"]),
    ("agy", ["--conversation", UUID, "--dangerously-skip-permissions"], ["--dangerously-skip-permissions"]),
    ("agy", ["-c", "--model", "x", "--sandbox"], ["--model", "x", "--sandbox"]),
    ("kimi", ["--session", UUID], []),                                        # a CLI not on this machine:
    ("pi", ["--session", "/s/x.jsonl"], []),                                  # the resume tokens and nothing else
    ("letta", ["--conversation", "default", "--agent", "a1"], []),
    ("claude", [], []), ("codex", [], []), ("cursor", [], []),
])
def test_replayable_argv_per_kind(kind: str, argv: list[str], expected: list[str]) -> None:
    assert sleeper.replayable_argv(argv, kind) == (expected, None)


@pytest.mark.parametrize("kind, argv, needle", [
    ("codex", ["exec", "fix it"], "positional"),
    ("codex", ["resume", UUID, "and then do this"], "positional"),
    ("codex", ["fix the tests"], "positional"),
    ("codex", ["fork", UUID], "positional"),
    ("codex", ["-i", "a.png"], "-i"), ("codex", ["--image", "a.png"], "--image"),
    ("codex", ["--worktree"], "--worktree"), ("codex", ["-m"], "missing its value"),
    ("codex", ["-Z"], "unrecognised"),
    ("codex", ["--brand-new-flag"], "unrecognised"),                        # v0.1.2: an unlisted long option is refused too
    ("hermes", ["--token=sk-secret"], "unrecognised"), ("opencode", ["--api-key=sk-secret"], "unrecognised"),
    ("agy", ["--token", "sk-secret"], "unrecognised"), ("copilot", ["--secret=sk-secret"], "unrecognised"),
    ("copilot", ["-p", "do it"], "-p"), ("copilot", ["--prompt=do it"], "--prompt"),
    ("copilot", ["-i", "x"], "-i"), ("copilot", ["-s"], "-s"), ("copilot", ["--session-id", UUID], "--session-id"),
    ("copilot", ["--connect"], "--connect"), ("copilot", ["--attachment", "f"], "--attachment"),
    ("copilot", ["--share"], "--share"), ("copilot", ["--output-format", "json"], "--output-format"),
    ("copilot", ["sessions"], "positional"),
    ("cursor", ["-p", "x"], "-p"), ("cursor", ["--print"], "--print"), ("cursor", ["--api-key", "sk-1"], "--api-key"),
    ("cursor", ["-H", "Authorization: x"], "-H"), ("cursor", ["--list-models"], "--list-models"),
    ("cursor", ["-w", "feat"], "-w"), ("cursor", ["--worktree-base", "main"], "--worktree-base"),
    ("cursor", ["fix the bug"], "positional"), ("cursor", ["--yolo", "ls"], "positional"),
    ("cursor", ["--model", "x", "fix"], "positional"),
    ("hermes", ["-z", "do it"], "-z"), ("hermes", ["--oneshot", "x"], "--oneshot"),
    ("hermes", ["--usage-file", "f"], "--usage-file"), ("hermes", ["-w"], "-w"), ("hermes", ["--worktree"], "--worktree"),
    ("hermes", ["chat"], "positional"), ("hermes", ["-X"], "unrecognised"),
    ("opencode", ["--prompt", "x"], "--prompt"), ("opencode", ["--fork"], "--fork"),
    ("opencode", ["run", "x"], "positional"),
    ("omp", ["-p", "x"], "-p"), ("omp", ["--print"], "--print"), ("omp", ["--no-session"], "--no-session"),
    ("omp", ["--api-key=k"], "--api-key"), ("omp", ["--goal=x"], "--goal"), ("omp", ["--mode", "json"], "--mode"),
    ("omp", ["--export=f"], "--export"), ("omp", ["--from-claude"], "--from-claude"),
    ("qwen", ["-p", "x"], "-p"), ("qwen", ["--prompt", "x"], "--prompt"), ("qwen", ["-i", "x"], "-i"),
    ("qwen", ["--session-id", "x"], "--session-id"), ("qwen", ["--fork-session"], "--fork-session"),
    ("qwen", ["--openai-api-key", "k"], "--openai-api-key"), ("qwen", ["--acp"], "--acp"),
    ("qwen", ["--worktree"], "--worktree"), ("qwen", ["-o", "json"], "-o"),
    ("agy", ["-p", "x"], "-p"), ("agy", ["--print", "x"], "--print"), ("agy", ["--prompt", "x"], "--prompt"),
    ("agy", ["-i", "x"], "-i"), ("agy", ["--new-project"], "--new-project"),
    ("agy", ["--output-format", "json"], "--output-format"),
    # a CLI that could not be read on this machine: anything but the resume tokens is refused
    ("kimi", ["--yolo"], "not available"), ("pi", ["--session", "p", "hello"], "positional"),
    ("devin", ["-p", "x"], "not available"), ("grok", ["--model", "x"], "not available"),
    ("kilo", ["--session", UUID, "--auto"], "not available"), ("letta", ["--agent", "a", "--yolo"], "not available"),
    ("mastracode", ["--thread", UUID, "go"], "positional"),
    # every mode refuses these
    ("codex", ["--", "x"], "`--`"), ("cursor", ["--yolo", "--", "x"], "`--`"), ("kimi", ["--", "x"], "`--`"),
    # a kind with no entry is never replayable
    ("gemini", [], "no restore recipe"), ("cline", ["--yolo"], "no restore recipe"),
])
def test_replayable_argv_refuses_per_kind(kind: str, argv: list[str], needle: str) -> None:
    out, why = sleeper.replayable_argv(argv, kind)
    assert out is None and why and needle in why


def test_every_verified_kinds_resume_form_is_stripped_whatever_the_spelling() -> None:
    """The kind's own resume tokens never reach the replay (they are re-added once, last)."""
    for kind, _bin, _flags, theirs, ours, _prefix in KIND_CASES:
        assert sleeper.replayable_argv(theirs, kind) == ([], None), kind
        assert sleeper.replayable_argv(ours, kind) == ([], None), kind


@pytest.mark.parametrize("kind, argv, uuid, selected", [
    ("claude", ["--resume", UUID], UUID, True), ("claude", ["--resume", "other"], UUID, False),
    ("codex", ["resume", UUID], UUID, True), ("codex", ["resume", "other"], UUID, False),
    ("codex", ["--search", "resume", UUID], UUID, True), ("codex", ["-m", UUID], UUID, False),
    ("copilot", [f"--resume={UUID}"], UUID, True), ("copilot", ["--resume", UUID], UUID, True),
    ("copilot", ["--resume=other"], UUID, False),
    ("cursor", ["--yolo", "--resume", UUID], UUID, True), ("cursor", ["--resume"], UUID, False),
    ("hermes", ["--resume", UUID], UUID, True), ("hermes", ["-r", UUID], UUID, True),
    ("hermes", ["--continue", UUID], UUID, False),
    ("opencode", ["-s", UUID], UUID, True), ("opencode", ["--session", UUID], UUID, True),
    ("opencode", ["--prompt", UUID], UUID, False),
    ("omp", [f"--resume={UUID}"], UUID, True), ("omp", ["-r", UUID], UUID, True),
    ("kimi", ["--session", UUID], UUID, True), ("kimi", ["--resume", UUID], UUID, False),
    ("mastracode", ["--thread", UUID], UUID, True), ("pi", ["--session", "/p.jsonl"], "/p.jsonl", True),
    ("agy", ["--conversation", UUID], UUID, True), ("agy", ["-c", UUID], UUID, False),
    ("letta", ["--conversation", "c1"], "c1", True),
    ("letta", ["--conversation", "default", "--agent", "a1"], "default:a1", True),
    ("letta", ["--conversation", "default", "--agent", "a2"], "default:a1", False),
    ("gemini", ["--resume", UUID], UUID, False),
])
def test_selects_is_the_kinds_own_resume_form(kind: str, argv: list[str], uuid: str, selected: bool) -> None:
    assert sleeper.selects(kind, argv, uuid) is selected


# --- process identification: the binary is not always the kind

def test_agent_process_finds_a_cursor_pane_behind_its_node_wrapper(fake: FakeHerdr) -> None:
    """Live evidence (2026-10-08): argv0 cursor-agent, process name node, and the wrapper's own leading
    arguments `--use-system-ca …/index.js` in front of what the operator typed."""
    fake.agents[0]["agent"] = "cursor"
    fake.binary, fake.proc_name = "cursor-agent", "node"
    fake.prefix, fake.argv = ["--use-system-ca", "/x/versions/1/index.js"], ["--yolo", "--resume", UUID]
    assert sleeper.agent_process("w1:p1", "cursor") == (True, ["--yolo", "--resume", UUID], 1)


def test_agent_process_finds_a_script_run_by_an_interpreter() -> None:
    proc = {"argv": ["/venv/bin/python3.13", "/home/u/.local/bin/hermes", "--resume", UUID], "name": "python3.13"}
    assert sleeper.agent_proc_args("hermes", proc) == ["--resume", UUID]
    other = {"argv": ["/venv/bin/python3.13", "/home/u/bin/other-tool", "hermes"], "name": "python3.13"}
    assert sleeper.agent_proc_args("hermes", other) is None
    assert sleeper.agent_proc_args("codex", {"argv": ["/x/codex", "resume", UUID], "name": "codex"}) == ["resume", UUID]
    assert sleeper.agent_proc_args("cursor", {"argv": ["cursor-agent", "--yolo"], "name": "node"}) == ["--yolo"]
    assert sleeper.agent_proc_args("cursor", {"argv": ["cursor", "--yolo"], "name": "cursor"}) == ["--yolo"]


# --- recording: every kind, not only the ones that can sleep

@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
def test_merge_snapshot_records_every_kind_and_keeps_the_richer_record(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str]) -> None:
    setup_kind(fake, kind, binary, flags, theirs, prefix)
    sleeper.merge_snapshot(fake.agents, repair=True)
    row = snap_row()
    assert row["kind"] == kind and row["argv"] == flags          # the richer record survives the restart
    assert row["repair"]["state"] == "pending" and row["terminal_id"] == OLD_TERM


def test_merge_snapshot_records_a_kind_it_cannot_repair_without_a_marker(fake: FakeHerdr) -> None:
    fake.agents[0]["agent"] = "kimi"
    fake.binary = None
    fake.argv = ["--session", UUID]
    sleeper.merge_snapshot(fake.agents, repair=True)
    assert snap_row()["kind"] == "kimi" and snap_row()["argv"] == [] and "repair" not in snap_row()


def test_merge_snapshot_ignores_kinds_herdr_never_relaunches(fake: FakeHerdr) -> None:
    fake.agents[0]["agent"] = "gemini"
    sleeper.merge_snapshot(fake.agents, repair=True)
    assert "w1:p1" not in sleeper.read_json(sleeper.SNAPSHOT)


def test_merge_snapshot_repair_set_limits_the_kinds(fake: FakeHerdr) -> None:
    setup_kind(fake, "codex", "codex", ["--oss"], ["resume", UUID], [])
    sleeper.merge_snapshot(fake.agents, repair={"claude"})
    assert snap_row()["argv"] == [] and "repair" not in snap_row()    # codex is not covered: recorded bare


def test_snapshot_rows_of_kinds_that_cannot_sleep_are_no_wake_handle(fake: FakeHerdr) -> None:
    """Recording widened to every kind; the crash-recovery handle of `wake` did not."""
    setup_kind(fake, "codex", "codex", ["--oss"], ["resume", UUID], [], restored=False)
    sleeper.merge_snapshot(fake.agents, repair=False)
    assert sleeper.find_journal_entry("w1:p1") is None and sleeper.find_journal_entry("a") is None
    sleeper.write_json(sleeper.SNAPSHOT, {})
    record()                                                                  # a claude row still is one
    assert sleeper.find_journal_entry("w1:p1") is not None


def test_sleeping_is_not_widened_to_the_new_kinds(fake: FakeHerdr) -> None:
    for kind in ("codex", "cursor", "hermes", "copilot"):
        fake.agents[0]["agent"] = kind
        ok, reason, _ = sleeper.assess(fake.agents[0], fake.agents, 0, {}, set(), idle_since=0)
        assert not ok and "not supported" in reason
        assert not sleeper.sleep_agent(fake.agents[0], 0, set(), dry_run=True)[0]
    assert set(sleeper.PROFILES) == {"claude", "opencode"}
    for kind in ("codex", "cursor"):
        with pytest.raises(sleeper.ConfigError):
            sleeper.CONFIG_FILE.write_text(f'agents = ["{kind}"]\n')
            sleeper.load_config()


# --- the repair itself, per kind

@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
def test_restore_repair_relaunches_every_kind_with_its_flags(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str]) -> None:
    setup_kind(fake, kind, binary, flags, theirs, prefix)
    fake.agents[0]["label"] = "mylabel"
    tick()
    assert fake.signals == [(1, signal.SIGTERM)]
    (start,) = starts(fake)
    assert start[2] == "a" and start[start.index("--kind") + 1] == kind
    assert start[start.index("--") + 1:] == (*flags, *ours)                  # name kept, flags back, resume last
    assert journal() == {} and fake.agents[0]["label"] == "mylabel"
    assert events() == ["repaired"]
    last = json.loads(sleeper.EVENTS.read_text().splitlines()[-1])
    assert last["kind"] == kind and last["flags"] == [t.split("=")[0] for t in flags if t.startswith("-")] and last["from_terminal"] == OLD_TERM
    assert snap_row()["repair"]["state"] == "repaired"


@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
def test_a_pane_not_restored_is_left_alone_in_every_kind(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str]) -> None:
    """Same terminal id: nobody restarted Herdr, a bare relaunch was somebody's choice."""
    setup_kind(fake, kind, binary, flags, theirs, prefix, restored=False)
    fake.argv = theirs
    tick()
    assert fake.signals == [] and starts(fake) == []


@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
def test_a_pane_that_already_has_its_flags_is_left_alone_in_every_kind(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str]) -> None:
    setup_kind(fake, kind, binary, flags, theirs, prefix)
    fake.argv = [*flags, *theirs]
    tick()
    assert fake.signals == [] and starts(fake) == []


def test_a_kind_without_a_transcript_path_is_not_blocked_by_the_claude_transcript_gate(
        fake: FakeHerdr, state: Path) -> None:
    (state / "projects" / "proj" / f"{UUID}.jsonl").unlink()
    setup_kind(fake, "codex", "codex", ["--oss"], ["resume", UUID], [])
    tick()
    assert len(fake.signals) == 1 and events() == ["repaired"]
    # claude has a transcript to check: without it the same repair waits
    fake.agents[0].update(agent="claude", agent_session={"value": UUID}, terminal_id=OLD_TERM)
    fake.signals.clear()
    record_kind("claude", ["--dangerously-skip-permissions", "--resume", UUID])
    restore(fake)
    tick()
    assert fake.signals == [] and "transcript" in snap_row()["repair"]["reason"]


def test_a_recorded_argv_that_is_not_replayable_is_never_repaired(fake: FakeHerdr) -> None:
    setup_kind(fake, "cursor", "cursor-agent", ["--api-key", "sk-1", "--yolo"], ["--resume", UUID],
               ["--use-system-ca", "/x/index.js"])
    tick()
    assert fake.signals == [] and starts(fake) == []


def test_a_pane_of_another_kind_than_the_record_is_not_repaired(fake: FakeHerdr) -> None:
    setup_kind(fake, "codex", "codex", ["--oss"], ["resume", UUID], [])
    snap = sleeper.read_json(sleeper.SNAPSHOT)
    sleeper.write_json(sleeper.SNAPSHOT, snap)
    fake.agents[0]["agent"] = "hermes"
    fake.argv = ["--resume", UUID]
    tick()
    assert fake.signals == [] and starts(fake) == []


def test_the_repair_gate_itself_refuses_a_live_agent_of_another_kind_than_the_row(fake: FakeHerdr) -> None:
    """repair_restored filters on the row's kind first; the gate repeats it for the last look before the exit."""
    setup_kind(fake, "codex", "codex", ["--oss"], ["resume", UUID], [])
    now = {**fake.agents[0], "agent": "hermes"}
    reason, pid = sleeper.repair_gate(now, now, [now], snap_row(), ["--oss"], {}, repair_config())
    assert pid is None and reason and "not the recorded 'codex'" in reason


@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
@pytest.mark.parametrize("gate", ["focused", "working", "other-pane", "cwd"])
def test_the_common_gates_hold_for_every_kind(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str], gate: str) -> None:
    setup_kind(fake, kind, binary, flags, theirs, prefix)
    a = fake.agents[0]
    if gate == "focused":
        a["focused"] = True
    elif gate == "working":
        a["agent_status"] = "working"
    elif gate == "cwd":
        a["cwd"] = "/elsewhere"
    else:
        fake.agents.append(agent(pane="w1:p2", kind=kind, name="b", terminal="term-9"))
    tick()
    assert fake.signals == [] and starts(fake) == []
    assert snap_row()["repair"]["state"] == "pending"


# --- the composer rule

@pytest.mark.parametrize("kind", [k for k in sleeper.KINDS if k != "claude"])
def test_a_kind_without_a_composer_detector_has_no_detector(kind: str) -> None:
    assert sleeper.KINDS[kind]["composer"] is None
    for screen in (None, "", DRAFT_SCREEN, "› a draft", RECAP_SCREEN):
        assert sleeper.composer_refusal(kind, screen) is None
        assert sleeper.composer_refusal(kind, screen, require_empty=False) is None


@pytest.mark.parametrize("screen, require, refused", [
    (EMPTY_SCREEN, True, False),
    (DRAFT_SCREEN, True, True), (DRAFT_SCREEN, False, True),          # a draft always refuses
    (RECAP_SCREEN, True, True), (RECAP_SCREEN, False, False),         # unknown: the setting decides
    (None, True, True), (None, False, False),
    ("nothing that looks like a prompt\n", True, True), ("nothing that looks like a prompt\n", False, False),
])
def test_claude_composer_rule(screen: str | None, require: bool, refused: bool) -> None:
    got = sleeper.composer_refusal("claude", screen, require)
    assert (got is not None) is refused
    if got:
        assert got.startswith("composer ")


@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
@pytest.mark.parametrize("screen", [None, "› half typed", "anything"])
def test_repair_does_not_wait_for_a_composer_check_that_has_no_detector(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str], screen: str | None) -> None:
    if kind == "claude":
        pytest.skip("claude has a detector, tested separately")
    setup_kind(fake, kind, binary, flags, theirs, prefix)
    fake.screen = screen
    tick()
    assert len(fake.signals) == 1 and events() == ["repaired"]


def test_claude_repair_waits_on_a_draft_and_on_an_unreadable_composer(fake: FakeHerdr) -> None:
    for screen in (DRAFT_SCREEN, RECAP_SCREEN, None):
        record()
        restore(fake)
        fake.screen = screen
        tick()
        assert fake.signals == [] and snap_row()["repair"]["state"] == "pending"
        assert snap_row()["repair"]["reason"].startswith("composer ")


def test_require_empty_composer_false_lets_an_unknown_composer_through_but_not_a_draft(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    fake.screen = DRAFT_SCREEN
    tick(repair_config(require_empty_composer=False))
    assert fake.signals == [] and "composer draft" in snap_row()["repair"]["reason"]
    fake.screen = RECAP_SCREEN
    tick(repair_config(require_empty_composer=False))
    assert len(fake.signals) == 1 and events() == ["repaired"]


# --- config: repair_kinds, require_empty_composer

@pytest.mark.parametrize("subset", [False, True])
def test_restore_table_repair_kinds_and_composer(state: Path, monkeypatch: pytest.MonkeyPatch, subset: bool) -> None:
    if subset:
        monkeypatch.setattr(sleeper, "tomllib", None)
    cfg = state / "config.toml"
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    cfg.write_text('[restore]\nrepair_kinds = ["claude", "cursor"]\nrequire_empty_composer = false\n')
    values, source = sleeper.load_config()
    assert values["repair_kinds"] == ["claude", "cursor"] and values["require_empty_composer"] is False
    assert source["repair_kinds"] == str(cfg)
    assert values["agents"] == ["claude", "opencode"]                          # sleeping is a separate list
    monkeypatch.setenv("HERDR_SLEEPER_REPAIR_KINDS", "codex hermes")
    monkeypatch.setenv("HERDR_SLEEPER_REQUIRE_EMPTY_COMPOSER", "1")
    values, source = sleeper.load_config()
    assert values["repair_kinds"] == ["codex", "hermes"] and values["require_empty_composer"] is True
    assert source["repair_kinds"] == "$HERDR_SLEEPER_REPAIR_KINDS"


@pytest.mark.parametrize("text", [
    '[restore]\nrepair_kinds = ["gemini"]\n', '[restore]\nrepair_kinds = ["claude", "nonsense"]\n',
    "[restore]\nrepair_kinds = 3\n", "[restore]\nrepair_kinds = [1]\n", "[restore]\nrequire_empty_composer = 1\n",
    '[restore]\nrequire_empty_composer = "yes"\n', 'repair_kinds = ["claude"]\n', "require_empty_composer = true\n",
])
def test_restore_table_rejects_bad_kinds_and_composer_values(state: Path, monkeypatch: pytest.MonkeyPatch,
                                                              text: str) -> None:
    cfg = state / "config.toml"
    cfg.write_text(text)
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    with pytest.raises(sleeper.ConfigError):
        sleeper.load_config()


def test_repair_kinds_helper() -> None:
    on = {"repair_flags": True}
    assert sleeper.repair_kinds(on) == set(sleeper.KINDS) - {"codex"} and sleeper.repair_enabled(on)   # codex is opt-in
    assert sleeper.repair_kinds({**on, "repair_kinds": list(sleeper.KINDS)}) == set(sleeper.KINDS)
    assert sleeper.repair_kinds({**on, "repair_kinds": ["codex", "gemini"]}) == {"codex"}
    assert sleeper.repair_kinds({**on, "repair_kinds": []}) == set() and not sleeper.repair_enabled({**on, "repair_kinds": []})
    assert sleeper.repair_kinds({"repair_flags": False}) == set() and sleeper.repair_kinds({}) == set()


# --- list and events carry the kind

def test_list_names_the_kind_of_a_pane_waiting_for_repair(fake: FakeHerdr, capsys: pytest.CaptureFixture[str]) -> None:
    setup_kind(fake, "cursor", "cursor-agent", ["--yolo"], ["--resume", UUID], ["--use-system-ca", "/x/index.js"])
    fake.agents[0]["focused"] = True
    tick()
    capsys.readouterr()
    assert sleeper.cmd_list(types.SimpleNamespace()) == 0
    out = capsys.readouterr().out
    assert "cursor" in out and "repair-pending" in out and "restored without --yolo" in out


def test_a_plain_wake_event_carries_the_kind(fake: FakeHerdr) -> None:
    sleeper.sleep_agent(agent(), 0, set(), dry_run=False)
    sleeper.wake_entry(journal()["w1:p1"])
    last = json.loads(sleeper.EVENTS.read_text().splitlines()[-1])
    assert last["event"] == "woke" and last["kind"] == "claude"


# ============================================================ the watcher reloads itself

class Reloaded(Exception):
    """Stands in for a successful os.execv, which never returns."""


def version_text(n: int) -> str:
    """A stand-in script that ends the way the real one does (a complete file), version n."""
    return f"x = {n}\nif __name__ == '__main__':\n    pass\n"


def deployed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, body: str | None = None, smoke: bool = False) -> Path:
    """A stand-in script on disk that SELF (and so code_stamp) points at. A stand-in cannot answer `version`, so the
    smoke run is stubbed out unless the test is about it (`smoke=True`)."""
    script = tmp_path / "deploy" / "herdr-sleeper"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text(version_text(1) if body is None else body)
    monkeypatch.setattr(sleeper, "SELF", script)
    if not smoke:
        monkeypatch.setattr(sleeper, "smoke_problem", lambda script=None: None)
    return script


def test_a_changed_stamp_is_judged_only_when_seen_on_two_consecutive_looks(
        state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch)
    started = sleeper.code_stamp()
    check = sleeper.ReloadCheck(started)
    assert check.look() == ("same", started)
    script.write_text(version_text(2))
    new = sleeper.code_stamp()
    assert check.look() == ("settling", new)                       # first sight: a deploy may still be writing
    assert check.look() == ("reload", new)                         # the same stamp a tick later, complete and runnable
    script.write_text(version_text(3) + "# once more\n")           # it changed again: settle again
    again = sleeper.code_stamp()
    assert check.look() == ("settling", again) and check.look() == ("reload", again)
    script.write_text(version_text(1))                              # back to the same bytes is still a new stamp
    assert check.look()[0] == "settling"
    script.unlink()
    assert check.look() == ("gone", str(script))


@pytest.mark.parametrize("body, why", [
    ("", "an empty file compiles"),
    ("def helper():\n    return 1\n", "a valid prefix, cut at a statement boundary, compiles"),
    ("x = 1\nif __name__ == '__main__':\n", "cut after the guard line: does not compile"),
    ("def broken(:\n", "a syntax error"),
    ("x = 1\x00\nif __name__ == '__main__':\n    pass\n", "NUL bytes"),
    ("if __name__ == '__main__':\n    pass\nx = 1\n", "code after the guard: the file does not end with it"),
])
def test_an_incomplete_deploy_is_never_reloaded_and_never_smoke_run(
        state: Path, monkeypatch: pytest.MonkeyPatch, body: str, why: str) -> None:
    script = deployed(state, monkeypatch)
    monkeypatch.setattr(sleeper, "smoke_problem", lambda script=None: pytest.fail("smoke-ran an incomplete file"))
    check = sleeper.ReloadCheck(sleeper.code_stamp())
    script.write_bytes(body.encode())
    assert check.look()[0] == "settling"
    verdict, detail = check.look()
    assert verdict == "incomplete", why
    assert check.look() == (verdict, detail)                       # remembered: judged once per stamp


@pytest.mark.parametrize("source, ok", [
    ("x = 1\nif __name__ == '__main__':\n    main()\n", True),
    ('if __name__ == "__main__":  # run\n    main()\n\n# trailing comment\n', True),
    ("def main():\n    pass\n\nif __name__ == '__main__':\n    try:\n        main()\n    finally:\n        pass\n", True),
    ("", False), ("x = 1\n", False), ("if __name__ == '__main__':\n    main()\nprint('after')\n", False),
    ("if __name__ == '__main__':\n    main()\n)\n", False),
])
def test_ends_with_main_guard(source: str, ok: bool) -> None:
    assert sleeper.ends_with_main_guard(source) is ok


def test_a_deploy_that_compiles_but_cannot_start_is_not_reloaded(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Complete, ends with the guard, compiles: and raises at import. The smoke run (`version`, really executed) catches it."""
    script = deployed(state, monkeypatch, smoke=True)
    check = sleeper.ReloadCheck(sleeper.code_stamp())
    script.write_text("raise RuntimeError('boom at import')\nif __name__ == '__main__':\n    pass\n")
    check.look()
    verdict, detail = check.look()
    assert verdict == "unrunnable" and "`version` exited 1" in detail and "boom at import" in detail
    script.write_text("import sys\nif __name__ == '__main__':\n    print('herdr-sleeper stand-in')\n")   # runs, exits 0
    check.look()
    assert check.look()[0] == "reload"


def test_the_smoke_run_is_judged_once_per_stamp_and_a_hang_is_a_failure(
        state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch, smoke=True)
    calls: list[int] = []
    with monkeypatch.context() as local:     # only this patch is undone: the state fixture's isolation stays (review L4)
        local.setattr(sleeper, "smoke_problem", lambda script=None: calls.append(1) or "`version` did not finish")
        check = sleeper.ReloadCheck(sleeper.code_stamp())
        script.write_text(version_text(2))
        for _ in range(5):
            verdict, detail = check.look()
        assert verdict == "unrunnable" and "did not finish" in detail and calls == [1]
    # the real smoke_problem, with a run that times out
    monkeypatch.setattr(sleeper.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(
        sleeper.subprocess.TimeoutExpired("x", 1)))
    assert "did not finish within" in sleeper.smoke_problem(script)
    monkeypatch.setattr(sleeper.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(OSError("no exec")))
    assert "could not run it" in sleeper.smoke_problem(script)


def test_the_smoke_run_executes_version_without_the_lock_fd_marker(
        state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch, body="import os, sys\nif __name__ == '__main__':\n"
                                               "    sys.exit(7 if os.environ.get('HERDR_SLEEPER_WATCH_LOCK_FD') else "
                                               "(0 if sys.argv[1:] == ['version'] else 3))\n", smoke=True)
    monkeypatch.setenv(sleeper.WATCH_LOCK_FD_ENV, "5")
    assert sleeper.smoke_problem(script) is None                   # argv is exactly `version`, and the marker is not passed on


def drive(monkeypatch: pytest.MonkeyPatch, tick: Any, ticks: int) -> list[int]:
    """watch_loop for `ticks` ticks of a scripted watch_tick; returns the tick numbers that ran."""
    ran: list[int] = []

    def fake_tick(clocks: dict[str, Any], config: dict[str, Any]) -> None:
        ran.append(len(ran) + 1)
        if len(ran) >= ticks:
            sleeper.STOP["requested"] = True
        tick(len(ran))

    monkeypatch.setattr(sleeper, "watch_tick", fake_tick)
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    monkeypatch.setattr(sleeper.signal, "signal", lambda *a: None)
    sleeper.watch_loop()
    return ran


def log() -> str:
    return sleeper.LOGFILE.read_text() if sleeper.LOGFILE.exists() else ""


def test_the_watcher_execs_the_new_script_one_tick_after_it_first_sees_it(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch)
    execs: list[tuple[str, list[str], dict[str, Any]]] = []
    ran: list[int] = []

    def execv(path: str, argv: list[str]) -> None:
        fd = int(os.environ[sleeper.WATCH_LOCK_FD_ENV])
        execs.append((path, argv, {"fd_inheritable": os.get_inheritable(fd),
                                   "lock_held": sleeper.watcher_lock_held()}))    # asked of the OS at the moment of the exec
        raise Reloaded

    def tick_(n: int) -> None:
        ran.append(n)
        if n == 1:
            script.write_text(version_text(2) + "# deployed during tick 1\n")

    monkeypatch.setattr(sleeper.os, "execv", execv)
    monkeypatch.setenv(sleeper.WATCH_LOCK_FD_ENV, "x")                       # restored by monkeypatch at teardown
    with pytest.raises(Reloaded):
        drive(monkeypatch, tick_, 9)
    assert ran == [1, 2]                                                      # seen after tick 1, settled, exec before tick 3
    assert execs == [(sys.executable, [sys.executable, str(script), "watch"], {"fd_inheritable": True, "lock_held": True})]
    assert "watcher code changed on disk" in log() and "re-executing" in log()


def test_a_change_seen_before_the_first_tick_still_costs_one_old_code_tick_not_more(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A Herdr restart after a deploy: the new file is there when the loaded code starts its loop. It is looked at on two
    consecutive ticks before it is trusted, so exactly one tick runs on the old code."""
    script = deployed(state, monkeypatch)
    ticks = {"n": 0}
    real_stamp = sleeper.code_stamp
    calls = {"n": 0}

    def stamp() -> str:
        calls["n"] += 1
        if calls["n"] == 2:                                                  # the first look inside the loop
            script.write_text(version_text(3) + "# deployed\n")
        return real_stamp()

    monkeypatch.setattr(sleeper.os, "execv", lambda *a: (_ for _ in ()).throw(Reloaded()))
    monkeypatch.setenv(sleeper.WATCH_LOCK_FD_ENV, "x")
    monkeypatch.setattr(sleeper, "code_stamp", stamp)
    monkeypatch.setattr(sleeper, "watch_tick", lambda *a: ticks.update(n=ticks["n"] + 1))
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    monkeypatch.setattr(sleeper.signal, "signal", lambda *a: None)
    with pytest.raises(Reloaded):
        sleeper.watch_loop()
    assert ticks["n"] == 1


def test_an_unchanged_script_is_never_re_executed(fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    deployed(state, monkeypatch)
    monkeypatch.setattr(sleeper.os, "execv", lambda *a: pytest.fail("re-executed an unchanged script"))
    assert drive(monkeypatch, lambda n: None, 4) == [1, 2, 3, 4]
    assert "re-executing" not in log()


def test_a_failed_exec_keeps_the_old_code_running_and_is_not_retried_every_tick(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch)
    attempts: list[int] = []

    def execv(*a: Any) -> None:
        attempts.append(1)
        raise OSError("exec format error")

    monkeypatch.setattr(sleeper.os, "execv", execv)
    ran = drive(monkeypatch, lambda n: script.write_text(version_text(2) + "# deployed\n") if n == 1 else None, 6)
    assert ran == [1, 2, 3, 4, 5, 6] and attempts == [1]                     # kept ticking, tried once
    assert "re-exec of the changed watcher failed" in log()
    assert sleeper.WATCH_LOCK_FD_ENV not in os.environ                        # and took its marker back


def test_a_half_written_deploy_waits_then_reloads_when_it_is_complete(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch)
    monkeypatch.setattr(sleeper.os, "execv", lambda *a: (_ for _ in ()).throw(Reloaded()))
    monkeypatch.setenv(sleeper.WATCH_LOCK_FD_ENV, "x")

    def tick(n: int) -> None:
        if n == 1:
            script.write_text("def broken(:\n")
        elif n == 3:
            script.write_text(version_text(2) + "# complete\n")

    with pytest.raises(Reloaded):
        drive(monkeypatch, tick, 12)
    assert log().count("not a complete Python file") == 1                     # logged once for the same stamp, not per tick
    assert "re-executing" in log()


def test_an_empty_or_cut_deploy_is_logged_once_and_the_loaded_code_keeps_running(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch)
    monkeypatch.setattr(sleeper.os, "execv", lambda *a: pytest.fail("re-executed an incomplete file"))
    ran = drive(monkeypatch, lambda n: script.write_text("") if n == 1 else None, 6)    # a cp that truncated and has not written yet
    assert ran == [1, 2, 3, 4, 5, 6] and log().count("not a complete Python file") == 1


def test_a_deploy_that_cannot_start_is_logged_once_and_the_loaded_code_keeps_running(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch, smoke=True)
    monkeypatch.setattr(sleeper.os, "execv", lambda *a: pytest.fail("re-executed a script that cannot start"))
    bad = "raise RuntimeError('boom at import')\nif __name__ == '__main__':\n    pass\n"
    ran = drive(monkeypatch, lambda n: script.write_text(bad) if n == 1 else None, 6)
    assert ran == [1, 2, 3, 4, 5, 6] and log().count("does not start") == 1


def test_a_deleted_script_is_logged_once_and_the_loaded_code_keeps_running(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = deployed(state, monkeypatch)
    monkeypatch.setattr(sleeper.os, "execv", lambda *a: pytest.fail("re-executed a script that is gone"))
    ran = drive(monkeypatch, lambda n: script.unlink() if n == 1 else None, 5)
    assert ran == [1, 2, 3, 4, 5]
    assert log().count("is gone or unreadable") == 1


def test_a_watcher_told_to_stop_between_the_loop_check_and_the_reload_check_does_not_re_execute(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The SIGTERM lands after `while not STOP` and before the reload decision acts: the `not STOP` guard on the exec is
    what stops the exec (the loop condition alone cannot, it was already evaluated)."""
    script = deployed(state, monkeypatch)
    monkeypatch.setattr(sleeper.os, "execv", lambda *a: pytest.fail("re-executed while stopping"))
    seen = {"records": 0}

    def record_then_stop() -> None:
        seen["records"] += 1
        if seen["records"] == 3:                                    # the third loop pass, which would have reloaded
            sleeper.STOP["requested"] = True

    monkeypatch.setattr(sleeper, "watcher_pid_record", lambda: record_then_stop())
    ran = drive(monkeypatch, lambda n: script.write_text(version_text(2) + "# deployed\n") if n == 1 else None, 9)
    assert ran == [1, 2, 3] and seen["records"] == 3 and "re-executing" not in log()   # the pass still ticks, it just does not exec


# --- the lock goes across the exec

def test_take_watcher_lock_waits_for_a_normal_start_and_gives_up_after_the_wait(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    fh = sleeper.take_watcher_lock()
    assert fh is not None and not os.get_inheritable(fh.fileno())
    fh.close()
    held = hold_flock(sleeper.WATCHER_LOCK)
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    monkeypatch.setattr(sleeper, "WATCHER_LOCK_WAIT_SECONDS", 2)
    assert sleeper.take_watcher_lock() is None
    held.close()


def test_take_watcher_lock_ignores_an_inherited_fd_that_is_not_the_lock(
        fake: FakeHerdr, state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    other = state / "unrelated"
    other.write_text("x")
    fd = os.open(other, os.O_RDONLY)
    try:
        monkeypatch.setenv(sleeper.WATCH_LOCK_FD_ENV, str(fd))
        fh = sleeper.take_watcher_lock()
        assert fh is not None and fh.fileno() != fd
        os.fstat(fd)                                                        # the unrelated fd was not closed
        assert sleeper.WATCH_LOCK_FD_ENV not in os.environ                   # popped, so no child ever sees it
        fh.close()
    finally:
        os.close(fd)
    monkeypatch.setenv(sleeper.WATCH_LOCK_FD_ENV, "not-a-number")
    sleeper.take_watcher_lock().close()  # type: ignore[union-attr]


def test_a_real_exec_hands_the_watcher_lock_over_without_a_gap(tmp_path: Path) -> None:
    """The decision (and why): the flock lives on the open file description, which an exec keeps when the
    fd is inheritable. The old image names the fd in the environment, the new one adopts it, and at no
    moment can another process take the lock (a heal from any plugin activity would otherwise spawn a
    second watcher in a release-then-reacquire window). Done with real processes."""
    import subprocess
    import sys as _sys
    lock = tmp_path / "watcher.lock"
    new_image = f"""
import fcntl, os, subprocess, sys, types
from pathlib import Path
S = types.ModuleType("m"); S.__file__ = {str(SCRIPT)!r}
exec(compile(open(S.__file__).read(), S.__file__, "exec"), S.__dict__)
S.WATCHER_LOCK = Path({str(lock)!r})
probe = "import fcntl,sys\\nfh=open(sys.argv[1],'a+')\\ntry:\\n fcntl.flock(fh,fcntl.LOCK_EX|fcntl.LOCK_NB)\\n print('free')\\nexcept OSError:\\n print('held')"
before = subprocess.run([sys.executable, "-c", probe, {str(lock)!r}], capture_output=True, text=True).stdout.strip()
fh = S.take_watcher_lock()
after = subprocess.run([sys.executable, "-c", probe, {str(lock)!r}], capture_output=True, text=True).stdout.strip()
print(before, after, fh is not None, S.WATCH_LOCK_FD_ENV in os.environ, os.get_inheritable(fh.fileno()))
"""
    old_image = f"""
import fcntl, os, sys
fh = open({str(lock)!r}, "a+")
fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
os.set_inheritable(fh.fileno(), True)
os.environ["HERDR_SLEEPER_WATCH_LOCK_FD"] = str(fh.fileno())
os.execv(sys.executable, [sys.executable, "-c", {new_image!r}])
"""
    out = subprocess.run([_sys.executable, "-c", old_image], capture_output=True, text=True, timeout=120,
                         check=False)
    assert out.returncode == 0, out.stderr
    # held before the new image took anything over, held after, adopted, marker popped, not inheritable again
    assert out.stdout.strip() == "held held True False False"


# --- stray watchers

def ps_text(*lines: str) -> str:
    return "\n".join(lines) + "\n"


def test_stray_watchers_are_found_structurally(monkeypatch: pytest.MonkeyPatch) -> None:
    me = str(sleeper.SELF)
    text = ps_text(
        f"101 501 /usr/bin/python3 {me} watch",                              # this very script: not a stray
        "102 501 /usr/bin/python3 /old/place/herdr-sleeper watch",          # a stray
        "103 502 /usr/bin/python3 /other/user/herdr-sleeper watch",         # another user's
        "104 501 cargo watch -x test",
        "105 501 /usr/bin/python3 /old/herdr-sleeper list",
        "106 501 vim /old/herdr-sleeper watch",
        "107 501 /old/direct/herdr-sleeper watch",                          # exec'd directly (shebang)
        "108 501 python3 -u /old/u/herdr-sleeper watch",
        "113 501 /opt/homebrew/Frameworks/Python.framework/Resources/Python.app/Contents/MacOS/Python "
        "/old/mac/herdr-sleeper watch",                                       # macOS reports the interpreter as `Python`
        "109 501 python3 ./herdr-sleeper watch",                            # relative: cannot be told, left alone
        "110 501 /usr/bin/python3 /old/herdr-sleeper watch extra",
        "111 501 /usr/bin/python3 /old/herdr-sleeper-tools watch",
        "112 501 grep herdr-sleeper watch",
        f"{os.getpid()} 501 /usr/bin/python3 /me/herdr-sleeper watch",      # never ourselves
        f"{os.getppid()} 501 /usr/bin/python3 /parent/herdr-sleeper watch",
        "  junk",
        "not-a-pid 501 /x/herdr-sleeper watch",
    )
    everyone = set(range(100, 120)) | {os.getpid(), os.getppid()}                # all of them serve this state dir
    assert sleeper.stray_watchers(text, uid=501, serving=everyone) == [
        (102, "/old/place/herdr-sleeper"), (107, "/old/direct/herdr-sleeper"), (108, "/old/u/herdr-sleeper"),
        (113, "/old/mac/herdr-sleeper")]
    assert sleeper.stray_watchers(text, uid=502, serving=everyone) == [(103, "/other/user/herdr-sleeper")]
    assert sleeper.stray_watchers(text, uid=501, serving={102, 113}) == [                   # only what serves this state dir
        (102, "/old/place/herdr-sleeper"), (113, "/old/mac/herdr-sleeper")]
    assert sleeper.stray_watchers(text, uid=501, serving=set()) == []


def test_a_script_reached_through_a_symlink_is_not_a_stray(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real = deployed(tmp_path, monkeypatch)
    link = tmp_path / "link-to-herdr-sleeper"
    link.symlink_to(real)
    text = ps_text(f"101 501 python3 {link} watch")
    monkeypatch.setattr(sleeper, "SELF", real.resolve())
    assert sleeper.stray_watchers(text, uid=501, serving={101}) == []


def test_a_ps_that_fails_finds_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([], fail=True))
    assert sleeper.stray_watchers() == []


def test_stop_stray_watchers_sigterms_each_and_waits(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gone: set[int] = set()
    sent: list[tuple[int, int]] = []

    def kill(pid: int, sig: int) -> None:
        if sig == 0:
            if pid in gone:
                raise ProcessLookupError(pid)
            return
        sent.append((pid, sig))
        gone.add(pid)

    monkeypatch.setattr(sleeper.os, "kill", kill)
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    monkeypatch.setattr(sleeper, "stray_watchers", lambda: [(102, "/old/a/herdr-sleeper"), (108, "/old/b/herdr-sleeper")])
    assert REAL_STOP_STRAYS() == [102, 108]
    assert sent == [(102, signal.SIGTERM), (108, signal.SIGTERM)]
    text = log()
    assert "stopping stray watcher pid=102 running /old/a/herdr-sleeper" in text and "pid=108" in text
    assert events() == ["stray-watcher-stopped", "stray-watcher-stopped"]


def test_stop_stray_watchers_never_escalates_and_never_raises(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(sleeper.os, "kill", lambda pid, sig: sent.append((pid, sig)))     # it never dies
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    monkeypatch.setattr(sleeper, "STRAY_WAIT_SECONDS", 0)
    monkeypatch.setattr(sleeper, "stray_watchers", lambda: [(102, "/old/herdr-sleeper")])
    assert REAL_STOP_STRAYS() == [102]
    assert (102, signal.SIGKILL) not in sent and "still running after" in log()
    monkeypatch.setattr(sleeper, "stray_watchers", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    assert REAL_STOP_STRAYS() == [] and "stray watcher scan failed" in log()
    monkeypatch.setattr(sleeper, "stray_watchers", lambda: [(103, "/old/herdr-sleeper")])
    monkeypatch.setattr(sleeper.os, "kill", lambda pid, sig: (_ for _ in ()).throw(PermissionError("no")))
    assert REAL_STOP_STRAYS() == [] and "could not stop stray watcher pid=103" in log()


def test_startup_sweeps_strays_but_a_plain_gap_fill_does_not(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    swept: list[int] = []
    monkeypatch.setattr(sleeper, "stop_stray_watchers", lambda: swept.append(1) or [])
    monkeypatch.setattr(sleeper.subprocess, "run", Ps([]))
    monkeypatch.setattr(sleeper.subprocess, "Popen", lambda *a, **k: types.SimpleNamespace(pid=88))
    monkeypatch.setattr(sleeper, "migrate_legacy", lambda: None)
    monkeypatch.setattr(sleeper, "migrate_legacy_config", lambda: None)
    assert sleeper.cmd_startup() == 0 and swept == [1]
    sleeper.ensure_watcher(replace_changed=False, heal=True)
    sleeper.ensure_watcher(replace_changed=True, heal=True)        # what heal_watcher asks for when nothing holds the lock
    sleeper.ensure_watcher()
    assert swept == [1]
    REAL_HEAL()                                                    # a hook's heal: no ps, no signal, no wait
    assert swept == [1]


# --- the version command

def test_version_prints_one_line_for_a_deploy_check(
        state: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    script = deployed(state, monkeypatch)
    (script.parent / "herdr-plugin.toml").write_text('id = "x"\nname = "N"\nversion = "9.8.7"\n')
    monkeypatch.setattr(sleeper.subprocess, "Popen", lambda *a, **k: pytest.fail("version must start nothing"))
    sleeper.STATE_DIR.mkdir(parents=True)
    stamp = sleeper.code_stamp()
    sleeper.WATCHER_PID.write_text(json.dumps({"pid": 4242, "stamp": stamp, "lock": True}))
    monkeypatch.setattr(sleeper, "watcher_alive", lambda rec: True)
    assert sleeper.main(["version"]) == 0
    out = capsys.readouterr().out
    assert out == f"herdr-sleeper 9.8.7 script {script} stamp {stamp} watcher pid 4242 stamp {stamp} (current)\n"

    sleeper.WATCHER_PID.write_text(json.dumps({"pid": 4242, "stamp": "1:2", "lock": True}))
    sleeper.cmd_version(types.SimpleNamespace())
    assert "watcher pid 4242 stamp 1:2 (STALE)" in capsys.readouterr().out

    monkeypatch.setattr(sleeper, "watcher_alive", lambda rec: False)
    sleeper.cmd_version(types.SimpleNamespace())
    assert capsys.readouterr().out.endswith("watcher none running\n")


def test_version_without_a_manifest_or_a_pid_file(
        state: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    script = deployed(state, monkeypatch)
    assert sleeper.manifest_version() == "unknown"
    sleeper.cmd_version(types.SimpleNamespace())
    out = capsys.readouterr().out
    assert out.startswith(f"herdr-sleeper unknown script {script} stamp ") and out.endswith("watcher none running\n")
    (script.parent / "herdr-plugin.toml").write_text("no version here\n")
    assert sleeper.manifest_version() == "unknown"


def test_the_real_manifest_version_is_read(capsys: pytest.CaptureFixture[str]) -> None:
    import re
    declared = re.search(r'(?m)^version = "([^"]+)"', (SCRIPT.parent / "herdr-plugin.toml").read_text())
    assert declared and sleeper.manifest_version() == declared.group(1)


# ====================================================== combined review, findings 1 to 6 (slice D-fix)

class Procs:
    """A scripted `subprocess.run` for `ps` and `lsof`. ps lists `ps_text`; `ps -p PID -o etime=,args=` answers from
    `per_pid`; `lsof -t -- PATH` lists the pids `holders` gives for exactly that path (nobody else's), exits 1 for an
    unheld file like the real one, and raises (or exits `lsof_rc`) when told to."""

    def __init__(self, ps_text: str = "", holders: dict[str, list[int]] | None = None, lsof_fails: bool = False,
                 lsof_rc: int | None = None, per_pid: dict[int, str] | None = None) -> None:
        self.ps_text, self.holders, self.lsof_fails, self.lsof_rc = ps_text, holders or {}, lsof_fails, lsof_rc
        self.per_pid = per_pid or {}
        self.calls: list[list[str]] = []

    def __call__(self, cmd: list[str], **kw: Any) -> Any:
        self.calls.append(list(cmd))
        if os.path.basename(cmd[0]) == "lsof":
            if self.lsof_fails:
                raise OSError("lsof: not found")
            pids = self.holders.get(cmd[-1], [])
            rc = self.lsof_rc if self.lsof_rc is not None else (0 if pids else 1)
            return types.SimpleNamespace(stdout="".join(f"{p}\n" for p in pids), returncode=rc, stderr="")
        if cmd[:2] == ["ps", "-p"]:
            return types.SimpleNamespace(stdout=self.per_pid.get(int(cmd[2]), ""), returncode=0, stderr="")
        return types.SimpleNamespace(stdout=self.ps_text, returncode=0, stderr="")

    def lsof_paths(self) -> list[str]:
        return [c[-1] for c in self.calls if os.path.basename(c[0]) == "lsof"]


# --- finding 1: the stray sweep is scoped to this state dir

OTHER_LOCK = "sessions/other/watcher.lock"
TWO_WATCHERS = ps_text("102 501 /usr/bin/python3 /old/place/herdr-sleeper watch",
                       "103 501 /usr/bin/python3 /lab/copy/herdr-sleeper watch")


def test_a_stray_is_a_watcher_of_this_state_dir_and_another_sessions_watcher_is_left_alone(
        state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mine = str(sleeper.WATCHER_LOCK)
    other = str(sleeper.STATE_DIR / OTHER_LOCK)
    procs = Procs(TWO_WATCHERS, holders={mine: [102], other: [103]})
    monkeypatch.setattr(sleeper.subprocess, "run", procs)
    assert sleeper.stray_watchers(uid=501) == [(102, "/old/place/herdr-sleeper")]
    assert procs.lsof_paths() == [mine] and procs.calls[-1][-2] == "--"          # asked about THIS lock only, as a path


def test_the_lab_session_does_not_sweep_the_production_watcher(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The reverse of the live case: the lab session's startup, running the lab copy, sees the production watcher."""
    production, lab = str(state / "production" / "watcher.lock"), state / "lab" / "watcher.lock"
    monkeypatch.setattr(sleeper, "WATCHER_LOCK", lab)
    monkeypatch.setattr(sleeper, "WATCHER_PID", state / "lab" / "watcher.pid")
    monkeypatch.setattr(sleeper, "SELF", Path("/lab/copy/herdr-sleeper"))
    monkeypatch.setattr(sleeper.subprocess, "run", Procs(TWO_WATCHERS, holders={production: [102], str(lab): [103]}))
    assert sleeper.stray_watchers(uid=501) == []                                  # 103 is us, 102 serves another state dir


def test_the_sweep_signals_only_the_watchers_of_this_state_dir(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    mine, other = str(sleeper.WATCHER_LOCK), str(sleeper.STATE_DIR / OTHER_LOCK)
    monkeypatch.setattr(sleeper.subprocess, "run", Procs(TWO_WATCHERS, holders={mine: [102], other: [103]}))
    monkeypatch.setattr(sleeper.os, "getuid", lambda: 501)
    gone: set[int] = set()
    sent: list[tuple[int, int]] = []

    def kill(pid: int, sig: int) -> None:
        if sig == 0:
            if pid in gone:
                raise ProcessLookupError(pid)
            return
        sent.append((pid, sig))
        gone.add(pid)

    monkeypatch.setattr(sleeper.os, "kill", kill)
    monkeypatch.setattr(sleeper.time, "sleep", lambda s: None)
    assert REAL_STOP_STRAYS() == [102] and sent == [(102, signal.SIGTERM)]
    assert events() == ["stray-watcher-stopped"]


@pytest.mark.parametrize("procs", [Procs(TWO_WATCHERS, lsof_fails=True), Procs(TWO_WATCHERS, lsof_rc=2)],
                         ids=["lsof-missing", "lsof-error"])
def test_when_lsof_cannot_say_nothing_is_a_stray(state: Path, monkeypatch: pytest.MonkeyPatch, procs: Procs) -> None:
    monkeypatch.setattr(sleeper.subprocess, "run", procs)
    monkeypatch.setattr(sleeper.os, "getuid", lambda: 501)
    monkeypatch.setattr(sleeper.os, "kill", lambda *a: pytest.fail("signalled a process on no evidence"))
    assert sleeper.stray_watchers(uid=501) == [] and "lsof could not say" in log()
    assert REAL_STOP_STRAYS() == []


def test_nobody_holding_the_lock_means_no_stray(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper.subprocess, "run", Procs(TWO_WATCHERS))                # lsof: exit 1, empty
    assert sleeper.stray_watchers(uid=501) == []


def test_a_watcher_older_than_the_lock_is_found_through_the_pid_file_only(
        state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """watcher.pid names a v0.1.0 watcher (no `lock` flag: lsof has nothing to see). A `lock: true` record that names
    a pid not holding the lock is a reused pid that may be another session's watcher: not ours to signal."""
    monkeypatch.setattr(sleeper.subprocess, "run", Procs(TWO_WATCHERS))
    sleeper.STATE_DIR.mkdir(parents=True)
    sleeper.WATCHER_PID.write_text(json.dumps({"pid": 102, "stamp": "old"}))
    assert sleeper.stray_watchers(uid=501) == [(102, "/old/place/herdr-sleeper")]
    sleeper.WATCHER_PID.write_text(json.dumps({"pid": 102, "stamp": "old", "lock": True}))
    assert sleeper.stray_watchers(uid=501) == []


def test_the_watcher_running_this_very_script_is_never_a_stray(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    text = ps_text(f"101 501 /usr/bin/python3 {sleeper.SELF} watch")
    monkeypatch.setattr(sleeper.subprocess, "run", Procs(text, holders={str(sleeper.WATCHER_LOCK): [101]}))
    assert sleeper.stray_watchers(uid=501) == []


# --- finding 2: a bare resume in a new pane is not a Herdr restore

def test_terminal_ids_carry_their_creation_time() -> None:
    assert sleeper.terminal_born(term_at(1_790_000_000.25, 12)) == pytest.approx(1_790_000_000.25, abs=1e-5)
    assert sleeper.terminal_born("term_65d1bb1650c552") == pytest.approx(1_791_223_381.95, abs=0.01)   # a real one
    for odd in ("term-1", "", None, "term_zz", "term_65d1bb165", "terminal_65d1bb1650c552", "term_0000000000000"):
        assert sleeper.terminal_born(odd) is None


@pytest.mark.parametrize("terminal, seen_at, ok", [
    (NEW_TERM, "2026-10-08T10:00:00", True),                                   # a restore: young terminal, old record
    (term_at(SERVER_START - 1), "2026-10-08T10:00:00", True),                 # ps counts whole seconds: a little slack
    (term_at(SERVER_START + 590), "2026-10-08T10:00:00", True),               # a slow restore of many panes
    (term_at(SERVER_START + 3600), "2026-10-08T10:00:00", False),             # born an hour after the start: a new pane
    (term_at(SERVER_START - 100), "2026-10-08T10:00:00", False),              # born before this server: not its restore
    (NEW_TERM, sleeper.stamp(SERVER_START + 10), False),                      # the record was made in this server run
    (NEW_TERM, "not a time", False), (NEW_TERM, None, False),
    ("term-2", "2026-10-08T10:00:00", False), (None, "2026-10-08T10:00:00", False),
])
def test_restart_evidence(state: Path, terminal: str | None, seen_at: object, ok: bool) -> None:
    assert sleeper.restart_evidence(terminal, seen_at) is ok


def test_no_evidence_when_the_servers_start_cannot_be_told(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "herdr_server_started", lambda: None)
    assert sleeper.restart_evidence(NEW_TERM, "2026-10-08T10:00:00") is False


def test_parse_etime() -> None:
    assert sleeper.parse_etime("03-07:17:04") == 3 * 86400 + 7 * 3600 + 17 * 60 + 4
    assert sleeper.parse_etime("07:17") == 437 and sleeper.parse_etime("1:02:03") == 3723 and sleeper.parse_etime("x") is None


def test_the_server_start_comes_from_the_process_that_listens_on_the_socket(
        state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/x/herdr.sock")
    monkeypatch.setitem(sleeper.SERVER_START_CACHE, "value", None)
    procs = Procs(holders={"/x/herdr.sock": [979]}, per_pid={979: "03-07:17:04 /Users/x/.local/bin/herdr server\n"})
    monkeypatch.setattr(sleeper.subprocess, "run", procs)
    got = REAL_SERVER_STARTED()
    assert got == pytest.approx(time.time() - 285424, abs=5) and procs.lsof_paths() == ["/x/herdr.sock"]
    monkeypatch.setattr(sleeper.subprocess, "run", lambda *a, **k: pytest.fail("asked again within the cache time"))
    assert REAL_SERVER_STARTED() == got


@pytest.mark.parametrize("procs, why", [
    (Procs(holders={"/x/herdr.sock": []}), "nobody listens"),
    (Procs(lsof_fails=True), "lsof is missing"),
    (Procs(holders={"/x/herdr.sock": [979]}, per_pid={979: "00:10 herdr --session default\n"}), "a client, not the server"),
    (Procs(holders={"/x/herdr.sock": [979, 980]},
           per_pid={979: "00:10 herdr server\n", 980: "00:20 herdr server\n"}), "two servers: ambiguous"),
    (Procs(holders={"/x/herdr.sock": [979]}, per_pid={979: "garbage herdr server\n"}), "unreadable elapsed time"),
])
def test_the_server_start_is_unknown_rather_than_guessed(
        state: Path, monkeypatch: pytest.MonkeyPatch, procs: Procs, why: str) -> None:
    monkeypatch.setenv("HERDR_SOCKET_PATH", "/x/herdr.sock")
    monkeypatch.setitem(sleeper.SERVER_START_CACHE, "value", None)
    monkeypatch.setattr(sleeper.subprocess, "run", procs)
    assert REAL_SERVER_STARTED() is None, why


def test_a_bare_resume_in_a_new_pane_is_not_mistaken_for_a_restore(fake: FakeHerdr) -> None:
    """The operator ran the session with --dangerously-skip-permissions in pane A, closed it, and resumes it bare in a new
    tab B. B's terminal id is new and the old record persists, but it sits under pane A's key: B is left alone, even when B's
    terminal id passes the age rule."""
    record()                                                       # pane w1:p1, flags, the previous server run
    fake.agents[0]["pane_id"] = "w1:p2"                            # the new tab's pane (pane ids of a live workspace are not reused)
    restore(fake)                                                  # bare argv, a terminal born right after the server start
    tick()
    tick()
    assert fake.signals == [] and starts(fake) == [] and events() == []
    assert "repair" not in snap_row("w1:p2") and snap_row("w1:p2")["argv"] == []
    assert snap_row("w1:p1")["argv"] == [*FLAGS, "--resume", UUID]   # the old record is untouched, as seeded


@pytest.mark.parametrize("terminal, seen, why", [
    (term_at(SERVER_START + 3600), "2026-10-08T10:00:00", "the pane id was reused by a workspace opened an hour after the start"),
    (term_at(SERVER_START - 100), "2026-10-08T10:00:00", "a terminal that predates this server"),
    (NEW_TERM, sleeper.stamp(SERVER_START + 1), "a record made while this server was already running"),
    ("term-2", "2026-10-08T10:00:00", "an id of another shape"),
])
def test_the_same_pane_id_is_not_enough_without_the_age_evidence(
        fake: FakeHerdr, terminal: str, seen: str, why: str) -> None:
    record(seen=seen)
    restore(fake, terminal)
    tick()
    assert fake.signals == [] and starts(fake) == [] and "repair" not in snap_row(), why
    assert snap_row()["argv"] == []                              # recorded as it is


def test_no_repair_when_the_servers_start_is_unknown(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "herdr_server_started", lambda: None)
    record()
    restore(fake)
    tick()
    assert fake.signals == [] and "repair" not in snap_row()


def test_a_real_restore_marks_the_pane_with_its_proof(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    sleeper.merge_snapshot(fake.agents)
    assert snap_row()["repair"]["evidence"] == sleeper.RESTORE_EVIDENCE
    tick()
    assert len(fake.signals) == 1 and events() == ["repaired"]


@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
def test_a_bare_resume_in_a_new_pane_is_left_alone_for_every_kind(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str]) -> None:
    setup_kind(fake, kind, binary, flags, theirs, prefix)
    fake.agents[0]["pane_id"] = "w1:p7"
    tick()
    tick()
    assert fake.signals == [] and starts(fake) == [] and "repair" not in snap_row("w1:p7")


# --- finding 4: what repair covers by default, and what a kind without a composer check needs

def test_codex_is_opt_in_for_restore_repair_and_every_other_kind_is_on(state: Path) -> None:
    assert sleeper.REPAIR_OPT_IN == ("codex",)
    assert set(sleeper.DEFAULT_REPAIR_KINDS) == set(sleeper.KINDS) - {"codex"}
    values, _src = sleeper.load_config()
    assert "codex" not in values["repair_kinds"] and len(values["repair_kinds"]) == len(sleeper.KINDS) - 1
    assert sleeper.repair_kinds(values) == set(sleeper.KINDS) - {"codex"}
    assert sleeper.repair_kinds({**values, "repair_kinds": [*values["repair_kinds"], "codex"]}) == set(sleeper.KINDS)


@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
def test_the_default_repair_covers_every_kind_but_codex(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str]) -> None:
    setup_kind(fake, kind, binary, flags, theirs, prefix)
    tick({**sleeper.DEFAULTS, **sleeper.RESTORE_DEFAULTS, "notify": False})        # repair_kinds left at its default
    assert (len(fake.signals) == 1) is (kind != "codex")
    assert (events() == ["repaired"]) is (kind != "codex")


@pytest.mark.parametrize("kind, binary, flags, theirs, ours, prefix", KIND_CASES, ids=KIND_IDS)
def test_the_gate_wants_the_restore_proof_before_it_lets_any_kind_go(
        fake: FakeHerdr, kind: str, binary: str, flags: list[str], theirs: list[str], ours: list[str],
        prefix: list[str]) -> None:
    """A kind with no composer check (everything but claude) goes on the proof alone, plus idle or done and unfocused.
    A marker without the proof (an older version's, or edited) waits, whatever the kind."""
    setup_kind(fake, kind, binary, flags, theirs, prefix)
    sleeper.merge_snapshot(fake.agents)
    row = snap_row()
    now = fake.agents[0]
    extras = sleeper.replayable_argv(row["argv"], kind)[0] or []
    assert sleeper.repair_gate(now, now, [now], row, extras, {}, repair_config())[0] is None
    del row["repair"]["evidence"]
    reason, pid = sleeper.repair_gate(now, now, [now], row, extras, {}, repair_config())
    assert pid is None and reason and "no proof" in reason


# --- finding 3 end to end: a real watcher process, a real deploy

def fake_herdr_binary(directory: Path) -> Path:
    path = directory / "fake-herdr"
    path.write_text("#!/bin/sh\necho '{\"id\":\"x\",\"result\":{\"agents\":[]}}'\n")
    path.chmod(0o755)
    return path


def test_a_real_watcher_reloads_itself_in_place_when_its_script_is_replaced(tmp_path: Path) -> None:
    """The reload path as production runs it: a real `watch` process of a copy of the script, a real file change, no
    helper standing in for the old image. Same pid, the watcher lock never free, a new stamp in watcher.pid, still
    ticking (panes.json is rewritten every tick), one watcher process."""
    import fcntl
    import shutil
    import subprocess
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    script = deploy / "herdr-sleeper"
    shutil.copy(SCRIPT, script)
    state_root = tmp_path / "state"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("HERDR_SLEEPER_", "HERDR_PLUGIN_", "HERDR_SESSION"))}
    env.update(HERDR_PLUGIN_STATE_DIR=str(state_root), HERDR_PLUGIN_CONFIG_DIR=str(tmp_path / "cfg"),
               HERDR_BIN_PATH=str(fake_herdr_binary(tmp_path)), HERDR_SLEEPER_POLL_SECONDS="1")
    lock, pid_file, panes = state_root / "watcher.lock", state_root / "watcher.pid", state_root / "panes.json"

    def lock_is_held() -> bool:
        with lock.open("a+") as fh:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return True
            fcntl.flock(fh, fcntl.LOCK_UN)
            return False

    def watcher_record() -> dict[str, Any]:
        try:
            return json.loads(pid_file.read_text())
        except (OSError, ValueError):
            return {}

    def wait_for(cond: Any, what: str, seconds: float = 40.0) -> None:
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if proc.poll() is not None:
                pytest.fail(f"the watcher exited ({proc.returncode}) while waiting for {what}")
            if cond():
                return
            time.sleep(0.05)
        pytest.fail(f"timed out waiting for {what}")

    proc = subprocess.Popen([sys.executable, str(script), "watch"], env=env, cwd=tmp_path,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
    try:
        wait_for(lambda: watcher_record().get("pid") == proc.pid and panes.exists(), "the first tick")
        first = watcher_record()
        assert first["lock"] is True and lock_is_held()
        script.write_text(script.read_text() + "\n# deployed: a new version of the file\n")      # a deploy
        samples = 0

        def reloaded() -> bool:
            nonlocal samples
            samples += 1
            assert lock_is_held(), "the watcher lock was free at some moment of the reload"
            return watcher_record().get("stamp") not in (None, first["stamp"])

        wait_for(reloaded, "the new stamp in watcher.pid")
        second = watcher_record()
        st = script.stat()
        assert second["pid"] == proc.pid and second["stamp"] == f"{st.st_mtime_ns}:{st.st_size}" and second["lock"] is True
        before = panes.stat().st_mtime_ns
        wait_for(lambda: panes.stat().st_mtime_ns > before and lock_is_held(), "a tick of the new image")
        text = (state_root / "sleeper.log").read_text()
        assert text.count("watcher start") == 2 and "re-executing to load it" in text
        listing = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True, check=True).stdout
        mine = [ln for ln in listing.splitlines() if f"{script} watch" in ln]
        assert len(mine) == 1 and mine[0].split()[0] == str(proc.pid)                       # one watcher, the same pid
        assert samples > 3                                                                    # the lock was sampled throughout
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()
    assert proc.returncode == 0 and not lock_is_held()


# --- finding 5: the stub keeps asking to mark its row while it waits for an answer

def test_mark_stub_row_reports_what_happened(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    entry = slept_entry(fake)
    entry.pop("stub_confirm", None)
    sleeper.write_json(sleeper.JOURNAL, {"w1:p1": entry})
    monkeypatch.setattr(sleeper, "STUB_MARK_ATTEMPT_SECONDS", 0.3)
    with sleeper.locked():
        assert sleeper.mark_stub_row("w1:p1", UUID) == ("busy", None)
    state_, row = sleeper.mark_stub_row("w1:p1", UUID)
    assert state_ == "marked" and row and row["stub_confirm"] is True and journal()["w1:p1"]["stub_confirm"] is True
    assert sleeper.mark_stub_row("w1:p1", UUID)[0] == "marked"                     # idempotent
    assert sleeper.mark_stub_row("w1:p1", "another-uuid") == ("gone", None)
    assert sleeper.mark_stub_row("w9:p9", UUID) == ("gone", None)
    sleeper.JOURNAL.write_text("[not an object")
    assert sleeper.mark_stub_row("w1:p1", UUID) == ("gone", None)                   # damaged: stop asking


def test_a_stub_whose_first_attempt_lost_the_lock_keeps_trying_while_it_waits_at_the_prompt(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """The old watcher holds the global lock for a whole tick, across every due pane: one 5 s attempt can lose. The row
    must be marked before the prompt is answered, or `wake` sends a bare Enter the new stub ignores."""
    entry = slept_entry(fake)
    entry.pop("stub_confirm", None)
    sleeper.write_json(sleeper.JOURNAL, {"w1:p1": entry})
    prepare_stub(fake, monkeypatch, entry)
    monkeypatch.setattr(sleeper, "STUB_MARK_ATTEMPT_SECONDS", 0.3)
    lock = sleeper.locked()
    lock.__enter__()                                               # the old watcher is mid-tick
    asked: list[int] = []

    def at_the_prompt(_prompt: str = "") -> str:
        asked.append(1)
        assert "stub_confirm" not in journal()["w1:p1"]            # the first attempt lost
        lock.__exit__(None, None, None)                            # the tick ends
        end = time.monotonic() + 15
        while "stub_confirm" not in journal()["w1:p1"] and time.monotonic() < end:
            time.sleep(0.05)
        raise EOFError

    monkeypatch.setattr("builtins.input", at_the_prompt)
    assert sleeper.cmd_stub("w1:p1", UUID) == 0 and asked == [1]
    assert journal()["w1:p1"]["stub_confirm"] is True


def test_the_marker_thread_stops_when_told_and_does_not_start_when_already_told(
        monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def busy(*a: Any, **k: Any) -> tuple[str, None]:
        calls.append(1)
        time.sleep(0.01)
        return "busy", None

    monkeypatch.setattr(sleeper, "mark_stub_row", busy)
    stop = threading.Event()
    stop.set()
    sleeper.keep_marking_stub_row("w1:p1", UUID, stop).join(5)
    assert calls == []
    stop.clear()
    thread = sleeper.keep_marking_stub_row("w1:p1", UUID, stop)
    time.sleep(0.2)
    stop.set()
    thread.join(5)
    assert not thread.is_alive() and len(calls) >= 2
    seen = len(calls)
    time.sleep(0.1)
    assert len(calls) == seen
    stop.clear()
    sleeper.keep_marking_stub_row("w1:p1", UUID, stop, seconds=0.05).join(5)         # gives up after its time


# ============================================ v0.1.2 review round 2 (restore repair guards, secrets, uuids)

def clocked(monkeypatch: pytest.MonkeyPatch, start: float = 1_000_000.0) -> dict[str, float]:
    clock = {"t": start}
    monkeypatch.setattr(sleeper.time, "time", lambda: clock["t"])
    return clock


def test_repair_waits_out_the_idle_dwell_and_restarts_it_when_the_state_moves(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "REPAIR_IDLE_DWELL_SECONDS", 60)
    clock = clocked(monkeypatch)
    record()
    restore(fake)
    tick()                                                       # the first idle tick never repairs
    assert fake.signals == [] and "idle" in snap_row()["repair"]["reason"]
    clock["t"] += 50
    fake.agents[0]["state_change_seq"] += 1                      # the agent did something: the dwell starts over
    tick()
    clock["t"] += 30
    tick()
    assert fake.signals == []
    clock["t"] += 31
    tick()
    assert len(fake.signals) == 1 and events() == ["repaired"]


def test_repair_leaves_a_pane_alone_when_the_agent_process_changed_after_the_restore(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    """The operator exits the restored agent and runs a bare `claude --resume` themselves: another pid."""
    monkeypatch.setattr(sleeper, "REPAIR_IDLE_DWELL_SECONDS", 60)
    clock = clocked(monkeypatch)
    record()
    restore(fake)
    tick()
    assert snap_row()["repair"]["pid"] == 1
    fake.agents[0]["pid_hint"] = 2
    clock["t"] += 120
    tick()
    tick()
    assert fake.signals == [] and starts(fake) == []
    assert snap_row()["repair"]["state"] == "relaunched" and events() == ["repair-settled"]


def test_repair_treats_a_reused_pid_with_another_start_time_as_a_relaunch(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "REPAIR_IDLE_DWELL_SECONDS", 60)
    clock = clocked(monkeypatch)
    record()
    restore(fake)
    tick()
    monkeypatch.setattr(sleeper, "process_started", lambda pid: PROC_START + 3600)
    clock["t"] += 120
    tick()
    assert fake.signals == [] and snap_row()["repair"]["state"] == "relaunched"


def test_repair_fails_closed_when_the_pid_was_never_readable(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    fake.pidless = True
    tick()
    tick()
    assert fake.signals == [] and starts(fake) == []
    assert "pid" in snap_row()["repair"]["reason"] and "pid" not in snap_row()["repair"]


def test_repair_fails_closed_when_the_pid_is_unreadable_at_the_time_of_the_repair(
        fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "REPAIR_IDLE_DWELL_SECONDS", 60)
    clock = clocked(monkeypatch)
    record()
    restore(fake)
    tick()
    fake.pidless = True
    clock["t"] += 120
    tick()
    assert fake.signals == [] and snap_row()["repair"]["state"] == "pending"
    fake.pidless = False
    tick()
    assert len(fake.signals) == 1


def test_repair_fails_closed_when_the_start_time_is_unreadable(fake: FakeHerdr, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sleeper, "process_started", lambda pid: None)
    record()
    restore(fake)
    tick()
    assert fake.signals == [] and "pid" not in snap_row()["repair"]


def test_repair_leaves_bypass_flags_out_unless_the_operator_opted_in(fake: FakeHerdr) -> None:
    record()                                                     # --dangerously-skip-permissions --model haiku
    restore(fake)
    tick(repair_config(repair_bypass=False))
    assert len(starts(fake)) == 1
    start = starts(fake)[0]
    assert start[start.index("--") + 1:] == ("--model", "haiku", "--resume", UUID)    # the rest comes back
    last = json.loads(sleeper.EVENTS.read_text().splitlines()[-1])
    assert last["flags"] == ["--model"]


def test_repair_re_adds_bypass_flags_with_the_opt_in(fake: FakeHerdr) -> None:
    record()
    restore(fake)
    tick(repair_config(repair_bypass=True))
    start = starts(fake)[0]
    assert start[start.index("--") + 1:] == (*FLAGS, "--resume", UUID)


def test_repair_with_only_bypass_flags_recorded_does_nothing_without_the_opt_in(fake: FakeHerdr) -> None:
    record(flags=["--dangerously-skip-permissions"])
    restore(fake)
    tick(repair_config(repair_bypass=False))
    tick(repair_config(repair_bypass=False))
    assert fake.signals == [] and starts(fake) == []
    assert "repair_bypass" in snap_row()["repair"]["reason"]


def test_bypass_class_is_declared_in_the_kinds_table_and_repair_bypass_defaults_off() -> None:
    assert sleeper.RESTORE_DEFAULTS["repair_bypass"] is False and sleeper.RESTORE_DEFAULTS["repair_flags"] is True
    assert "--dangerously-skip-permissions" in sleeper.KINDS["claude"]["bypass"]
    assert "--dangerously-bypass-approvals-and-sandbox" in sleeper.KINDS["codex"]["bypass"]
    assert sleeper.without_bypass("codex", ["-a", "never", "--oss", "-m", "x"]) == (["--oss", "-m", "x"], ["-a"])
    assert sleeper.without_bypass("qwen", ["--approval-mode=yolo", "--model", "m"]) == (["--model", "m"], ["--approval-mode"])
    for kind, spec in sleeper.KINDS.items():
        known = (sleeper.BOOLEAN_OPTIONS | sleeper.VALUE_OPTIONS if kind == "claude" else spec["values"] | spec["bools"])
        assert spec["bypass"] <= known, (kind, sorted(spec["bypass"] - known))   # a bypass flag the kind can replay


def test_repair_bypass_is_a_boolean_config_key(state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = state / "c.toml"
    monkeypatch.setattr(sleeper, "CONFIG_FILE", cfg)
    cfg.write_text("[restore]\nrepair_bypass = true\n")
    assert sleeper.load_config()[0]["repair_bypass"] is True
    cfg.write_text('[restore]\nrepair_bypass = "yes"\n')
    with pytest.raises(sleeper.ConfigError, match="repair_bypass must be a boolean"):
        sleeper.load_config()
    cfg.write_text("")
    monkeypatch.setenv("HERDR_SLEEPER_REPAIR_BYPASS", "1")
    assert sleeper.load_config()[0]["repair_bypass"] is True


def test_option_values_never_reach_the_log_the_events_or_the_notification(fake: FakeHerdr) -> None:
    secret = "sk-secret-value-123"
    record(flags=["--model", secret])
    restore(fake)
    tick(repair_config(notify=True))
    assert len(starts(fake)) == 1                                          # it was repaired, with the value
    shown = [c for c in fake.calls if c[:2] == ("notification", "show")]
    assert shown and secret not in " ".join(shown[0])
    for path in (sleeper.LOGFILE, sleeper.EVENTS):
        assert path.exists() and secret not in path.read_text()


def test_a_refusal_reason_names_the_option_never_its_value() -> None:
    for kind in ("hermes", "opencode", "agy", "copilot"):
        argv, why = sleeper.replayable_argv(["--api-key=sk-secret-value"], kind)
        assert argv is None and "sk-secret-value" not in (why or "") and "--api-key" in (why or "")
    assert "sk-secret" not in (sleeper.replayable_argv(["--token", "sk-secret"], "claude")[1] or "")
    assert "sk-secret" not in (sleeper.replayable_argv(["sk-secret prompt"], "claude")[1] or "")


class TtyErr:
    """A stderr that says it is a terminal (what a person running `herdr-sleeper wake` has)."""
    def __init__(self) -> None:
        self.text = ""

    def isatty(self) -> bool:
        return True

    def write(self, data: str) -> int:
        self.text += data
        return len(data)

    def flush(self) -> None:
        pass


def test_the_manual_resume_line_keeps_values_for_the_operator_but_the_log_masks_them(
        state: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    entry = {"pane_id": "w1:p1", "uuid": UUID, "kind": "claude", "cwd": "/tmp/p",
             "argv": ["--model", "secretval", "--resume", UUID]}
    line = sleeper.manual_command(entry)
    assert "secretval" in line
    tty = TtyErr()
    monkeypatch.setattr(sleeper.sys, "stderr", tty)
    sleeper.say(f"cannot wake: Manual: {line}")
    assert "secretval" in tty.text                                         # a terminal gets it whole, to paste
    logged = sleeper.LOGFILE.read_text()
    assert "secretval" not in logged and "--model" in logged and UUID in logged


def test_state_dir_and_logs_are_private(state: Path) -> None:
    sleeper.STATE_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(sleeper.STATE_DIR, 0o755)
    sleeper.LOGFILE.write_text("old\n")
    os.chmod(sleeper.LOGFILE, 0o644)
    sleeper.say("hello")
    sleeper.event("e")
    assert (os.stat(sleeper.STATE_DIR).st_mode & 0o777) == 0o700
    for path in (sleeper.LOGFILE, sleeper.EVENTS):
        assert (os.stat(path).st_mode & 0o777) == 0o600, path
    fresh = sleeper.STATE_DIR / "sessions" / "x"
    sleeper.set_state_dir(fresh)
    sleeper.say("again")
    assert (os.stat(fresh).st_mode & 0o777) == 0o700 and (os.stat(sleeper.LOGFILE).st_mode & 0o777) == 0o600


@pytest.mark.parametrize("value", [UUID, "u", "sess-1", "ses_01HXYZabc", "default:agent-1", "/s/x.jsonl", "20251010_123456_ab"])
def test_real_session_id_shapes_pass(value: str) -> None:
    assert sleeper.valid_session_id(value)
    assert sleeper.valid_entry({"pane_id": "w1:p1", "uuid": value, "kind": "claude"})


@pytest.mark.parametrize("value", ["", "a b", "x'y", 'x"y', "x;rm", "x$(id)", "x`id`", "x\\'y", "x\ny", "-rf", "--resume",
                                   "a|b", "a&b", "a>b", "x\x1b[0m", 5, None, "a" * 400])
def test_a_session_id_that_is_not_a_plain_token_is_refused_everywhere(value: Any, fake: FakeHerdr) -> None:
    assert not sleeper.valid_session_id(value)
    assert not sleeper.valid_entry({"pane_id": "w1:p1", "uuid": value, "kind": "claude"})
    if isinstance(value, str) and value:
        assert sleeper.resume_argv("claude", value) is None
        with pytest.raises(sleeper.SleeperError, match="plain token"):
            sleeper.stub_command("w1:p1", value, "now")
        ok, reason, _ = sleeper.assess(agent(uuid=value), [], 1.0, {})
        assert not ok and "plain-token" in reason


def test_a_hostile_session_id_is_never_slept(fake: FakeHerdr) -> None:
    fake.agents[0]["agent_session"] = {"value": "x';touch /tmp/pwn;'"}
    assert sleeper.sleep_agent(fake.agents[0], 12, set(), dry_run=False)[0] is False
    assert fake.signals == [] and not any(c[:2] == ("pane", "run") for c in fake.calls)


# ============================================ v0.1.2 review round 3 (bypass lists, draft text, filtered argv, stderr)

ADDED_BYPASS = [
    ("claude", "--add-dir", True), ("claude", "--permission-prompt-tool", True),
    ("codex", "-c", True), ("codex", "--config", True), ("codex", "-p", True), ("codex", "--profile", True),
    ("codex", "--add-dir", True), ("codex", "--search", False),
    ("copilot", "--allow-tool", True), ("copilot", "--allow-url", True), ("copilot", "--autopilot", False),
    ("copilot", "--add-dir", True),
    ("qwen", "--allowed-tools", True), ("qwen", "--include-directories", True), ("qwen", "--add-dir", True),
    ("omp", "--plan-yolo-into", True), ("omp", "--allow-home", False), ("omp", "--add-dir", True),
    ("cursor", "--sandbox", True), ("cursor", "--auto-review", False), ("cursor", "--add-dir", True),
    ("agy", "--mode", True), ("agy", "--add-dir", True),
]
SAFE_OPTION = {"claude": "--model", "codex": "-m", "copilot": "--model", "qwen": "-m", "omp": "--model",
               "cursor": "--model", "agy": "--model"}


def spelled(flag: str, takes_value: bool, how: str) -> list[str]:
    value = "sandbox_mode=danger-full-access" if flag in ("-c", "--config") else "v1"
    if not takes_value:
        return [flag]
    return [f"{flag}={value}"] if how == "eq" else [flag, value]


@pytest.mark.parametrize("how", ["eq", "separate"])
@pytest.mark.parametrize("kind, flag, takes_value", ADDED_BYPASS)
def test_each_added_bypass_flag_is_dropped_without_repair_bypass_and_kept_with_it(
        kind: str, flag: str, takes_value: bool, how: str) -> None:
    assert flag in sleeper.KINDS[kind]["bypass"]
    safe = [SAFE_OPTION[kind], "m1"]
    argv = [*safe, *spelled(flag, takes_value, how)]
    extras, why = sleeper.replayable_argv(argv, kind)
    assert extras == argv, why                                    # repair_bypass on: the whole record comes back
    kept, dropped = sleeper.without_bypass(kind, extras)          # repair_bypass off: the flag and its value go
    assert kept == safe and dropped == [flag]


def test_codex_config_and_profile_are_bypass_class_in_both_spellings() -> None:
    for argv in (["-c", "approval_policy=never"], ["--config=sandbox_mode=danger-full-access"], ["-p", "work"],
                 ["--profile=work"], ["-c=sandbox_mode=read-only"]):
        extras, why = sleeper.replayable_argv(["--oss", *argv], "codex")
        assert extras == ["--oss", *argv], why
        assert sleeper.without_bypass("codex", extras)[0] == ["--oss"]


def test_cursor_sandbox_goes_with_its_value_whatever_the_value() -> None:
    for value in ("disabled", "enabled"):
        assert sleeper.without_bypass("cursor", ["--sandbox", value, "--model", "m"]) == (["--model", "m"], ["--sandbox"])
    assert sleeper.without_bypass("cursor", ["--sandbox=disabled"]) == ([], ["--sandbox"])


def test_claude_repair_leaves_a_widened_path_out_without_the_opt_in(fake: FakeHerdr) -> None:
    record(flags=["--model", "haiku", "--add-dir", "/outside"])
    restore(fake)
    tick(repair_config(repair_bypass=False))
    start = starts(fake)[0]
    assert start[start.index("--") + 1:] == ("--model", "haiku", "--resume", UUID)
    assert json.loads(sleeper.EVENTS.read_text().splitlines()[-1])["flags"] == ["--model"]


def test_claude_repair_puts_a_widened_path_back_with_the_opt_in(fake: FakeHerdr) -> None:
    record(flags=["--model", "haiku", "--add-dir=/outside"])
    restore(fake)
    tick(repair_config(repair_bypass=True))
    start = starts(fake)[0]
    assert start[start.index("--") + 1:] == ("--model", "haiku", "--add-dir=/outside", "--resume", UUID)


def test_a_typed_draft_never_reaches_the_log_the_panes_file_or_the_events(fake: FakeHerdr) -> None:
    secret = "my private plan"
    record()
    restore(fake)
    fake.screen = f"⏺ pong\n────────\n❯ {secret}\n────────\n  ◤ graft\n"
    tick()
    assert fake.signals == []
    assert snap_row()["repair"]["reason"] == f"composer draft: draft present ({len(secret)} chars)"
    for path in (sleeper.LOGFILE, sleeper.SNAPSHOT, sleeper.EVENTS):
        assert secret not in (path.read_text() if path.exists() else ""), path
    below = sleeper.composer_state("⏺ pong\n────────\n❯\nsecond line secret\n")
    assert below == ("draft", "draft present (18 chars)")


def test_panes_json_keeps_only_the_replayable_flags_of_the_live_argv(fake: FakeHerdr) -> None:
    fake.argv = ["--model", "sk-flag-value", "--resume", UUID]
    sleeper.merge_snapshot(fake.agents, repair=True)
    assert snap_row()["argv"] == ["--model", "sk-flag-value"]            # no resume tokens, only what a repair may replay
    fake.argv = ["--print", "a private prompt text", "--resume", UUID]
    sleeper.merge_snapshot(fake.agents, repair=True)
    assert snap_row()["argv"] is None
    assert "a private prompt text" not in sleeper.SNAPSHOT.read_text()
    tick()
    assert fake.signals == [] and starts(fake) == []                     # a null argv is nothing to replay


def test_stderr_gets_the_masked_manual_line_unless_it_is_a_terminal(
        state: Path, capsys: pytest.CaptureFixture[str]) -> None:
    entry = {"pane_id": "w1:p1", "uuid": UUID, "kind": "claude", "cwd": "/tmp/p",
             "argv": ["--model", "secretval", "--resume", UUID]}
    sleeper.say(f"cannot wake: Manual: {sleeper.manual_command(entry)}")   # a hook, an action or the watcher: not a tty
    err = capsys.readouterr().err
    assert "secretval" not in err and "--model" in err and UUID in err
