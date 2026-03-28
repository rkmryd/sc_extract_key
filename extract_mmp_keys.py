#!/usr/bin/env python3
"""
extract_mmp_keys.py

Extracts MMP mouflon pkey + pdkey from Stripchat's live HLS player using
headless Chrome and raw CDP (Chrome DevTools Protocol) over WebSocket.

Phase 1: CDP transport layer (Chrome launch, WebSocket JSON-RPC, events)
Phase 2: Page lifecycle & network interception (navigate, block, m3u8 pkey)

Dependencies: websockets (16.0), Python 3.10+
Chrome: ~/.cache/puppeteer/chrome/linux-*/chrome-linux64/chrome or /snap/bin/chromium
"""

import asyncio
import json
import os
import re
import shutil
import signal
import subprocess
import sys
from pathlib import Path

import websockets


# ---------------------------------------------------------------------------
# Chrome launcher
# ---------------------------------------------------------------------------

# Candidate Chrome/Chromium binary paths, checked in order.
_CHROME_CANDIDATES = [
    # Puppeteer-managed installs (newest first)
    *sorted(
        Path.home().glob(".cache/puppeteer/chrome/linux-*/chrome-linux64/chrome"),
        reverse=True,
    ),
    # System Chromium
    Path("/snap/bin/chromium"),
    Path("/usr/bin/chromium-browser"),
    Path("/usr/bin/chromium"),
    Path("/usr/bin/google-chrome"),
]

_CHROME_ARGS = [
    "--headless",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--autoplay-policy=no-user-gesture-required",
    "--mute-audio",
    "--remote-debugging-port=0",  # OS picks a free port
]


def _find_chrome() -> str:
    """Return the path to the first available Chrome/Chromium binary."""
    for candidate in _CHROME_CANDIDATES:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    # Fallback: check PATH
    found = shutil.which("chromium") or shutil.which("google-chrome")
    if found:
        return found
    raise FileNotFoundError(
        "No Chrome/Chromium binary found. Install Chromium or set CHROME_BIN."
    )


async def launch_chrome() -> tuple[subprocess.Popen, str]:
    """Launch headless Chrome and return (process, devtools_ws_url).

    Parses the DevTools WebSocket URL from Chrome's stderr line:
        ``DevTools listening on ws://127.0.0.1:<port>/devtools/browser/<id>``
    """
    chrome_bin = os.environ.get("CHROME_BIN") or _find_chrome()
    proc = subprocess.Popen(
        [chrome_bin, *_CHROME_ARGS],
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
    )

    # Read stderr lines until we see the WS URL (or Chrome exits).
    ws_url = None
    while True:
        line = proc.stderr.readline()
        if not line:
            break
        text = line.decode("utf-8", errors="replace").strip()
        m = re.search(r"ws://[^\s]+", text)
        if m:
            ws_url = m.group(0)
            break

    if ws_url is None:
        proc.kill()
        raise RuntimeError("Chrome did not emit a DevTools WebSocket URL")

    return proc, ws_url


# ---------------------------------------------------------------------------
# CDP session
# ---------------------------------------------------------------------------

class CDPSession:
    """Async Chrome DevTools Protocol session over a single WebSocket.

    Supports both the browser-level connection and flattened child sessions
    (via ``session_id``).  Event callbacks are registered with ``on()``.
    """

    def __init__(self, ws, session_id: str | None = None):
        self._ws = ws
        self._session_id = session_id
        self._next_id = 1
        self._pending: dict[int, asyncio.Future] = {}
        self._listeners: dict[str, list] = {}
        self._recv_task: asyncio.Task | None = None
        self._child_sessions: dict[str, "CDPSession"] = {}

    # -- public API ---------------------------------------------------------

    async def send(self, method: str, params: dict | None = None) -> dict:
        """Send a CDP command and await its response."""
        msg_id = self._next_id
        self._next_id += 1

        msg: dict = {"id": msg_id, "method": method, "params": params or {}}
        if self._session_id:
            msg["sessionId"] = self._session_id

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending[msg_id] = future

        await self._ws.send(json.dumps(msg))
        result = await future
        if "error" in result:
            err = result["error"]
            raise RuntimeError(f"CDP error {err.get('code')}: {err.get('message')}")
        return result.get("result", {})

    def on(self, event: str, callback):
        """Register an async or sync callback for a CDP event."""
        self._listeners.setdefault(event, []).append(callback)

    def start_recv_loop(self):
        """Start the background receive loop (call once on the root session)."""
        if self._recv_task is None:
            self._recv_task = asyncio.ensure_future(self._recv_loop())

    # -- child sessions via Target.attachToTarget(flatten=true) -------------

    def child_session(self, session_id: str) -> "CDPSession":
        """Return (or create) a CDPSession for a flattened child session."""
        if session_id not in self._child_sessions:
            child = CDPSession(self._ws, session_id=session_id)
            # Children share the same id-counter space to avoid collisions.
            child._next_id = self._next_id
            child._pending = self._pending
            # Bind so counter stays in sync.
            self._child_sessions[session_id] = child
        return self._child_sessions[session_id]

    # -- internals ----------------------------------------------------------

    async def _recv_loop(self):
        """Read WebSocket messages and dispatch responses / events."""
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                self._dispatch(msg)
        except websockets.exceptions.ConnectionClosed:
            pass

    def _dispatch(self, msg: dict):
        """Route a parsed CDP message to the correct handler."""
        # Flattened child-session message?
        sid = msg.get("sessionId")
        if sid and sid in self._child_sessions:
            self._child_sessions[sid]._dispatch(msg)
            return

        # Response to a pending send()?
        if "id" in msg:
            msg_id = msg["id"]
            future = self._pending.pop(msg_id, None)
            if future and not future.done():
                future.set_result(msg)
            # Keep id counter in sync across children.
            if msg_id >= self._next_id:
                self._next_id = msg_id + 1
            return

        # CDP event
        method = msg.get("method")
        if method and method in self._listeners:
            for cb in self._listeners[method]:
                try:
                    ret = cb(msg.get("params", {}))
                    if asyncio.iscoroutine(ret):
                        asyncio.ensure_future(ret)
                except Exception as exc:
                    print(f"[CDP] event handler error ({method}): {exc}",
                          file=sys.stderr)


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

async def connect_cdp(ws_url: str) -> CDPSession:
    """Open a WebSocket to Chrome and return a ready CDPSession."""
    ws = await websockets.connect(ws_url, max_size=50 * 1024 * 1024)
    session = CDPSession(ws)
    session.start_recv_loop()
    return session


# ---------------------------------------------------------------------------
# Phase 2 — Page lifecycle and network interception
# ---------------------------------------------------------------------------

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

# Patterns to block (images, fonts, stylesheets) to speed up page load.
_BLOCKED_RESOURCE_PATTERNS = [
    {"urlPattern": "*.png", "resourceType": "Image"},
    {"urlPattern": "*.jpg", "resourceType": "Image"},
    {"urlPattern": "*.jpeg", "resourceType": "Image"},
    {"urlPattern": "*.gif", "resourceType": "Image"},
    {"urlPattern": "*.svg", "resourceType": "Image"},
    {"urlPattern": "*.webp", "resourceType": "Image"},
    {"urlPattern": "*.ico", "resourceType": "Image"},
    {"urlPattern": "*.woff*", "resourceType": "Font"},
    {"urlPattern": "*.ttf", "resourceType": "Font"},
    {"urlPattern": "*.css", "resourceType": "Stylesheet"},
]


def setup_m3u8_interception(page: CDPSession, state: dict, try_resolve):
    """Register CDP event handlers to extract pkey from m3u8 playlists.

    Extracts pkey from:
      1. m3u8 URL query params (v2.4.3+: ?pkey=... in the URL)
      2. m3u8 response body (#EXT-X-MOUFLON:PSCH:v2:<pkey>)

    Also handles Fetch.requestPaused to block matched resource patterns.
    """
    request_urls: dict[str, str] = {}  # requestId → url

    def on_request_paused(params):
        asyncio.ensure_future(
            page.send("Fetch.failRequest", {
                "requestId": params["requestId"],
                "errorReason": "BlockedByClient",
            })
        )

    def on_request_will_be_sent(params):
        url = params.get("request", {}).get("url", "")
        if ".m3u8" not in url:
            return
        request_urls[params["requestId"]] = url
        # Extract pkey from URL query params
        if not state.get("pkey"):
            m = re.search(r"[?&]pkey=([A-Za-z][A-Za-z0-9]{13,21})", url)
            if m:
                state["pkey"] = m.group(1)
                print(f'[m3u8  ] pkey from URL param: "{state["pkey"]}"')
                try_resolve()

    async def on_loading_finished(params):
        request_id = params.get("requestId", "")
        if request_id not in request_urls:
            return
        url = request_urls.pop(request_id)
        try:
            body = await page.send(
                "Network.getResponseBody", {"requestId": request_id}
            )
            text = body.get("body", "")
            m = re.search(r"#EXT-X-MOUFLON:PSCH:v2:([A-Za-z0-9]+)", text)
            if m and not state.get("pkey"):
                state["pkey"] = m.group(1)
                print(f'[m3u8  ] pkey from playlist body: "{state["pkey"]}"')
                try_resolve()
        except Exception:
            pass

    page.on("Fetch.requestPaused", on_request_paused)
    page.on("Network.requestWillBeSent", on_request_will_be_sent)
    page.on("Network.loadingFinished", on_loading_finished)


async def trigger_video_play(page: CDPSession):
    """Evaluate video.play() in the page to start HLS buffering."""
    try:
        await page.send("Runtime.evaluate", {
            "expression": (
                "(async () => {"
                "  const v = document.querySelector('video');"
                "  if (v) { v.muted = true; await v.play().catch(() => {}); return 'playing'; }"
                "  return 'no video';"
                "})()"
            ),
            "awaitPromise": True,
            "returnByValue": True,
        })
        print("[play  ] video.play() triggered")
    except Exception as exc:
        print(f"[play  ] video.play() error: {exc}")


# ---------------------------------------------------------------------------
# Phase 3 — Debugger breakpoints and script detection
# ---------------------------------------------------------------------------

# URL substring to identify the MMP player chunk (version-resilient)
CHUNK_URL_PATTERN = "mmp.doppiocdn.com/player/mmp/"

# Regex to find the for-loop + arrow closure that assembles pdkey
FOR_LOOP_RE = re.compile(
    r"for\(let \w+ of this(?:\[.{1,40}\]|\.\w+)\)\w+=\w+\[.{1,30}\]\(\w+,\w+,\(\)=>"
)


def is_key_candidate(v) -> bool:
    """Check if *v* looks like a 14–22 char alphanumeric key starting with alpha."""
    return (
        isinstance(v, str)
        and 14 <= len(v) <= 22
        and v[0].isalpha()
        and v.isalnum()
    )


def _extract_arrow_body(source: str, arrow_body_start: int) -> str | None:
    """Track balanced parens/braces/brackets from *arrow_body_start*
    until a top-level comma to find the end of the arrow body expression."""
    depth = brace_depth = bracket_depth = 0
    limit = min(arrow_body_start + 20000, len(source))
    for i in range(arrow_body_start, limit):
        ch = source[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "{":
            brace_depth += 1
        elif ch == "}":
            brace_depth -= 1
        elif ch == "[":
            bracket_depth += 1
        elif ch == "]":
            bracket_depth -= 1
        if ch == "," and depth == 0 and brace_depth == 0 and bracket_depth == 0:
            return source[arrow_body_start:i]
    return None


async def _handle_chunk_parsed(session: CDPSession, evt: dict,
                                label: str, state: dict):
    """Get script source, find the pdkey closure, and set BPs."""
    try:
        src = await session.send(
            "Debugger.getScriptSource", {"scriptId": evt["scriptId"]}
        )
        source = src.get("scriptSource", "")

        coerce_idx = source.find("coerceTimestamps")
        if coerce_idx < 0:
            print(f"[CDP{label}] chunk has no coerceTimestamps, skipping")
            return
        print(f"[CDP{label}] coerceTimestamps at col {coerce_idx} "
              f"(source len={len(source)})")

        # Search region before coerceTimestamps, fall back to full source
        search_start = max(0, coerce_idx - 100_000)
        region = source[search_start:coerce_idx]
        m = FOR_LOOP_RE.search(region)
        if m:
            for_loop_col = search_start + m.start()
            arrow_body_start = search_start + m.start() + len(m.group())
        else:
            print(f"[CDP{label}] for-loop pattern not in region, trying full source…")
            m = FOR_LOOP_RE.search(source)
            if not m:
                print(f"[CDP{label}] for-loop pattern not found anywhere")
                return
            for_loop_col = m.start()
            arrow_body_start = m.start() + len(m.group())

        print(f"[CDP{label}] for-loop at col {for_loop_col}, "
              f"arrow body starts at col {arrow_body_start}")

        arrow_body = _extract_arrow_body(source, arrow_body_start)
        if arrow_body is None:
            print(f"[CDP{label}] could not find arrow body end")
            return

        print(f"[CDP{label}] arrow body: {len(arrow_body)} chars")
        print(f"[CDP{label}] starts: {arrow_body[:60]}")
        print(f"[CDP{label}] ends:   {arrow_body[-60:]}")

        # Store the raw expression — NOT wrapped in ()
        state["pdkey_closure_expr"] = arrow_body

        # Find valid V8 BP locations near the for-loop
        bp_result = await session.send("Debugger.getPossibleBreakpoints", {
            "start": {
                "scriptId": evt["scriptId"],
                "lineNumber": 0,
                "columnNumber": for_loop_col,
            },
            "end": {
                "scriptId": evt["scriptId"],
                "lineNumber": 0,
                "columnNumber": for_loop_col + 200,
            },
            "restrictToFunction": False,
        })
        locs = bp_result.get("locations", [])
        print(f"[CDP{label}] {len(locs)} possible BPs near for-loop")

        for loc in locs[:5]:
            try:
                r = await session.send("Debugger.setBreakpoint", {
                    "location": {
                        "scriptId": evt["scriptId"],
                        "lineNumber": loc["lineNumber"],
                        "columnNumber": loc["columnNumber"],
                    }
                })
                print(f"[CDP{label}] BP set at col {loc['columnNumber']} "
                      f"id={r['breakpointId']}")
            except Exception as e:
                print(f"[CDP{label}] BP failed col {loc['columnNumber']}: {e}")
    except Exception as e:
        print(f"[CDP{label}] handleChunkParsed error: {e}")


# ---------------------------------------------------------------------------
# Phase 4 — Paused handler and key evaluation
# ---------------------------------------------------------------------------

def _make_paused_handler(session: CDPSession, label: str,
                          state: dict, try_resolve):
    """Return an async Debugger.paused handler that evaluates the closure."""

    async def on_paused(params):
        try:
            col = (params.get("callFrames") or [{}])[0].get(
                "location", {}
            ).get("columnNumber")

            expr = state.get("pdkey_closure_expr")
            if expr and not state.get("pdkey"):
                state["_eval_count"] = state.get("_eval_count", 0) + 1
                if state["_eval_count"] > 3:
                    await session.send("Debugger.resume")
                    return

                frame_id = (params.get("callFrames") or [{}])[0].get(
                    "callFrameId"
                )
                if frame_id:
                    print(f"[{label}][paused] col {col} — evaluating pdkey closure…")
                    r = await session.send("Debugger.evaluateOnCallFrame", {
                        "callFrameId": frame_id,
                        "expression": expr,
                        "returnByValue": True,
                        "throwOnSideEffect": False,
                    })
                    if (
                        "exceptionDetails" not in r
                        and isinstance(r.get("result", {}).get("value"), str)
                    ):
                        val = r["result"]["value"]
                        print(f"[{label}][eval  ] → \"{val}\" (len={len(val)})")
                        # v2.4.2 returned "pkey:pdkey"; v2.4.3+ returns pdkey only
                        if ":" in val:
                            pkey_part, pdkey_part = val.split(":", 1)
                            if is_key_candidate(pkey_part) and is_key_candidate(pdkey_part):
                                if not state.get("pkey"):
                                    state["pkey"] = pkey_part
                                if not state.get("pdkey"):
                                    state["pdkey"] = pdkey_part
                                print(f"[{label}][FOUND ] pkey=\"{state['pkey']}\" "
                                      f"pdkey=\"{state['pdkey']}\"")
                                try_resolve()
                        elif is_key_candidate(val):
                            state["pdkey"] = val
                            print(f"[{label}][FOUND ] pdkey=\"{state['pdkey']}\"")
                            try_resolve()
                    elif "exceptionDetails" in r:
                        exc = r["exceptionDetails"]
                        msg = exc.get("text") or exc.get("exception", {}).get(
                            "description", "?"
                        )
                        print(f"[{label}][eval  ] EXCEPTION: {msg[:200]}")

            await session.send("Debugger.resume")
        except Exception as err:
            try:
                await session.send("Debugger.resume")
            except Exception:
                pass
            print(f"[{label}][CDP] paused handler error: {err}")

    return on_paused


# ---------------------------------------------------------------------------
# Phase 5 — Attach debugger (page + workers)
# ---------------------------------------------------------------------------

async def attach_debugger(session: CDPSession, label: str,
                           state: dict, try_resolve):
    """Enable debugger on *session*, register chunk detection + paused handler."""
    await session.send("Debugger.enable")
    session.on(
        "Debugger.paused",
        _make_paused_handler(session, label, state, try_resolve),
    )

    def on_script_parsed(evt):
        url = evt.get("url", "")
        if not url:
            return
        if CHUNK_URL_PATTERN not in url:
            return
        if "chunk-" not in url:
            return
        print(f"[CDP{label}] script: {url[-70:]} endCol={evt.get('endColumn')}")
        if evt.get("endColumn", 0) < 100_000:
            return
        if state.get("pdkey_closure_expr"):
            return  # only process once
        asyncio.ensure_future(
            _handle_chunk_parsed(session, evt, label, state)
        )

    session.on("Debugger.scriptParsed", on_script_parsed)


# ---------------------------------------------------------------------------
# Phase 6 — Main entry point
# ---------------------------------------------------------------------------

async def main():
    model = sys.argv[1] if len(sys.argv) > 1 else "MetishaCaprice"
    url = f"https://stripchat.com/{model}"
    timeout_s = 90

    print(f"[extract_mmp_keys] Model : {model}")
    print(f"[extract_mmp_keys] URL   : {url}")
    print(f"[extract_mmp_keys] Chunk  : dynamic detection via URL + regex")
    print(f"[extract_mmp_keys] Waiting up to {timeout_s}s for both keys…\n")

    proc, ws_url = await launch_chrome()
    print(f"[launch] DevTools: {ws_url}")

    state: dict = {
        "pkey": None,
        "pdkey": None,
        "pdkey_closure_expr": None,
    }
    done_event = asyncio.Event()

    def try_resolve():
        if state.get("pkey") and state.get("pdkey"):
            done_event.set()

    try:
        browser = await connect_cdp(ws_url)

        # Create page
        target = await browser.send(
            "Target.createTarget", {"url": "about:blank"}
        )
        target_id = target["targetId"]
        attach = await browser.send(
            "Target.attachToTarget",
            {"targetId": target_id, "flatten": True},
        )
        page = browser.child_session(attach["sessionId"])

        # Enable domains
        await page.send("Page.enable")
        await page.send("Network.enable")
        await page.send("Runtime.enable")

        # Block heavy resources
        await page.send(
            "Fetch.enable",
            {"patterns": _BLOCKED_RESOURCE_PATTERNS, "handleAuthRequests": False},
        )
        await page.send(
            "Network.setUserAgentOverride", {"userAgent": _USER_AGENT}
        )

        # m3u8 interception
        setup_m3u8_interception(page, state, try_resolve)

        # Debugger on main page
        await attach_debugger(page, "page", state, try_resolve)

        # Listen for workers — attach debugger to each
        await browser.send("Target.setDiscoverTargets", {"discover": True})

        def on_target_attached(params):
            info = params.get("targetInfo", {})
            t = info.get("type", "")
            if t not in ("worker", "service_worker"):
                return
            sid = params.get("sessionId")
            if not sid:
                return
            w_url = info.get("url", "(unknown)")
            print(f"[worker] Attached {t}: {w_url[-60:]}")
            child = browser.child_session(sid)
            asyncio.ensure_future(
                attach_debugger(child, f"w:{w_url[-30:]}", state, try_resolve)
            )

        browser.on("Target.attachedToTarget", on_target_attached)
        await browser.send("Target.setAutoAttach", {
            "autoAttach": True,
            "waitForDebuggerOnStart": False,
            "flatten": True,
        })

        # Navigate
        print(f"[nav   ] Loading {url} …")
        await page.send("Page.navigate", {"url": url})

        # After 8s, trigger video.play()
        async def delayed_play():
            await asyncio.sleep(8)
            await trigger_video_play(page)

        play_task = asyncio.ensure_future(delayed_play())

        # Wait for both keys or timeout
        try:
            await asyncio.wait_for(done_event.wait(), timeout=timeout_s - 5)
        except asyncio.TimeoutError:
            print("[timeout] Resolving with partial results…")

        play_task.cancel()

        try:
            await browser.send("Browser.close")
        except Exception:
            pass

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

    # ── Results ───────────────────────────────────────────────────────────────
    pkey = state.get("pkey")
    pdkey = state.get("pdkey")

    print("\n══════════════════════════════════════════════")
    print(" MMP KEY EXTRACTION RESULTS")
    print("══════════════════════════════════════════════")

    if pkey and pdkey:
        pair = f"{pkey}:{pdkey}"
        print(f"  pkey  : {pkey}")
        print(f"  pdkey : {pdkey}")
        print(f"\n  keys.txt entry:")
        print(f"  {pair}")
    else:
        print(f"  pkey  : {pkey or '(not captured)'}")
        print(f"  pdkey : {pdkey or '(not captured)'}")
        print("\n  [!] One or both keys missing — check if model is online and streaming.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
