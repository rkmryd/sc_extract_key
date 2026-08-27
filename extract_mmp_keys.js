#!/usr/bin/env node
/**
 * extract_mmp_keys.js
 *
 * Uses a headless Chromium + CDP debugger to extract the MMP mouflon
 * pkey and pdkey at runtime from the live Doppio player bundle.
 *
 * Strategy (version-resilient dynamic detection):
 *
 *   1. Match the MMP player chunk by URL pattern (mmp.doppiocdn.com/player/mmp/)
 *      and large script size (endColumn > 100000).
 *
 *   2. Find the for(let) loop inside coerceTimestamps's decryption path by
 *      regex: for(let VAR of this[...]...)VAR=VAR[...](VAR,VAR,()=>
 *      This locates the arrow function that assembles the pdkey from numeric
 *      literals in a lookup table.
 *
 *   3. Set a breakpoint at the for-loop so all module-level helpers are in
 *      scope, then evaluateOnCallFrame with the arrow body to get pdkey.
 *
 *   4. pkey is captured from m3u8 playlist requests (URL query param or
 *      #EXT-X-MOUFLON response body).
 *
 *   Breakpoints are registered on the main page AND on every Web Worker /
 *   Service Worker that gets created after navigation.
 *
 * Usage:
 *   NODE_PATH=/tmp/node_modules node extract_mmp_keys.js [model_name]
 *   NODE_PATH=/tmp/node_modules node extract_mmp_keys.js MetishaCaprice
 *
 * Requires:  npm install puppeteer   (done in /tmp, hence NODE_PATH override)
 */

'use strict';

const puppeteer = require('puppeteer');

const MODEL      = process.argv[2] || 'MetishaCaprice';
const PAGE_URL   = `https://stripchat.com/${MODEL}`;
const TIMEOUT_MS = (Number.parseInt(process.env.SC_TIMEOUT_S || '90', 10) || 90) * 1000;
const PDKEY_GRACE_MS = (Number.parseInt(process.env.SC_PDKEY_GRACE_S || '15', 10) || 15) * 1000;

// URL substring to identify the MMP player chunk (version-resilient)
const CHUNK_URL_PATTERN = 'mmp.doppiocdn.com/player/mmp/';

// Regex to find the for-loop + arrow closure that assembles pdkey
const FOR_LOOP_REGEX = /for\(let \w+ of this(?:\[.{1,40}\]|\.\w+)\)\w+=\w+\[.{1,30}\]\(\w+,\w+,\(\)=>/;

function extractArrowBody(source, arrowBodyStart) {
  let depth = 0, braceDepth = 0, bracketDepth = 0;
  for (let i = arrowBodyStart; i < arrowBodyStart + 20000 && i < source.length; i++) {
    const ch = source[i];
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    else if (ch === '{') braceDepth++;
    else if (ch === '}') braceDepth--;
    else if (ch === '[') bracketDepth++;
    else if (ch === ']') bracketDepth--;
    if (ch === ',' && depth === 0 && braceDepth === 0 && bracketDepth === 0) {
      return source.slice(arrowBodyStart, i);
    }
  }
  return null;
}

function findPdkeyExprCandidates(source, coerceIdx) {
  const regionStart = Math.max(0, coerceIdx - 120000);
  const regionEnd = Math.min(source.length, coerceIdx + 40000);
  const region = source.slice(regionStart, regionEnd);
  const candidates = [];
  const seen = new Set();

  const robustForLoop = /for\((?:let|const|var)\s+\w+\s+of\s+this(?:\[[^\]]{1,80}\]|\.\w+)\)\s*\w+\s*=\s*\w+(?:\[[^\]]{1,80}\]|\.\w+)\(\s*[^,]{1,120}\s*,\s*[^,]{1,120}\s*,\s*\(\)\s*=>/g;

  for (const m of region.matchAll(robustForLoop)) {
    const forLoopCol = regionStart + m.index;
    const arrowBodyStart = regionStart + m.index + m[0].length;
    const expr = extractArrowBody(source, arrowBodyStart);
    if (!expr || expr.length < 8 || expr.length > 12000 || seen.has(expr)) continue;
    seen.add(expr);
    candidates.push({ forLoopCol, expr });
    if (candidates.length >= 16) break;
  }

  if (candidates.length === 0) {
    for (const am of region.matchAll(/\(\)\s*=>/g)) {
      const arrowBodyStart = regionStart + am.index + am[0].length;
      const lookbackStart = Math.max(regionStart, regionStart + am.index - 260);
      const lookback = source.slice(lookbackStart, regionStart + am.index);
      const forIdx = lookback.lastIndexOf('for(');
      if (forIdx < 0) continue;
      const snippet = lookback.slice(forIdx);
      if (!snippet.includes(' of this')) continue;
      const forLoopCol = lookbackStart + forIdx;
      const expr = extractArrowBody(source, arrowBodyStart);
      if (!expr || expr.length < 8 || expr.length > 12000 || seen.has(expr)) continue;
      seen.add(expr);
      candidates.push({ forLoopCol, expr });
      if (candidates.length >= 16) break;
    }
  }

  return candidates;
}

// ── helpers ──────────────────────────────────────────────────────────────────

/** Pattern test for a key candidate (14–22 alphanumeric, starts with alpha). */
function isKeyCandidate(v) {
  return (
    typeof v === 'string' &&
    v.length >= 14 && v.length <= 22 &&
    /^[A-Za-z][A-Za-z0-9]+$/.test(v)
  );
}

// ── core paused handler (shared by page AND worker sessions) ─────────────────

/**
 * Build and return a Debugger.paused event handler for a given CDP session.
 * `context` is a label string for log prefixes (e.g. "[page]" or "[worker]").
 * `state` is a shared mutable object { pkey, pdkey } so worker hits update it.
 * `tryResolve` is called after each key capture to check if both are done.
 */
function makePausedHandler(session, context, state, tryResolve) {
  return async function onPaused(evt) {
    try {
      const col = evt.callFrames[0]?.location?.columnNumber;

      const exprCandidates = state.pdkeyExprCandidates?.length
        ? state.pdkeyExprCandidates
        : (state.pdkeyClosureExpr ? [state.pdkeyClosureExpr] : []);

      // Only handle if we have closure candidates ready
      if (exprCandidates.length > 0 && !state.pdkey) {
        state._evalCount = (state._evalCount || 0) + 1;
        if (state._evalCount > 8) {
          await session.send('Debugger.resume');
          return;
        }
        const frameId = evt.callFrames[0]?.callFrameId;
        if (frameId) {
          console.log(`${context}[paused] col ${col} — evaluating pdkey candidates…`);
          for (const [idx, expr] of exprCandidates.slice(0, 8).entries()) {
            const r = await session.send('Debugger.evaluateOnCallFrame', {
              callFrameId: frameId,
              expression: expr,
              returnByValue: true,
              throwOnSideEffect: false,
            });
            if (!r.exceptionDetails && typeof r.result.value === 'string') {
              const val = r.result.value;
              console.log(`${context}[eval  ] cand#${idx + 1} → "${val}" (len=${val.length})`);
              // v2.4.2 returned "pkey:pdkey"; v2.4.3+ returns pdkey only
              if (val.includes(':')) {
                const colonIdx = val.indexOf(':');
                const pkeyPart  = val.slice(0, colonIdx);
                const pdkeyPart = val.slice(colonIdx + 1);
                if (isKeyCandidate(pkeyPart) && isKeyCandidate(pdkeyPart)) {
                  if (!state.pkey)  state.pkey  = pkeyPart;
                  if (!state.pdkey) {
                    state.pdkey = pdkeyPart;
                    state.pdkeyFoundAt = Date.now();
                  }
                  console.log(`${context}[FOUND ] pkey="${state.pkey}" pdkey="${state.pdkey}"`);
                  tryResolve();
                  break;
                }
              } else if (isKeyCandidate(val)) {
                state.pdkey = val;
                state.pdkeyFoundAt = Date.now();
                console.log(`${context}[FOUND ] pdkey="${state.pdkey}"`);
                tryResolve();
                break;
              }
            } else if (r.exceptionDetails && idx === 0) {
              const msg = r.exceptionDetails.text ||
                          r.exceptionDetails.exception?.description || '?';
              console.log(`${context}[eval  ] cand#1 EXCEPTION: ${msg.slice(0, 200)}`);
            }
          }
        }
      }

      await session.send('Debugger.resume');
    } catch (err) {
      await session.send('Debugger.resume').catch(() => {});
      if (!String(err.message || '').includes('Target closed')) {
        console.error(`${context}[CDP] error in paused handler: ${err.message}`);
      }
    }
  };
}

/**
 * Dynamically find the MMP chunk, locate the pdkey closure, and set breakpoints.
 * Called from the Debugger.scriptParsed handler.
 */
async function handleChunkParsed(session, evt, label, state) {
  try {
    const src = await session.send('Debugger.getScriptSource', { scriptId: evt.scriptId });
    const source = src.scriptSource || '';

    // Verify this chunk has coerceTimestamps
    const coerceIdx = source.indexOf('coerceTimestamps');
    if (coerceIdx < 0) {
      console.log(`[CDP${label}] chunk has no coerceTimestamps, skipping`);
      return;
    }
    console.log(`[CDP${label}] coerceTimestamps at col ${coerceIdx} (source len=${source.length})`);

    const matches = findPdkeyExprCandidates(source, coerceIdx);
    if (matches.length === 0) {
      console.log(`[CDP${label}] no pdkey closure candidates found`);
      return;
    }

    state.pdkeyExprCandidates = matches.map(m => m.expr);
    state.pdkeyClosureExpr = state.pdkeyExprCandidates[0];
    console.log(`[CDP${label}] found ${matches.length} closure candidate(s)`);

    for (const [idx, m] of matches.slice(0, 3).entries()) {
      console.log(`[CDP${label}] cand#${idx + 1} for-loop col ${m.forLoopCol}, expr ${m.expr.length} chars`);
      console.log(`[CDP${label}] cand#${idx + 1} starts: ${m.expr.slice(0, 60)}`);
      console.log(`[CDP${label}] cand#${idx + 1} ends:   ${m.expr.slice(-60)}`);
    }

    const seenCols = new Set();
    for (const m of matches.slice(0, 6)) {
      if (seenCols.has(m.forLoopCol)) continue;
      seenCols.add(m.forLoopCol);
      await setBreakpointsNearLoop(session, evt, label, m.forLoopCol);
    }
  } catch (e) {
    console.warn(`[CDP${label}] handleChunkParsed error: ${e.message}`);
  }
}

/**
 * Set breakpoints near a candidate for-loop location.
 */
async function setBreakpointsNearLoop(session, evt, label, forLoopCol) {
  const bpResult = await session.send('Debugger.getPossibleBreakpoints', {
    start: { scriptId: evt.scriptId, lineNumber: 0, columnNumber: forLoopCol },
    end: { scriptId: evt.scriptId, lineNumber: 0, columnNumber: forLoopCol + 220 },
    restrictToFunction: false,
  });
  const locs = bpResult.locations || [];
  console.log(`[CDP${label}] ${locs.length} possible BPs near col ${forLoopCol}`);

  for (const loc of locs.slice(0, 4)) {
    try {
      const r = await session.send('Debugger.setBreakpoint', {
        location: { scriptId: evt.scriptId, lineNumber: loc.lineNumber, columnNumber: loc.columnNumber },
      });
      console.log(`[CDP${label}] BP set at col ${loc.columnNumber} id=${r.breakpointId}`);
    } catch (e) {
      console.warn(`[CDP${label}] BP failed col ${loc.columnNumber}: ${e.message}`);
    }
  }
}

/** Attach debugger and set up dynamic chunk detection on a CDP session. */
async function attachDebugger(cdpSession, label, state, tryResolve) {
  await cdpSession.send('Debugger.enable');
  cdpSession.on('Debugger.paused', makePausedHandler(cdpSession, `[${label}]`, state, tryResolve));

  // When any script is parsed, check if it's the MMP chunk
  cdpSession.on('Debugger.scriptParsed', async (evt) => {
    if (!evt.url) return;
    if (!evt.url.includes(CHUNK_URL_PATTERN)) return;
    if (!evt.url.includes('chunk-')) return;
    console.log(`[CDP${label}] script: ${evt.url.slice(-70)} endCol=${evt.endColumn}`);
    if (evt.endColumn < 100000) return;

    // Only process once (first chunk match wins)
    if (state.pdkeyClosureExpr) return;
    await handleChunkParsed(cdpSession, evt, label, state);
  });
}

// ── main ─────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`[extract_mmp_keys] Model : ${MODEL}`);
  console.log(`[extract_mmp_keys] URL   : ${PAGE_URL}`);
  console.log(`[extract_mmp_keys] Chunk  : dynamic detection via URL + regex`);
  console.log(`[extract_mmp_keys] Waiting up to ${TIMEOUT_MS / 1000}s for both keys…\n`);

  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--blink-settings=imagesEnabled=false',
      '--autoplay-policy=no-user-gesture-required',
      '--mute-audio',
    ],
  });

  // Shared state updated by handlers on any context (page or worker)
  const state = {
    pkey: null,
    pdkey: null,
    pdkeyClosureExpr: null,
    pdkeyExprCandidates: [],
    pdkeyFoundAt: null,
  };
  let m3u8Pkey = null;


  try {
    const page = await browser.newPage();

    // ── Promise that resolves once BOTH keys are captured ─────────────────────
    let resolveDone;
    const keysReady = new Promise(resolve => { resolveDone = resolve; });

    const timer = setTimeout(() => {
      console.log('[timeout] Resolving with partial results…');
      resolveDone();
    }, TIMEOUT_MS - 5_000);
    let pdkeyGraceTimer = null;

    let playTimer = null;  // stored so we can cancel if keys are found early

    function tryResolve() {
      if (state.pkey && state.pdkey) {
        clearTimeout(timer);
        if (pdkeyGraceTimer) clearInterval(pdkeyGraceTimer);
        if (playTimer) clearTimeout(playTimer);
        console.log('[done  ] both keys captured; finishing…');
        resolveDone();
      }
    }

    // Poll to propagate m3u8 fallback into state.pkey
    const poll = setInterval(() => {
      if (m3u8Pkey && !state.pkey) {
        state.pkey = m3u8Pkey;
        console.log(`[m3u8  ] pkey via m3u8 fallback: "${state.pkey}"`);
        tryResolve();
      }
      if (state.pkey && state.pdkey) clearInterval(poll);
    }, 500);

    pdkeyGraceTimer = setInterval(() => {
      if (state.pdkey && !state.pkey && state.pdkeyFoundAt) {
        if ((Date.now() - state.pdkeyFoundAt) >= PDKEY_GRACE_MS) {
          console.log(`[timeout] pdkey captured but pkey missing after ${PDKEY_GRACE_MS / 1000}s grace; resolving partial results…`);
          clearInterval(pdkeyGraceTimer);
          resolveDone();
        }
      }
    }, 500);

    // ── Helper: attach debugger to a CDP session (page or worker) ─────────────
    // ── Attach to main page ───────────────────────────────────────────────────
    const pageSession = await page.target().createCDPSession();
    await attachDebugger(pageSession, 'page', state, tryResolve);

    // ── Attach to any Web Workers that get created ────────────────────────────
    // HLS.js v1+ decrypts encrypted segments in a dedicated worker.
    // Register BPs on every worker's CDP session as soon as it's created.
    // In Puppeteer v21+, WebWorker exposes a `client` getter (CDPSession).
    // Use page.on('workercreated') for page-scoped workers (not browser targets).
    page.on('workercreated', async (worker) => {
      const wurl = worker.url?.() ?? worker.url ?? '(unknown)';
      console.log(`[worker] New web worker: ${wurl}`);
      try {
        // Puppeteer v21+ CdpWebWorker exposes .client as the CDPSession
        const workerSession = worker.client;
        if (!workerSession) throw new Error('.client is undefined');
        const label = `worker:${wurl.slice(-40)}`;
        await attachDebugger(workerSession, label, state, tryResolve);
      } catch (e) {
        console.warn(`[worker] Failed to attach CDP: ${e.message}`);
      }
    });

    // ── Attach to Service Workers (stripchat.com registers one for PWA) ───────
    // Service workers are browser-level targets, not page-scoped workers.
    // If the SW intercepts HLS requests and runs chunk-628 code, vt() fires there.
    browser.on('targetcreated', async (target) => {
      if (target.type() === 'service_worker') {
        const swUrl = target.url();
        console.log(`[SW    ] Service worker created: ${swUrl.slice(0, 100)}`);
        try {
          const swSession = await target.createCDPSession();
          const swLabel = `SW:${swUrl.slice(-40)}`;
          await attachDebugger(swSession, swLabel, state, tryResolve);
        } catch (e) {
          console.warn(`[SW    ] Failed to attach CDP: ${e.message}`);
        }
      }
    });

    // ── Block heavy resources; allow JS and media ─────────────────────────────
    await page.setRequestInterception(true);
    page.on('request', (req) => {
      if (['image', 'font', 'stylesheet'].includes(req.resourceType())) {
        req.abort();
      } else {
        req.continue();
      }
    });

    // ── Intercept m3u8 HLS playlists for pkey cross-check / fallback ──────────
    let segmentCount = 0;
    let m3u8Count = 0;
    page.on('response', async (resp) => {
      const url = resp.url();
      // Extract pkey from m3u8 URL query params (v2.4.3+: pkey=... in URL)
      if (url.includes('.m3u8') && !m3u8Pkey) {
        const pkeyMatch = url.match(/[?&]pkey=([A-Za-z][A-Za-z0-9]{13,21})/);
        if (pkeyMatch) {
          m3u8Pkey = pkeyMatch[1];
          console.log(`[m3u8  ] pkey from URL param: "${m3u8Pkey}"`);
        }
      }
      if (url.includes('.m3u8')) {
        try {
          const text = await resp.text();
          // Also try response body pattern (v2.4.2 format)
          const match = text.match(/#EXT-X-MOUFLON:PSCH:v2:([A-Za-z0-9]+)/);
          if (match && !m3u8Pkey) {
            m3u8Pkey = match[1];
            console.log(`[m3u8  ] pkey from playlist body: "${m3u8Pkey}"`);
          }
          // Log first 5 m3u8 responses to see all playlist formats
          if (m3u8Count < 5 && text.length < 30000) {
            m3u8Count++;
            const relevant = text.split('\n').filter(l =>
              l.includes('MOUFLON') || l.includes('.mp4') || l.includes('.ts') || l.startsWith('#EXT')
            ).slice(0, 20);
            console.log(`[m3u8  ] #${m3u8Count} (${url.slice(-70)}, len=${text.length}):`);
            for (const l of relevant) console.log(`[m3u8  ]   ${l.slice(0, 160)}`);
          }
        } catch (_) {}
      }
      // Log the first few media segment fetches
      if (segmentCount < 5 && (url.match(/\.(ts|aac|mp4|m4s|fmp4)(\?|$)/i) || url.includes('segment'))) {
        segmentCount++;
        console.log(`[seg   ] #${segmentCount} ${resp.status()} ${url.slice(0, 120)}`);
      }
    });

    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
      '(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
    );
    await page.setExtraHTTPHeaders({ 'Accept-Language': 'en-US,en;q=0.9' });

    // Navigate — BPs are already registered so there is no race condition
    console.log(`[nav   ] Loading ${PAGE_URL} …`);
    page.goto(PAGE_URL, { waitUntil: 'domcontentloaded', timeout: TIMEOUT_MS })
      .catch(() => {});  // navigation errors are OK — page stays open

    // After 8 s, explicitly trigger video.play() to ensure buffering+decryption start.
    // The video element may be inside an iframe — check all frames.
    // Store reference so it can be cancelled if keys are found before 8 s.
    playTimer = setTimeout(async () => {
      try {
        const played = await page.evaluate(async () => {
          // Try main frame first
          const v0 = document.querySelector('video');
          if (v0) { await v0.play().catch(() => {}); return 'main frame'; }
          // Check all iframes in the main document
          for (const iframe of document.querySelectorAll('iframe')) {
            try {
              const v = iframe.contentDocument?.querySelector('video');
              if (v) { await v.play().catch(() => {}); return `iframe: ${iframe.src || '(no src)'}`; }
            } catch (_) {}
          }
          // No video found — list what we do have
          const vids = document.querySelectorAll('video').length;
          const iframes = document.querySelectorAll('iframe').length;
          return `not found (videos=${vids}, iframes=${iframes})`;
        });
        console.log(`[play  ] video.play() result: ${played}`);

        // Also try to play inside each frame via page.frames()
        for (const frame of page.frames().slice(1)) {  // skip main frame
          try {
            const r = await frame.evaluate(async () => {
              const v = document.querySelector('video');
              if (v) { v.muted = true; await v.play().catch(() => {}); return v.readyState; }
              return null;
            });
            if (r !== null) {
              console.log(`[play  ] video in child frame readyState=${r}: ${frame.url().slice(0, 80)}`);
            }
          } catch (_) {}
        }
      } catch (e) {
        console.warn(`[play  ] video.play() error: ${e.message}`);
      }
    }, 8000);

    // Wait for both BPs (or timeout)
    await keysReady;
    if (pdkeyGraceTimer) clearInterval(pdkeyGraceTimer);
    clearInterval(poll);

  } finally {
    await browser.close();
  }

  // ── Results ───────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════════');
  console.log(' MMP KEY EXTRACTION RESULTS');
  console.log('══════════════════════════════════════════════');

  const { pkey, pdkey } = state;

  if (pkey && pdkey) {
    const pair = `${pkey}:${pdkey}`;
    console.log(`  pkey  : ${pkey}`);
    console.log(`  pdkey : ${pdkey}`);
    if (m3u8Pkey) {
      const matchStr = m3u8Pkey === pkey ? '✓ matches' : `✗ MISMATCH (m3u8 has "${m3u8Pkey}")`;
      console.log(`  m3u8  : ${matchStr}`);
    }
    console.log('\n  keys.txt entry:');
    console.log(`  ${pair}`);
  } else {
    console.log(`  pkey  : ${pkey  ?? '(not captured)'}`);
    console.log(`  pdkey : ${pdkey ?? '(not captured)'}`);
    if (m3u8Pkey) console.log(`  m3u8  : ${m3u8Pkey} (from playlist)`);
    console.log('\n  [!] One or both keys missing — check if model is online and streaming.');
    process.exitCode = 1;
  }
}

main().catch(e => {
  console.error('Fatal:', e.message);
  process.exit(1);
});
