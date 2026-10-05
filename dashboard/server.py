#!/usr/bin/env python
#
###############################################################################
#   Copyright (C) 2016-2026  Cortney T. Buffington, N0MJS <n0mjs@me.com>
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 3 of the License, or
#   (at your option) any later version.
###############################################################################

'''
HBlink3 dashboard backend.

Connects to HBlink3's NDJSON reporting feed (one JSON object per line), keeps the
authoritative display state, and serves a single-page UI that receives live JSON
deltas over a WebSocket. Run: python server.py  (or via run_dashboard.py).
'''

import asyncio
import datetime
import json
import logging
import os
import socket
import ssl
import sys
import time
from collections import deque
from contextlib import asynccontextmanager
from urllib.request import urlopen

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
import ijson
import uvicorn

from dmr_utils3.utils import mk_id_dict, get_alias

# Ensure THIS directory's config.py wins over HBlink3's top-level config.py.
HERE = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(HERE, 'static')
sys.path.insert(0, HERE)

# ---- configuration -----------------------------------------------------------
try:
    from config import (REPORT_NAME, HBLINK_IP, HBLINK_PORT, WEB_HOST, WEB_PORT,
                         LOG_LINES, PATH, PEER_FILE, SUBSCRIBER_FILE, TGID_FILE,
                         LOCAL_SUB_FILE, LOCAL_PEER_FILE)
except ImportError:
    sys.exit('No config.py found -- copy config_sample.py to config.py and edit it.')

try:
    from config import TRY_DOWNLOAD, PEER_URL, SUBSCRIBER_URL, STALE_DAYS
except ImportError:
    TRY_DOWNLOAD = False
    PEER_URL = 'https://www.radioid.net/static/rptrs.json'
    SUBSCRIBER_URL = 'https://www.radioid.net/static/users.json'
    STALE_DAYS = 7

try:
    from config import FILTER_COUNTRIES
except ImportError:
    FILTER_COUNTRIES = None

try:
    from config import LAST_HEARD, LAST_HEARD_COUNT
except ImportError:
    LAST_HEARD = 'open'
    LAST_HEARD_COUNT = 10

try:
    from config import SERVER_REPEATERS
except ImportError:
    SERVER_REPEATERS = 'open'

# Talkgroup IDs never shown to browsers (e.g. emergency groups): the name still shows,
# but the number is removed from every stream event, bridge and state snapshot sent
# out. The call audit log on disk keeps the real ID.
try:
    from config import HIDE_TGIDS
except ImportError:
    HIDE_TGIDS = []
_HIDE_TGIDS = set(int(t) for t in HIDE_TGIDS)

# Directory for the call audit log (one JSON line per finished incoming call, in
# daily files calls-YYYY-MM-DD.jsonl, UTC dates). '' disables it.
try:
    from config import CALL_LOG_DIR
except ImportError:
    CALL_LOG_DIR = ''

# Feed transport: 'tcp' (default, connect to HBLINK_IP:HBLINK_PORT) or 'unix'
# (connect to the daemon's local Unix socket HBLINK_SOCKET). Optional in config.
try:
    from config import HBLINK_TRANSPORT
except ImportError:
    HBLINK_TRANSPORT = 'tcp'
try:
    from config import HBLINK_SOCKET
except ImportError:
    HBLINK_SOCKET = ''

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('hbdash')

# Drop an "active" stream this many seconds after its START if no END arrives.
# This is only a backstop for a lost/never-sent terminator while the feed stays
# connected -- a dropped feed already clears all active streams on disconnect,
# and HBlink3 itself synthesizes an END a couple seconds after audio stops
# (bridge.py STREAM_TIMEOUT). Keep it well above any real transmission (amateur
# TOT is ~3 min) so legitimate long calls are never clipped mid-stream.
STREAM_STALE = 300


# ---- alias resolution --------------------------------------------------------
def _abs(p):
    return p if os.path.isabs(p) else os.path.join(HERE, p)

# RadioID record fields kept on download (users have fname; repeaters don't). Repeaters
# also keep color_code, frequency, offset and trustee: Trestle reads them for its monitor,
# the Add repeater prefill and callsign verification (trustee = who may manage it).
_ID_FIELDS = ('id', 'callsign', 'fname', 'city', 'state', 'country', 'color_code',
              'frequency', 'offset', 'trustee')

def _stream_id_file(url, path, json_key, countries, stale_secs):
    now = time.time()
    if os.path.isfile(path) and (os.path.getmtime(path) + stale_secs) >= now:
        logger.info('ID ALIAS MAPPER: %s is current, not downloaded', os.path.basename(path))
        return
    no_verify = ssl._create_unverified_context()
    tmp = path + '.tmp'
    try:
        with urlopen(url, context=no_verify) as response, \
             open(tmp, 'w', encoding='utf-8') as out:
            out.write('{"%s":[' % json_key)
            first = True
            for record in ijson.items(response, json_key + '.item'):
                if not countries or record.get('country') in countries:
                    if not first:
                        out.write(',')
                    # id + callsign drive the aliases; the rest is the detail line
                    # (name, location) shown with a caller or repeater
                    json.dump({k: record[k] for k in _ID_FIELDS if record.get(k) not in (None, '')}, out)
                    first = False
            out.write(']}')
        os.replace(tmp, path)
        label = ', '.join(sorted(countries)) if countries else 'all countries'
        logger.info('ID ALIAS MAPPER: %s downloaded (%s)', os.path.basename(path), label)
    except IOError as e:
        logger.error('ID ALIAS MAPPER: download of %s failed: %s', os.path.basename(path), e)
        if os.path.exists(tmp):
            os.remove(tmp)

def _download_aliases():
    if not TRY_DOWNLOAD:
        return
    base = _abs(PATH)
    stale_secs = int(STALE_DAYS) * 86400
    countries = set(FILTER_COUNTRIES) if FILTER_COUNTRIES else None
    _stream_id_file(PEER_URL,       base + PEER_FILE,       'rptrs', countries, stale_secs)
    _stream_id_file(SUBSCRIBER_URL, base + SUBSCRIBER_FILE, 'users', countries, stale_secs)

# One-line detail for an ID from its RadioID record: "Geoff · Richmond, British Columbia"
# (users) or "Vancouver, British Columbia" (repeaters). Country is added only when it
# isn't Canada. Stored as one string per ID to keep ~300k entries small in memory.
def _detail(rec):
    loc = [rec.get('city', ''), rec.get('state', '')]
    if rec.get('country') and rec.get('country') != 'Canada':
        loc.append(rec['country'])
    loc = ', '.join(x.strip() for x in loc if x and x.strip())
    name = (rec.get('fname') or '').strip()
    return ' · '.join(x for x in (name, loc) if x)

def _mk_detail_dict(path):
    out = {}
    try:
        with open(path, encoding='utf-8', errors='replace') as f:
            recs = json.load(f)
    except (OSError, ValueError):
        return out
    for rec in next(iter(recs.values()), []):
        try:
            d = _detail(rec)
            if d:
                out[int(rec['id'])] = d
        except (KeyError, ValueError, TypeError):
            pass
    return out

def _reload_aliases():
    global PEER_IDS, SUBSCRIBER_IDS, TALKGROUP_IDS, PEER_INFO, SUBSCRIBER_INFO
    base = _abs(PATH)
    PEER_INFO       = _mk_detail_dict(base + PEER_FILE)
    SUBSCRIBER_INFO = _mk_detail_dict(base + SUBSCRIBER_FILE)
    PEER_IDS       = mk_id_dict(base, PEER_FILE)
    SUBSCRIBER_IDS = mk_id_dict(base, SUBSCRIBER_FILE)
    TALKGROUP_IDS  = mk_id_dict(base, TGID_FILE)
    if LOCAL_PEER_FILE:
        PEER_IDS.update(mk_id_dict(base, LOCAL_PEER_FILE))
    if LOCAL_SUB_FILE:
        SUBSCRIBER_IDS.update(mk_id_dict(base, LOCAL_SUB_FILE))
    logger.info('aliases loaded: %d peers, %d subscribers, %d talkgroups (details: %d peers, %d subscribers)',
                len(PEER_IDS), len(SUBSCRIBER_IDS), len(TALKGROUP_IDS),
                len(PEER_INFO), len(SUBSCRIBER_INFO))

PEER_IDS = {}
SUBSCRIBER_IDS = {}
TALKGROUP_IDS = {}
PEER_INFO = {}
SUBSCRIBER_INFO = {}

async def _alias_refresh_loop():
    while True:
        await asyncio.sleep(86400)
        logger.info('aliases: starting daily refresh')
        await asyncio.to_thread(_download_aliases)
        _reload_aliases()

# Logo
try:
    from config import LOGO_FILE
except ImportError:
    LOGO_FILE = ''
_logo_path = _abs(LOGO_FILE) if LOGO_FILE else ''
_logo_exists = bool(_logo_path and os.path.isfile(_logo_path))
_logo_media_type = {
    '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
    '.gif': 'image/gif', '.svg': 'image/svg+xml', '.webp': 'image/webp',
}.get(os.path.splitext(_logo_path)[1].lower(), 'image/png') if _logo_exists else 'image/png'
LOGO_HTML = '<img src="/logo" alt="" class="logo">' if _logo_exists else ''

def alias(_id, _dict):
    a = get_alias(_id, _dict)
    return None if a == _id else a


# ---- shared state ------------------------------------------------------------
class State:
    def __init__(self):
        self.systems = {}                 # last 'config' event payload
        self.bridges = {}                 # last 'bridges' event payload (enriched)
        self.streams = {}                 # key -> last START stream event (active calls)
        self.log = deque(maxlen=LOG_LINES)
        self.hblink = False
        self.clients = set()              # connected dashboard WebSockets
        self.ping_time = 5                # PING_TIME from hblink global config
        self.max_missed = 3               # MAX_MISSED from hblink global config
        self.ping_loss_warn = 5           # PING_LOSS_WARN %: gold callsign at/above this

STATE = State()


# ---- loss summary: per system and per repeater, in memory ----
# Built from RX call ENDs that carry a loss figure, over a rolling window, and shown
# on the Server / Outbound / OpenBridge system tables. Resets when the dashboard
# restarts (long-term metrics are a separate, future dashboard).
LOSS_WINDOW_SECS = 24 * 3600

class LossSummary:
    def __init__(self):
        self.calls = deque()   # (time, system, peer, loss %, duration s)

    def add(self, evt, when=None):
        dur = evt.get('duration') or 0.0
        if evt.get('loss') is None or dur <= 0:
            return
        self.calls.append((when or time.time(), evt['system'], evt['peer'], evt['loss'], dur))

    @staticmethod
    def _group(rows, keyf):
        g = {}
        for r in rows:
            e = g.setdefault(keyf(r), {'calls': 0, '_w': 0.0, '_d': 0.0, 'worst': 0.0})
            e['calls'] += 1
            e['_w'] += r[3] * r[4]       # duration-weighted, so short calls don't dominate
            e['_d'] += r[4]
            e['worst'] = max(e['worst'], r[3])
        for e in g.values():
            e['avg'] = round(e.pop('_w') / e['_d'], 1) if e['_d'] else 0.0
            e.pop('_d')
        return g

    def snapshot(self):
        cutoff = time.time() - LOSS_WINDOW_SECS
        while self.calls and self.calls[0][0] < cutoff:
            self.calls.popleft()
        rows = list(self.calls)
        # systems: keyed by hblink system name (Outbound, OpenBridge)
        # peers:   keyed "system|peer id" (each repeater of a Server system)
        return {'type': 'loss_summary', 'window_secs': LOSS_WINDOW_SECS,
                'systems': self._group(rows, lambda r: r[1]),
                'peers': self._group(rows, lambda r: '{}|{}'.format(r[1], r[2]))}

LOSS = LossSummary()


# ---- call audit log ------------------------------------------------------------
# Every finished incoming (RX) call is appended as one JSON line, with names resolved
# at the time of the call, so there's a durable record beyond the in-memory call log.
# Records keep the stream-event field names, so they reload straight into the call
# log on startup. Files are never pruned here.
_CALL_KEYS = ('call_type', 'system', 'stream_id', 'peer', 'peer_alias', 'peer_info', 'src',
              'src_alias', 'src_info', 'slot', 'dst', 'dst_alias', 'duration', 'rssi', 'loss',
              'loss_src')

def _call_log_path(ts):
    return os.path.join(CALL_LOG_DIR, 'calls-{}.jsonl'.format(
        datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime('%Y-%m-%d')))

def write_call_record(evt):
    if not CALL_LOG_DIR:
        return
    end = evt.get('_ts') or time.time()
    rec = {'end': datetime.datetime.fromtimestamp(end, datetime.timezone.utc).isoformat(timespec='seconds'),
           'start': datetime.datetime.fromtimestamp(end - (evt.get('duration') or 0),
                                                    datetime.timezone.utc).isoformat(timespec='seconds')}
    rec.update({k: evt[k] for k in _CALL_KEYS if evt.get(k) is not None})
    try:
        os.makedirs(CALL_LOG_DIR, exist_ok=True)
        with open(_call_log_path(end), 'a') as f:
            f.write(json.dumps(rec, separators=(',', ':')) + '\n')
    except OSError as e:
        logger.error('call log: could not write %s: %s', CALL_LOG_DIR, e)

def load_recent_calls(n, since=0.0):
    """The newest n call records (ending after `since`) as END stream events,
    newest first."""
    if not CALL_LOG_DIR or not os.path.isdir(CALL_LOG_DIR):
        return []
    out = []
    for name in sorted((f for f in os.listdir(CALL_LOG_DIR)
                        if f.startswith('calls-') and f.endswith('.jsonl')), reverse=True):
        try:
            with open(os.path.join(CALL_LOG_DIR, name)) as f:
                lines = f.readlines()
        except OSError:
            continue
        for line in reversed(lines):
            try:
                rec = json.loads(line)
                end = datetime.datetime.fromisoformat(rec['end']).timestamp()
            except (ValueError, KeyError):
                continue
            if end < since:
                return out                      # files and lines are in time order
            evt = {k: rec[k] for k in _CALL_KEYS if k in rec}
            evt.update({'type': 'stream_end', 'action': 'END', 'trx': 'RX', '_ts': end})
            out.append(evt)
            if len(out) >= n:
                return out
    return out


def stream_key(evt):
    return '{}|{}|{}'.format(evt['system'], evt['slot'], evt['stream_id'])

def enrich_stream(evt):
    evt['src_alias'] = alias(evt['src'], SUBSCRIBER_IDS)
    evt['peer_alias'] = alias(evt['peer'], PEER_IDS)
    evt['src_info'] = SUBSCRIBER_INFO.get(evt['src'])     # name · location
    evt['peer_info'] = PEER_INFO.get(evt['peer'])         # repeater location
    # A unit call's destination is a subscriber; a group call's is a talkgroup.
    if evt.get('call_type') == 'UNIT VOICE':
        evt['dst_alias'] = alias(evt['dst'], SUBSCRIBER_IDS)
    else:
        evt['dst_alias'] = alias(evt['dst'], TALKGROUP_IDS)
    return evt

# Convert server-side absolute epochs to "seconds since/until" deltas so the
# browser can tick live counters without depending on the client's clock.
def enrich_config(systems):
    now = time.time()
    for sysview in systems.values():
        if sysview['MODE'] == 'SERVER':
            for p in sysview.get('REPEATERS', {}).values():
                p['connected_secs'] = int(max(0, now - p.get('CONNECTED', now)))
                last_ping = p.get('LAST_PING', 0)
                p['last_ping_secs'] = int(max(0, now - last_ping)) if last_ping else None
        elif sysview['MODE'] == 'OUTBOUND':
            c = sysview.get('STATS', {}).get('CONNECTED')
            sysview['STATS']['connected_secs'] = int(max(0, now - c)) if c else None
    return systems

def enrich_bridges(bridges):
    now = time.time()
    for members in bridges.values():
        for m in members:
            m['TGID_NAME'] = alias(m['TGID'], TALKGROUP_IDS)
            if m['TO_TYPE'] in ('ON', 'OFF'):
                m['remaining'] = int(m['TIMER'] - now)
            else:
                m['remaining'] = None
    return bridges


def redact(obj):
    """Copy of a browser-bound message with HIDE_TGIDS numbers removed."""
    if not _HIDE_TGIDS or not isinstance(obj, dict):
        return obj
    t = obj.get('type')
    if t in ('stream_start', 'stream_end', 'stream_update') or 'action' in obj:
        if obj.get('dst') in _HIDE_TGIDS:
            obj = dict(obj, dst=None)
        return obj
    if t == 'bridges' or 'bridges' in obj:
        out = {}
        for name, members in obj['bridges'].items():
            out[name] = [dict(m, TGID=None if m.get('TGID') in _HIDE_TGIDS else m.get('TGID'),
                              ON=[x for x in m.get('ON', []) if x not in _HIDE_TGIDS],
                              OFF=[x for x in m.get('OFF', []) if x not in _HIDE_TGIDS])
                         for m in members]
        obj = dict(obj, bridges=out)
    return obj

async def broadcast(obj):
    obj = redact(obj)
    dead = set()
    for ws in STATE.clients:
        try:
            await ws.send_json(obj)
        except Exception:
            dead.add(ws)
    STATE.clients -= dead


async def handle_event(evt):
    global FEED_READ_TIMEOUT
    t = evt.get('type')
    if t == 'config':
        interval = evt.get('report_interval')
        if interval:
            FEED_READ_TIMEOUT = max(float(interval) * 3, 30.0)
        STATE.ping_time = evt.get('ping_time', STATE.ping_time)
        STATE.max_missed = evt.get('max_missed', STATE.max_missed)
        STATE.ping_loss_warn = evt.get('ping_loss_warn', STATE.ping_loss_warn)
        STATE.systems = enrich_config(evt['systems'])
        await broadcast({'type': 'config', 'systems': STATE.systems,
                         'ping_time': STATE.ping_time, 'max_missed': STATE.max_missed,
                         'ping_loss_warn': STATE.ping_loss_warn})
    elif t in ('repeater_connected', 'repeater_disconnected'):
        # Granular repeater connect/disconnect delta. Apply it to the in-memory
        # systems view and re-broadcast the (enriched) config so the browser
        # renders it without waiting for the next full push. Ignored if the
        # system isn't known yet -- the on-connect snapshot will carry it.
        sysview = STATE.systems.get(evt.get('system'))
        if sysview is not None and sysview.get('MODE') == 'SERVER':
            reps = sysview.setdefault('REPEATERS', {})
            rid = str(evt.get('radio_id'))
            if t == 'repeater_connected' and evt.get('info') is not None:
                reps[rid] = evt['info']
            elif t == 'repeater_disconnected':
                reps.pop(rid, None)
            STATE.systems = enrich_config(STATE.systems)
            await broadcast({'type': 'config', 'systems': STATE.systems,
                             'ping_time': STATE.ping_time, 'max_missed': STATE.max_missed,
                         'ping_loss_warn': STATE.ping_loss_warn})
    elif t == 'bridges':
        STATE.bridges = enrich_bridges(evt['bridges'])
        await broadcast({'type': 'bridges', 'bridges': STATE.bridges})
    elif t == 'stream_update':
        # Live mid-call reading (latest RSSI, loss so far). Keep the stored START
        # current so a browser that connects mid-call gets it; not logged -- the END
        # carries the call's figures.
        cur = STATE.streams.get(stream_key(evt))
        if cur is not None:
            for k in ('rssi', 'loss'):
                if k in evt:
                    cur[k] = evt[k]
            await broadcast(evt)
    elif t in ('stream_start', 'stream_end'):
        enrich_stream(evt)
        evt['_ts'] = time.time()   # authoritative server time so "Last Heard" age is correct across reloads
        key = stream_key(evt)
        if evt['action'] == 'START':
            evt['_seen'] = time.time()
            STATE.streams[key] = evt
        else:
            STATE.streams.pop(key, None)
        # Log ingress (RX) legs only, matching the original monitor's behavior.
        if evt['trx'] == 'RX':
            STATE.log.appendleft(evt)
            if evt['action'] == 'END':
                write_call_record(evt)
        await broadcast(evt)
        if evt['action'] == 'END' and evt['trx'] == 'RX' and evt.get('loss') is not None:
            LOSS.add(evt)
            await broadcast(LOSS.snapshot())
    elif t == 'ping':
        pass   # liveness heartbeat; receiving it already reset the feed read timeout
    else:
        logger.debug('ignoring unknown event type: %s', t)


# hblink3 pushes a config snapshot every REPORT_INTERVAL seconds -- that push is
# the de-facto heartbeat. If we go long enough with no line at all, the link is
# dead (whether or not a clean FIN arrived); without this, a silently-severed
# connection leaves readline() blocked forever and the reconnect loop never runs.
# The timeout must be safely LARGER than the push interval or it false-trips on a
# healthy idle link -- so we size it to 3x the interval the daemon advertises (see
# handle_event), not a fixed value. TCP keepalive (above) stays the primary
# detector of a truly dead socket. The default applies only until the first
# config arrives (which the daemon sends immediately on connect).
FEED_READ_TIMEOUT = 180.0


def _enable_tcp_keepalive(writer):
    sock = writer.get_extra_info('socket')
    if sock is None:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if hasattr(socket, 'TCP_KEEPIDLE'):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 4)
    except OSError as e:
        logger.warning('could not set TCP keepalive on HBlink3 feed: %s', e)


# ---- HBlink3 feed client (with reconnect) ------------------------------------
async def hblink_feed():
    while True:
        writer = None
        try:
            if HBLINK_TRANSPORT == 'unix':
                reader, writer = await asyncio.open_unix_connection(HBLINK_SOCKET)
                logger.info('connected to HBlink3 feed at unix socket %s', HBLINK_SOCKET)
            else:
                reader, writer = await asyncio.open_connection(HBLINK_IP, HBLINK_PORT)
                _enable_tcp_keepalive(writer)
                logger.info('connected to HBlink3 feed at %s:%s', HBLINK_IP, HBLINK_PORT)
            STATE.hblink = True
            await broadcast({'type': 'hblink', 'connected': True})
            while True:
                try:
                    line = await asyncio.wait_for(reader.readline(), FEED_READ_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.warning('no data from HBlink3 for %ss; assuming link dead, reconnecting',
                                   FEED_READ_TIMEOUT)
                    break
                if not line:
                    break                                  # connection closed
                try:
                    await handle_event(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning('bad JSON line from HBlink3: %r', line[:120])
        except (ConnectionRefusedError, OSError) as e:
            logger.warning('HBlink3 feed unavailable (%s); retrying', e)
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass
            if STATE.hblink:
                STATE.hblink = False
                STATE.systems = {}
                STATE.bridges = {}
                STATE.streams = {}
                await broadcast({'type': 'hblink', 'connected': False})
        await asyncio.sleep(3)                              # reconnect delay


async def reap_streams():
    while True:
        await asyncio.sleep(5)
        now = time.time()
        stale = [k for k, e in STATE.streams.items() if now - e.get('_seen', now) > STREAM_STALE]
        for k in stale:
            STATE.streams.pop(k, None)
        if stale:
            logger.debug('reaped %d stale stream(s)', len(stale))


@asynccontextmanager
async def lifespan(app):
    # Restore the call log and the 24 h loss figures from the audit files, so neither
    # resets on a restart
    for evt in reversed(load_recent_calls(LOG_LINES)):
        STATE.log.appendleft(evt)
    cutoff = time.time() - LOSS_WINDOW_SECS
    for evt in reversed(load_recent_calls(100000, since=cutoff)):
        LOSS.add(evt, when=evt['_ts'])
    if STATE.log:
        logger.info('call log: restored %d call(s) from %s (%d in the loss window)',
                    len(STATE.log), CALL_LOG_DIR, len(LOSS.calls))
    await asyncio.to_thread(_download_aliases)
    _reload_aliases()
    refresher = asyncio.create_task(_alias_refresh_loop())
    feed      = asyncio.create_task(hblink_feed())
    reaper    = asyncio.create_task(reap_streams())
    yield
    feed.cancel()
    reaper.cancel()
    refresher.cancel()

app = FastAPI(lifespan=lifespan)


# ---- HTTP + WebSocket --------------------------------------------------------
@app.get('/', response_class=HTMLResponse)
async def index():
    with open(os.path.join(STATIC, 'dashboard.html'), encoding='utf-8') as f:
        html = f.read()
    return (html.replace('{{REPORT_NAME}}', REPORT_NAME)
                .replace('{{LOGO_HTML}}', LOGO_HTML)
                .replace('{{LAST_HEARD}}', str(LAST_HEARD))
                .replace('{{LAST_HEARD_COUNT}}', str(LAST_HEARD_COUNT))
                .replace('{{SERVER_REPEATERS}}', str(SERVER_REPEATERS)))

@app.get('/logo')
async def serve_logo():
    if not _logo_exists:
        raise HTTPException(status_code=404)
    return FileResponse(_logo_path, media_type=_logo_media_type)

@app.get('/api/state')
async def api_state():
    return JSONResponse(current_state())

def current_state():
    return {
        'report_name': REPORT_NAME,
        'hblink': STATE.hblink,
        'systems': STATE.systems,
        'bridges': redact({'bridges': STATE.bridges})['bridges'],
        'streams': [redact(e) for e in STATE.streams.values()],
        'log': [redact(e) for e in STATE.log],
        'ping_time': STATE.ping_time,
        'max_missed': STATE.max_missed,
        'ping_loss_warn': STATE.ping_loss_warn,
        'loss_summary': LOSS.snapshot(),
    }

@app.websocket('/ws')
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    STATE.clients.add(ws)
    try:
        await ws.send_json({'type': 'initial', **current_state()})
        while True:
            await ws.receive_text()                        # ignore; keep the socket open
    except WebSocketDisconnect:
        pass
    finally:
        STATE.clients.discard(ws)


def main():
    uvicorn.run(app, host=WEB_HOST, port=WEB_PORT, log_level='warning')

if __name__ == '__main__':
    main()
