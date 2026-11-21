#!/usr/bin/env python3
"""
BPQ "SPOTS" application (RF-first)

Build features:
- On connect: shows glance + bottom menu (prompts for GRID if missing)
- <ENTER> or SPOTS: refresh glance
- CLEAR: clears grid; next refresh prompts for grid (bugfix)
- Glance format: Option 2 (two columns per line)
- Aggregated by band only (modes ignored)
- "Near me" fixed radius: receiver grid within FIXED_RADIUS_MI of user grid
- ANALYZE <band> gives synopsis + top paths (miles rounded to nearest 10mi)
- Handles WinError 64 disconnects quietly
- Compact storage (deduped index)
- MQTT diagnostics: prints rx stats every 10 seconds
- Background pruning so memory does not grow forever when idle

Commands:
  (Enter)        -> refresh glance
  SPOTS          -> refresh glance
  GRID <FM19ab>  -> set grid for this user
  CLEAR          -> clears grid (forces prompt next refresh / next connect)
  ANALYZE <band> -> analysis for band  (alias: A <band>)
  HELP           -> show commands
  Q / BYE / EXIT / QUIT -> exit
"""

from __future__ import annotations

import asyncio
import gc
import json
import math
import os
import re
import textwrap
import threading
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

# -----------------------------
# Optional memory stats
# -----------------------------
try:
    import psutil  # type: ignore
except Exception:
    psutil = None

# -----------------------------
# Defaults / Config
# -----------------------------

WINDOW_MINUTES_DEFAULT = 15
WINDOW_LABEL = "15 min"
MILES_ROUND_TO_DEFAULT = 10

# User requested: fixed radius, no RAD command
FIXED_RADIUS_MI = 300

DEFAULT_CONFIG = {
    "bpq_app": {"listen_host": "127.0.0.1", "listen_port": 63050},
    "storage": {"user_db_file": "spots_users.json"},
    "window_minutes": WINDOW_MINUTES_DEFAULT,
    "bands_glance": ["80m", "40m", "30m", "20m", "17m", "15m", "12m", "10m", "6m", "2m"],
    # Terminal width to fold output at, and whether the menu follows every
    # reply. See BPQ-Node/NODE-APP-STYLE.md; width 0 disables folding.
    "display": {"width": 42, "menu_every_reply": False},
    "miles_round_to": MILES_ROUND_TO_DEFAULT,
    "pskreporter": {
        "ingest": "mqtt",
        "mqtt": {
            "host": "mqtt.pskreporter.info",
            "port": 1883,
            "topics": [
                "pskr/filter/v2/80m/#",
                "pskr/filter/v2/40m/#",
                "pskr/filter/v2/30m/#",
                "pskr/filter/v2/20m/#",
                "pskr/filter/v2/17m/#",
                "pskr/filter/v2/15m/#",
                "pskr/filter/v2/12m/#",
                "pskr/filter/v2/10m/#",
                "pskr/filter/v2/6m/#",
                "pskr/filter/v2/2m/#",
            ],
            "username": "",
            "password": "",
            "qos": 0,
        },
    },
}

CONFIG_FILE = "spots_config.json"

# Grouped one purpose per line, <ENTER> first and the ?/NODE hint last, inside 42
# columns - see BPQ-Node/NODE-APP-STYLE.md.
MENU_LINE = ("<ENTER> | GRID <FM19ab> | CLEAR\r\n"
             "ANALYZE <band>\r\n"
             "[?=Commands, NODE=Back to Node]\r\n")

# Two columns inside 42: a 15-wide command field, then the gloss.
HELP_TEXT = (
    "Commands:\r\n"
    "  (Enter)        refresh the glance\r\n"
    "  SPOTS          refresh the glance\r\n"
    "  GRID <FM19ab>  set your grid\r\n"
    "  CLEAR          forget your grid\r\n"
    "  ANALYZE <band> analyse one band\r\n"
    "  A <band>       same, shorter\r\n"
    "  HELP           show commands\r\n"
    "  Q / BYE / NODE exit\r\n"
)

# House vocabulary, identical in all four apps. NODE matters: it is what a BPQ
# user types to get back to the node, and this app used to ignore it.
EXIT_WORDS = ("Q", "QUIT", "EXIT", "BYE", "NODE")
HELP_WORDS = ("HELP", "?")

BAND_ALIASES = {
    "80": "80m", "40": "40m", "30": "30m", "20": "20m", "17": "17m", "15": "15m", "12": "12m", "10": "10m", "6": "6m", "2": "2m",
    "80m": "80m", "40m": "40m", "30m": "30m", "20m": "20m", "17m": "17m", "15m": "15m", "12m": "12m", "10m": "10m", "6m": "6m", "2m": "2m",
}

# -----------------------------
# Maidenhead utilities
# -----------------------------

_grid_ll_cache: Dict[str, Optional[Tuple[float, float]]] = {}

OUT_WIDTH = 42
MENU_EVERY = False


def wrap_out(text: str, width: int) -> str:
    """Fold over-long lines at word boundaries for a narrow terminal.

    A phone client is about 43 columns, so anything wider folds mid-word.
    Lines already inside the width pass through untouched, which is what keeps
    the glance and path tables in column. See BPQ-Node/NODE-APP-STYLE.md.
    """
    if width <= 0 or not text:
        return text
    out = []
    for raw in text.replace("\r\n", "\n").splitlines(keepends=True):
        end = "\n" if raw.endswith("\n") else ""
        body = raw[:-1] if end else raw
        if len(body) <= width:
            out.append(body + end)
            continue
        lead = body[:len(body) - len(body.lstrip(" "))]
        folded = textwrap.wrap(body.strip(), width=width,
                               initial_indent=lead,
                               subsequent_indent=lead or "  ",
                               break_long_words=True, break_on_hyphens=False)
        out.append("\n".join(folded or [body]) + end)
    return "".join(out)


def send(writer: asyncio.StreamWriter, text: str) -> None:
    """The one place bytes leave this app.

    Output used to go straight to writer.write() in sixteen places, so there
    was nowhere to fold or strip control characters. Unlike WX and
    RepeaterSearch this app is not on a TRANS connection, so it keeps CRLF
    rather than converting to bare CR.
    """
    body = wrap_out(text, OUT_WIDTH).replace("\r\n", "\n")
    body = re.sub(r"[^\x09\x0a\x20-\x7e]", "", body)
    writer.write(body.replace("\n", "\r\n").encode("utf-8", "ignore"))


def grid_to_latlon(grid: str) -> Optional[Tuple[float, float]]:
    """Return (lat, lon) center of Maidenhead grid (4 or 6 chars). Cached."""
    g = (grid or "").strip().upper()
    if len(g) < 4:
        return None
    g = g[:6]
    if g in _grid_ll_cache:
        return _grid_ll_cache[g]

    if not (g[0].isalpha() and g[1].isalpha() and g[2].isdigit() and g[3].isdigit()):
        _grid_ll_cache[g] = None
        return None

    lon = (ord(g[0]) - ord("A")) * 20 - 180
    lat = (ord(g[1]) - ord("A")) * 10 - 90
    lon += int(g[2]) * 2
    lat += int(g[3]) * 1

    if len(g) >= 6 and g[4].isalpha() and g[5].isalpha():
        lon += (ord(g[4]) - ord("A")) * (5.0 / 60.0)
        lat += (ord(g[5]) - ord("A")) * (2.5 / 60.0)
        lon += (5.0 / 60.0) / 2.0
        lat += (2.5 / 60.0) / 2.0
    else:
        lon += 1.0
        lat += 0.5

    _grid_ll_cache[g] = (lat, lon)
    return (lat, lon)

def haversine_miles(lat1, lon1, lat2, lon2) -> float:
    R_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    km = R_km * c
    return km * 0.621371

def round_miles(x: float, nearest: int) -> int:
    if nearest <= 0:
        return int(round(x))
    return int(nearest * round(x / nearest))

def maybe_trim_grid_cache(max_size: int = 5000) -> None:
    global _grid_ll_cache
    if len(_grid_ll_cache) <= max_size:
        return
    # keep it simple: clear it if it grows unusually large
    _grid_ll_cache.clear()

# -----------------------------
# Compact spot index (deduped)
# -----------------------------

@dataclass(frozen=True)
class SpotKey:
    band: str
    rx_grid4: str
    sender: str

@dataclass
class SpotVal:
    ts: float
    sender_grid4: str

class SpotIndex:
    """
    Stores only latest timestamp per (band, receiver_grid4, sender_call).
    Compact and stable for high MQTT volume.
    """
    def __init__(self, window_minutes: int):
        self.window_s = window_minutes * 60
        self._lock = threading.Lock()
        self._data: Dict[SpotKey, SpotVal] = {}
        self._by_rx: Dict[str, Set[SpotKey]] = {}

    def add(self, band: str, sender: str, sender_grid4: str, rx_grid4: str, ts: float) -> None:
        key = SpotKey(band=band, rx_grid4=rx_grid4, sender=sender)
        val = SpotVal(ts=ts, sender_grid4=sender_grid4)
        with self._lock:
            existed = key in self._data
            self._data[key] = val
            if not existed:
                self._by_rx.setdefault(rx_grid4, set()).add(key)

    def prune(self) -> int:
        cutoff = time.time() - self.window_s
        removed = 0
        with self._lock:
            to_del = [k for k, v in self._data.items() if v.ts < cutoff]
            for k in to_del:
                self._data.pop(k, None)
                s = self._by_rx.get(k.rx_grid4)
                if s:
                    s.discard(k)
                    if not s:
                        self._by_rx.pop(k.rx_grid4, None)
                removed += 1
        return removed

    def size(self) -> int:
        with self._lock:
            return len(self._data)

    def receiver_grids(self) -> List[str]:
        with self._lock:
            return list(self._by_rx.keys())

    def iter_for_rx_grid4(self, rx_grid4: str) -> List[Tuple[SpotKey, SpotVal]]:
        with self._lock:
            keys = list(self._by_rx.get(rx_grid4, set()))
            out: List[Tuple[SpotKey, SpotVal]] = []
            for k in keys:
                v = self._data.get(k)
                if v:
                    out.append((k, v))
            return out

# -----------------------------
# User DB (grid only)
# -----------------------------

class UserDB:
    def __init__(self, path: str):
        self.path = Path(path)
        self.data: Dict[str, Dict] = {}
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}
        else:
            self.data = {}

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
        tmp.replace(self.path)

    def get_grid(self, callsign: str) -> Optional[str]:
        cs = (callsign or "").strip().upper()
        return (self.data.get(cs, {}) or {}).get("grid")

    def set_grid(self, callsign: str, grid: str) -> bool:
        cs = (callsign or "").strip().upper()
        g = (grid or "").strip().upper()
        if grid_to_latlon(g) is None:
            return False
        self.data.setdefault(cs, {})["grid"] = g[:6]
        self.save()
        return True

    def clear(self, callsign: str) -> None:
        cs = (callsign or "").strip().upper()
        if cs in self.data:
            self.data.pop(cs, None)
            self.save()

# -----------------------------
# Glance + analysis logic (near-me fixed radius)
# -----------------------------

def _near_rx_grids(index: SpotIndex, user_grid: str, radius_mi: int) -> List[str]:
    ug_ll = grid_to_latlon(user_grid)
    if ug_ll is None:
        return []
    near: List[str] = []
    for rx4 in index.receiver_grids():
        rx_ll = grid_to_latlon(rx4)
        if rx_ll is None:
            continue
        d = haversine_miles(ug_ll[0], ug_ll[1], rx_ll[0], rx_ll[1])
        if d <= radius_mi:
            near.append(rx4)
    return near

def counts_near_user(index: SpotIndex, user_grid: str, radius_mi: int, bands: List[str], window_minutes: int) -> Dict[str, int]:
    index.prune()
    near_rx = _near_rx_grids(index, user_grid, radius_mi)
    cutoff = time.time() - (window_minutes * 60)

    per_band_senders: Dict[str, Set[str]] = {b: set() for b in bands}
    for rx4 in near_rx:
        for k, v in index.iter_for_rx_grid4(rx4):
            if v.ts < cutoff:
                continue
            if k.band in per_band_senders:
                per_band_senders[k.band].add(k.sender)

    return {b: len(per_band_senders[b]) for b in bands}

def make_glance_option2(grid: str, counts: Dict[str, int], bands: List[str]) -> str:
    """From Past 15 Min, Grid EL96XL
    BAND/SPOT COUNT
    80m/0    | 40m/224
    30m/147  | 20m/540

    Each cell is band/count, two per row. The left cells are padded to the
    widest one so the "|" and the right column line up down the screen."""
    lines: List[str] = [f"From Past {WINDOW_LABEL.title()}, Grid {grid}",
                        "BAND/SPOT COUNT"]
    cells = [f"{b}/{counts.get(b, 0)}" for b in bands]
    lefts, rights = cells[0::2], cells[1::2]
    width = max((len(c) for c in lefts), default=0)
    for i, left in enumerate(lefts):
        row = left.ljust(width)
        if i < len(rights):
            row += " | " + rights[i]
        lines.append(row.rstrip())
    return "\r\n".join(lines) + "\r\n"

def analyze_band(index: SpotIndex, user_grid: str, radius_mi: int, band_in: str, miles_round_to: int, window_minutes: int) -> str:
    index.prune()
    band_key = (band_in or "").strip().lower()
    band = BAND_ALIASES.get(band_key, BAND_ALIASES.get(band_key.replace("m", ""), ""))
    if not band:
        return "Use: ANALYZE 20m (or: A 20m)\r\n"

    ug_ll = grid_to_latlon(user_grid)
    if ug_ll is None:
        return f"ANALYZE {band} ({WINDOW_LABEL}) Grid:{user_grid}\r\nInvalid grid.\r\n"

    near_rx = _near_rx_grids(index, user_grid, radius_mi)
    cutoff = time.time() - (window_minutes * 60)

    by_sender_grid: Dict[str, int] = {}
    for rx4 in near_rx:
        for k, v in index.iter_for_rx_grid4(rx4):
            if v.ts < cutoff:
                continue
            if k.band != band:
                continue
            sg4 = (v.sender_grid4 or "").upper()
            if len(sg4) < 4:
                continue
            by_sender_grid[sg4] = by_sender_grid.get(sg4, 0) + 1

    if not by_sender_grid:
        return f"ANALYZE {band} ({WINDOW_LABEL}) Grid:{user_grid}\r\nNo data for this band in the last {WINDOW_LABEL}.\r\n"

    items: List[Tuple[str, int, int]] = []
    for sg4, hits in by_sender_grid.items():
        sg_ll = grid_to_latlon(sg4)
        if sg_ll is None:
            continue
        miles = haversine_miles(ug_ll[0], ug_ll[1], sg_ll[0], sg_ll[1])
        miles_r = round_miles(miles, miles_round_to)
        items.append((sg4, miles_r, hits))
    items.sort(key=lambda x: (x[2], x[1]), reverse=True)
    top = items[:5]

    dists = [m for (_, m, _) in top]
    regional = any(300 <= d <= 800 for d in dists)
    longhaul = any(d >= 2000 for d in dists)
    intercont = any(d >= 4000 for d in dists)

    bits = []
    if regional and longhaul:
        bits.append(f"{band} is open regionally and long-haul.")
    elif regional:
        bits.append(f"{band} looks mainly regional.")
    elif longhaul:
        bits.append(f"{band} looks mainly long-haul.")
    else:
        bits.append(f"{band} shows limited reach.")
    if intercont:
        bits.append("Intercontinental reach is present.")

    higher_bands = ["15m", "12m", "10m", "6m"]
    higher_counts = counts_near_user(index, user_grid, radius_mi, higher_bands, window_minutes)
    bits.append("MUF likely elevated." if sum(higher_counts.values()) >= 5 else "MUF likely moderate.")

    out: List[str] = []
    out.append(f"ANALYZE {band} ({WINDOW_LABEL}) Grid:{user_grid}")
    out.append(f"Synopsis: {' '.join(bits)}")
    out.append("Top paths:")
    out.append(f"  {'GRID':<6} {'MI':>6}  {'SPOTS':>6}")
    for sg4, miles_r, hits in top:
        out.append(f"  {sg4:<6} {miles_r:>6}  {hits:>6}")
    return "\r\n".join(out) + "\r\n"

# -----------------------------
# MQTT ingestion + stats
# -----------------------------

class PskMqttIngestor:
    def __init__(self, index: SpotIndex, cfg: Dict):
        self.index = index
        self.cfg = cfg
        self.client = None
        self.rx_total = 0
        self.rx_last = 0
        self.last_topic = ""

    def start(self):
        try:
            import paho.mqtt.client as mqtt  # type: ignore
        except Exception:
            print("[MQTT] paho-mqtt not installed. Run: py -m pip install paho-mqtt")
            return

        mcfg = self.cfg["pskreporter"]["mqtt"]
        host = mcfg["host"]
        port = int(mcfg["port"])
        topics = mcfg.get("topics") or ["pskr/filter/v2/#"]
        user = mcfg.get("username") or None
        pwd = mcfg.get("password") or None
        qos = int(mcfg.get("qos", 0))

        self.client = mqtt.Client()
        if user and pwd:
            self.client.username_pw_set(user, pwd)

        def on_connect(client, userdata, flags, rc):
            print(f"[MQTT] Connected rc={rc}")
            for t in topics:
                print(f"[MQTT] Subscribing {t}")
                client.subscribe(t, qos=qos)

        def on_message(client, userdata, msg):
            self.rx_total += 1
            self.last_topic = msg.topic
            self.parse_topic(msg.topic)

        self.client.on_connect = on_connect
        self.client.on_message = on_message
        self.client.connect(host, port, keepalive=60)
        self.client.loop_start()

        def reporter():
            while True:
                time.sleep(10)
                delta = self.rx_total - self.rx_last
                self.rx_last = self.rx_total
                stored = self.index.size()
                if self.rx_total:
                    print(f"[MQTT] Spots rx: +{delta}/10s total={self.rx_total} stored={stored} last={self.last_topic[:140]}")
                else:
                    print("[MQTT] Spots rx: 0 (no messages yet)")
        threading.Thread(target=reporter, daemon=True).start()

    def parse_topic(self, topic: str):
        # pskr/filter/v2/<band>/<mode>/<sender>/<receiver>/<senderLocator>/<receiverLocator>/...
        parts = topic.split("/")
        if len(parts) < 9:
            return
        if parts[0] != "pskr" or parts[1] != "filter" or parts[2] != "v2":
            return

        band = parts[3]
        sender = parts[5].upper() if len(parts) > 5 else ""
        sgrid = parts[7].upper() if len(parts) > 7 else ""
        rgrid = parts[8].upper() if len(parts) > 8 else ""

        if band in ("", "+") or sender in ("", "+"):
            return
        if len(sgrid) < 4 or sgrid == "+":
            return
        if len(rgrid) < 4 or rgrid == "+":
            return

        self.index.add(
            band=band,
            sender=sender,
            sender_grid4=sgrid[:4],
            rx_grid4=rgrid[:4],
            ts=time.time(),
        )

# -----------------------------
# Background maintenance
# -----------------------------

def start_maintenance(index: SpotIndex) -> None:
    def loop():
        proc = psutil.Process(os.getpid()) if psutil else None
        while True:
            time.sleep(60)
            removed = index.prune()
            maybe_trim_grid_cache()
            gc.collect()

            msg = f"[MAINT] pruned={removed} stored={index.size()} grid_cache={len(_grid_ll_cache)}"
            if proc is not None:
                rss_mb = proc.memory_info().rss / 1024 / 1024
                msg += f" rss={rss_mb:.1f}MB"
            print(msg)

    threading.Thread(target=loop, daemon=True).start()

# -----------------------------
# Config load + upgrade
# -----------------------------

def apply_display_config(cfg: Dict) -> None:
    """Adopt display.width / display.menu_every_reply from config.

    A typo in either must not stop the node coming up, so both fall back.
    """
    global OUT_WIDTH, MENU_EVERY
    d = cfg.get("display") or {}
    try:
        OUT_WIDTH = int(d.get("width", OUT_WIDTH))
    except (TypeError, ValueError):
        print(f"[CFG] display.width unreadable - keeping {OUT_WIDTH}.")
    MENU_EVERY = bool(d.get("menu_every_reply", MENU_EVERY))


def load_and_upgrade_config() -> Dict:
    cfg_path = Path(CONFIG_FILE)
    if not cfg_path.exists():
        cfg_path.write_text(json.dumps(DEFAULT_CONFIG, indent=2), encoding="utf-8")
        print(f"[CFG] Created {CONFIG_FILE}.")
        return DEFAULT_CONFIG

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    # A config written before display settings existed simply has none, so
    # fill them from the defaults rather than making the sysop edit the file.
    for k, v in DEFAULT_CONFIG.items():
        if k not in cfg:
            cfg[k] = v
        elif isinstance(v, dict):
            for kk, vv in v.items():
                cfg[k].setdefault(kk, vv)

    try:
        mqtt = cfg.get("pskreporter", {}).get("mqtt", {})
        if "topics" not in mqtt:
            mqtt["topics"] = DEFAULT_CONFIG["pskreporter"]["mqtt"]["topics"]
            mqtt.pop("topic", None)
            cfg.setdefault("pskreporter", {})["mqtt"] = mqtt
            cfg_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
            print("[CFG] Upgraded mqtt.topic -> mqtt.topics")
    except Exception:
        pass

    return cfg

# -----------------------------
# BPQ TCP server / session handler
# -----------------------------

async def prompt_for_grid(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Optional[str]:
    send(writer, "LIVE SPOTS needs your grid.\r\n"
                 "Enter a grid (FM19 or FM19ab), or Q:\r\n> ")
    await writer.drain()
    try:
        line = await reader.readline()
    except (ConnectionResetError, OSError):
        return None
    if not line:
        return None
    ans = apply_line_edits(line.decode("utf-8", "ignore")).strip()
    if ans.upper() in EXIT_WORDS:
        return None
    return ans


def apply_line_edits(s: str) -> str:
    """Apply backspace / DEL the way a terminal would, then drop other control
    characters. A phone client that re-flows a word at its wrap column sends
    'neptune' BS BS 'ne'; kept raw, that read as 'neptunene'. Same helper in
    every node app (NODE-APP-STYLE.md s5)."""
    out = []
    for ch in s:
        if ch in "\x08\x7f":
            if out:
                out.pop()
        elif ch >= " " or ch == "\t":
            out.append(ch)
    return "".join(out)

async def show_glance(index: SpotIndex, writer: asyncio.StreamWriter, grid: str,
                      cfg: Dict, menu: bool = False) -> None:
    """The glance, and the menu only when it is actually wanted.

    It used to print the four-line menu after every glance, which buried the
    numbers it had just produced - see BPQ-Node/NODE-APP-STYLE.md.
    """
    counts = counts_near_user(index, grid, FIXED_RADIUS_MI, cfg["bands_glance"],
                              int(cfg["window_minutes"]))
    send(writer, make_glance_option2(grid, counts, cfg["bands_glance"]))
    send(writer, "\r\n" + (MENU_LINE if (menu or MENU_EVERY) else "") + "> ")
    await writer.drain()

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                        index: SpotIndex, userdb: UserDB, cfg: Dict):
    try:
        first = await asyncio.wait_for(reader.readline(), timeout=5.0)
    except Exception:
        try:
            writer.close()
        except Exception:
            pass
        return

    callsign = apply_line_edits(first.decode("utf-8", "ignore")).strip().upper() or "UNKNOWN"

    grid = userdb.get_grid(callsign)
    if not grid:
        ans = await prompt_for_grid(reader, writer)
        if not ans:
            try:
                writer.close()
            except Exception:
                pass
            return
        if not userdb.set_grid(callsign, ans):
            send(writer, "Invalid grid. 73!\r\n")
            await writer.drain()
            try:
                writer.close()
            except Exception:
                pass
            return
        grid = userdb.get_grid(callsign)

    grid = grid or "????"
    await show_glance(index, writer, grid, cfg, menu=True)

    while True:
        try:
            raw = await reader.readline()
        except (ConnectionResetError, OSError):
            break
        if not raw:
            break

        cmdline = apply_line_edits(raw.decode("utf-8", "ignore")).strip()
        # A blank line under the command the operator just typed, so the
        # answer reads as a separate block. See NODE-APP-STYLE.md.
        send(writer, "\r\n")

        if not cmdline or cmdline.upper() == "SPOTS":
            grid = userdb.get_grid(callsign)
            if not grid:
                ans = await prompt_for_grid(reader, writer)
                if not ans:
                    break
                if not userdb.set_grid(callsign, ans):
                    send(writer, "Invalid grid. Example: FM19 or FM19ab\r\n> ")
                    await writer.drain()
                    continue
                grid = userdb.get_grid(callsign)
            await show_glance(index, writer, grid or "????", cfg)
            continue

        parts = cmdline.split()
        op = parts[0].upper()

        if op in EXIT_WORDS:
            send(writer, "73!\r\n")
            await writer.drain()
            break

        if op in HELP_WORDS:
            send(writer, HELP_TEXT + "\r\n" + MENU_LINE + "> ")
            await writer.drain()
            continue

        if op == "GRID" and len(parts) < 2:
            send(writer, "Usage: GRID <FM19ab>, e.g. GRID EL96xl\r\n> ")
            await writer.drain()
            continue

        if op == "GRID" and len(parts) >= 2:
            if userdb.set_grid(callsign, parts[1]):
                grid = userdb.get_grid(callsign) or "????"
                send(writer, f"OK Grid:{grid}\r\n")
                await show_glance(index, writer, grid, cfg)
            else:
                send(writer, "Invalid grid. Example: FM19 or FM19ab\r\n> ")
                await writer.drain()
            continue

        if op in ("CLEAR", "CEAR"):
            userdb.clear(callsign)
            send(writer, "OK Grid cleared. You will be asked for one "
                         "next time.\r\n> ")
            await writer.drain()
            continue

        if op in ("ANALYZE", "A") and len(parts) >= 2:
            grid = userdb.get_grid(callsign)
            if not grid:
                ans = await prompt_for_grid(reader, writer)
                if not ans:
                    break
                if not userdb.set_grid(callsign, ans):
                    send(writer, "Invalid grid. Example: FM19 or FM19ab\r\n> ")
                    await writer.drain()
                    continue
                grid = userdb.get_grid(callsign)

            out = analyze_band(
                index=index,
                user_grid=grid or "????",
                radius_mi=FIXED_RADIUS_MI,
                band_in=parts[1],
                miles_round_to=int(cfg["miles_round_to"]),
                window_minutes=int(cfg["window_minutes"]),
            )
            send(writer, out + "\r\n> ")
            await writer.drain()
            continue

        if op in ("ANALYZE", "A"):
            send(writer, "Usage: ANALYZE <band>, e.g. ANALYZE 20m\r\n> ")
            await writer.drain()
            continue

        # Same wording as the other apps, and it quotes what it did not
        # understand rather than just saying "Unknown".
        send(writer, f"'{cmdline[:20]}' is not a command.\r\n"
                     + "\r\n" + MENU_LINE + "> ")
        await writer.drain()

    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass

# -----------------------------
# Main server
# -----------------------------

async def run_server():
    cfg = load_and_upgrade_config()
    apply_display_config(cfg)

    userdb = UserDB(cfg["storage"]["user_db_file"])
    index = SpotIndex(window_minutes=int(cfg["window_minutes"]))

    start_maintenance(index)

    ing = cfg.get("pskreporter", {}).get("ingest", "off").lower()
    if ing == "mqtt":
        PskMqttIngestor(index, cfg).start()
    else:
        print("[INGEST] PSKReporter ingestion is OFF. Counts will remain 0.")

    host = cfg["bpq_app"]["listen_host"]
    port = int(cfg["bpq_app"]["listen_port"])
    server = await asyncio.start_server(lambda r, w: handle_client(r, w, index, userdb, cfg), host, port)
    print(f"[BPQAPP] Listening on {host}:{port}")
    async with server:
        await server.serve_forever()

def main():
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()