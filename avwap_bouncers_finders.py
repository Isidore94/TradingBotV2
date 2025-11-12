#!/usr/bin/env python3
"""
avwap_bounce_notifier.py

Watches combined_avwap.txt and alerts when a line’s ticker appears in bouncers.txt.
- Live tail + periodic full refresh (default 30 min)
- Per-symbol cooldown (default 5 min)
- Prints to console, writes alerts to alerts.log, and opens a second terminal
  that tails alerts.log for a dedicated live view.

Defaults assume this script sits next to:
  combined_avwap.txt, bouncers.txt

Run:
  python avwap_bounce_notifier.py
Options:
  --dir PATH           Base directory (default: script directory)
  --avwap FILENAME     (default: combined_avwap.txt)
  --bouncers FILENAME  (default: bouncers.txt)
  --refresh SECONDS    full-file rescan interval (default: 1800)
  --cooldown SECONDS   per-symbol cooldown (default: 300)
  --no-launch-viewer   do not open secondary terminal
  --show-nonmatches    print non-matching lines for debugging
"""

import os
import re
import sys
import time
import argparse
import hashlib
import platform
import subprocess
from datetime import datetime, timedelta

# Optional color & desktop notifications
try:
    from colorama import init as colorama_init, Fore, Style
    colorama_init(autoreset=True)
    GREEN, RED, CYAN, YELLOW, RESET = Fore.GREEN, Fore.RED, Fore.CYAN, Fore.YELLOW, Style.RESET_ALL
except Exception:
    GREEN = RED = CYAN = YELLOW = RESET = ""

try:
    from plyer import notification
    HAVE_PLYER = True
except Exception:
    HAVE_PLYER = False

try:
    import winsound
    HAVE_WINSOUND = True
except Exception:
    HAVE_WINSOUND = False

SYMBOL_RE = re.compile(r"[A-Z0-9.\-]+")


def ping():
    if HAVE_WINSOUND:
        try:
            winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
        except Exception:
            sys.stdout.write("\a"); sys.stdout.flush()
    else:
        sys.stdout.write("\a"); sys.stdout.flush()


def notify(title: str, message: str):
    if HAVE_PLYER:
        try:
            notification.notify(title=title, message=message, timeout=5)
        except Exception:
            pass


def line_hash(s: str) -> str:
    return hashlib.md5(s.strip().encode("utf-8", errors="ignore")).hexdigest()


def load_bouncers(path: str) -> set:
    """
    Parse bouncers.txt lines like:
      HH:MM:SS | NVDA | VWAP, prev_day_high | long
    Ticker is the 2nd pipe-separated token.
    """
    syms = set()
    if not os.path.exists(path):
        return syms
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = [p.strip() for p in line.strip().split("|")]
            if len(parts) >= 2:
                sym = parts[1].upper()
                if SYMBOL_RE.fullmatch(sym or ""):
                    syms.add(sym)
    return syms


def parse_avwap_line(line: str):
    """
    Expect combined_avwap.txt signal line: ``SYMBOL,MM/DD,LEVEL,SIDE``.
    Section headers (lines starting with ``#``) are ignored.
    Returns tuple or None.
    """
    parts = [p.strip() for p in line.strip().split(",")]
    if len(parts) < 4:
        return None
    sym = parts[0].upper()
    if not SYMBOL_RE.fullmatch(sym or ""):
        return None
    return sym, parts[1], parts[2], parts[3]


def tail_file(path: str):
    """
    Yield new lines appended to `path`, starting at EOF.
    Re-open if file rotates/truncates.
    """
    f = None
    last_inode = None

    def _open():
        fp = open(path, "r", encoding="utf-8", errors="ignore")
        fp.seek(0, os.SEEK_END)
        return fp

    while True:
        try:
            if f is None:
                f = _open()
                last_inode = os.fstat(f.fileno()).st_ino

            pos = f.tell()
            line = f.readline()
            if not line:
                try:
                    st = os.stat(path)
                    if st.st_ino != last_inode or st.st_size < pos:
                        f.close()
                        f = _open()
                        last_inode = os.fstat(f.fileno()).st_ino
                    else:
                        time.sleep(0.3)
                except FileNotFoundError:
                    time.sleep(0.5)
                continue
            yield line

        except FileNotFoundError:
            time.sleep(0.5)
        except Exception as e:
            print(f"{YELLOW}[WARN]{RESET} tail_file error: {e}")
            time.sleep(0.5)


def scan_file(path: str):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield line


def open_secondary_viewer(log_path: str):
    """
    Open a second terminal that tails the given log file.
    """
    sysname = platform.system()
    try:
        if sysname == "Windows":
            # PowerShell window that follows the log
            cmd = [
                "powershell", "-NoExit",
                "-Command", f"$Host.UI.RawUI.WindowTitle='AVWAP Alerts'; "
                            f"Write-Host 'Tailing {log_path}'; "
                            f"Get-Content -Path '{log_path}' -Wait"
            ]
            subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)
        elif sysname == "Darwin":  # macOS
            osa = (
                'tell application "Terminal" to do script '
                f'"printf \\"Tailing {log_path}\\n\\n\\"; tail -f \\"{log_path}\\""'
            )
            subprocess.Popen(["osascript", "-e", osa])
        else:  # Linux / other Unix
            try:
                subprocess.Popen(
                    ["gnome-terminal", "--", "bash", "-c", f"echo 'Tailing {log_path}'; tail -f '{log_path}'; exec bash"]
                )
            except FileNotFoundError:
                subprocess.Popen(
                    ["xterm", "-T", "AVWAP Alerts", "-hold", "-e", f"bash -lc \"echo 'Tailing {log_path}'; tail -f '{log_path}'\""]
                )
    except Exception as e:
        print(f"{YELLOW}[WARN]{RESET} Could not launch secondary viewer: {e}")


def append_alert(log_path: str, line: str):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%H:%M:%S')} | {line}\n")


def handle_line(line: str,
                bouncer_symbols: set,
                last_alert_time: dict,
                cooldown: timedelta,
                show_nonmatches: bool,
                alerts_log: str):
    parsed = parse_avwap_line(line)
    if not parsed:
        if show_nonmatches:
            print(f"{YELLOW}[SKIP]{RESET} {line.strip()}")
        return

    sym, mmdd, level, side = parsed
    if sym in bouncer_symbols:
        now = datetime.now()
        last_t = last_alert_time.get(sym)
        if last_t is None or now - last_t >= cooldown:
            last_alert_time[sym] = now
            msg = f"{sym} | {mmdd} | {level} | {side}"
            print(f"{GREEN}[ALERT]{RESET} {msg}")
            append_alert(alerts_log, msg)
            notify("AVWAP Bounce Alert", msg)
            ping()
        else:
            remaining = int((cooldown - (now - last_t)).total_seconds())
            print(f"{YELLOW}[COOLDOWN]{RESET} {sym} ({remaining}s left)")
    else:
        if show_nonmatches:
            print(line.strip())


def main():
    parser = argparse.ArgumentParser(description="AVWAP + Bouncers notifier")
    parser.add_argument("--dir", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--avwap", default="combined_avwap.txt")
    parser.add_argument("--bouncers", default="bouncers.txt")
    parser.add_argument("--refresh", type=int, default=1800)   # 30 minutes
    parser.add_argument("--cooldown", type=int, default=300)   # 5 minutes
    parser.add_argument("--no-launch-viewer", action="store_true")
    parser.add_argument("--show-nonmatches", action="store_true")
    args = parser.parse_args()

    base = os.path.abspath(args.dir)
    avwap_path = os.path.join(base, args.avwap)
    bouncers_path = os.path.join(base, args.bouncers)
    alerts_log = os.path.join(base, "alerts.log")

    refresh_every = max(5, args.refresh)
    cooldown = timedelta(seconds=args.cooldown)

    # Make sure alerts.log exists
    open(alerts_log, "a", encoding="utf-8").close()

    # Load bouncers
    bouncer_symbols = load_bouncers(bouncers_path)
    last_bouncers_mtime = os.path.getmtime(bouncers_path) if os.path.exists(bouncers_path) else 0.0

    print(f"{CYAN}Directory:{RESET} {base}")
    print(f"{CYAN}Watching:{RESET} {avwap_path}")
    print(f"{CYAN}Bouncers:{RESET} {bouncers_path}  {CYAN}({len(bouncer_symbols)} symbols){RESET}")
    print(f"{CYAN}Refresh every:{RESET} {refresh_every}s   {CYAN}Cooldown:{RESET} {args.cooldown}s")
    print(f"{CYAN}Alerts log:{RESET} {alerts_log}")
    print(f"{CYAN}Desktop notifications:{RESET} {'enabled' if HAVE_PLYER else 'install \"plyer\" to enable'}\n")

    if not args.no_launch_viewer:
        open_secondary_viewer(alerts_log)

    # De-dupe sets
    seen_hashes = set()
    last_alert_time = {}

    # Seed de-dupe so we only alert on new content
    for l in scan_file(avwap_path):
        seen_hashes.add(line_hash(l))

    last_refresh = time.time()

    # Live tail loop
    for l in tail_file(avwap_path):
        # Reload bouncers if changed
        try:
            if os.path.exists(bouncers_path):
                mtime = os.path.getmtime(bouncers_path)
                if mtime > last_bouncers_mtime:
                    bouncer_symbols = load_bouncers(bouncers_path)
                    last_bouncers_mtime = mtime
                    print(f"{CYAN}[INFO]{RESET} Reloaded bouncers ({len(bouncer_symbols)} symbols)")
        except Exception:
            pass

        # Periodic full rescan to catch rewrites/truncations
        now_ts = time.time()
        if now_ts - last_refresh >= refresh_every:
            last_refresh = now_ts
            print(f"{CYAN}[REFRESH]{RESET} rescanning {os.path.basename(avwap_path)} …")
            for rl in scan_file(avwap_path):
                h = line_hash(rl)
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)
                handle_line(rl, bouncer_symbols, last_alert_time, cooldown,
                            args.show_nonmatches, alerts_log)

        # Handle newly tailed line
        h = line_hash(l)
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        handle_line(l, bouncer_symbols, last_alert_time, cooldown,
                    args.show_nonmatches, alerts_log)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExiting.")
