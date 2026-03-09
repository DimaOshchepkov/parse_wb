import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Dict, TypedDict

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

SESSION_FILE = "wb_session.json"
CDP_URL = "http://localhost:9222"


SessionHeaders = TypedDict(
    "SessionHeaders",
    {
        "User-Agent": str,
    },
)


class SessionData(TypedDict):
    cookies: Dict[str, str]
    headers: SessionHeaders


def _resolve_chrome_binary() -> str:
    explicit = os.getenv("CHROME_PATH")
    if explicit:
        return explicit

    candidates = [
        "chrome",
        "chrome.exe",
        "google-chrome",
        "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
        "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    ]

    for candidate in candidates:
        if os.path.isabs(candidate) and Path(candidate).exists():
            return candidate
        found = shutil.which(candidate)
        if found:
            return found

    raise RuntimeError(
        "Chrome not found. Install Google Chrome or set CHROME_PATH environment variable."
    )


def start_chrome() -> subprocess.Popen:
    chrome_bin = _resolve_chrome_binary()
    profile_dir = Path(".chrome-cdp-profile").resolve()
    profile_dir.mkdir(parents=True, exist_ok=True)

    process = subprocess.Popen(
        [
            chrome_bin,
            "--remote-debugging-port=9222",
            f"--user-data-dir={profile_dir}",
            "--disable-blink-features=AutomationControlled",
            "--no-first-run",
            "--no-default-browser-check",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait until Chrome starts and CDP is ready.
    time.sleep(3)
    return process


def refresh_session() -> SessionData:
    chrome_process = start_chrome()

    try:
        with Stealth().use_sync(sync_playwright()) as p:
            browser = p.chromium.connect_over_cdp(CDP_URL)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = context.new_page()

            page.goto(
                "https://www.wildberries.ru/catalog/0/search.aspx?search=%D0%BF%D0%B0%D0%BB%D1%8C%D1%82%D0%BE"
            )
            page.wait_for_timeout(10000)

            cookies = context.cookies()
            ua = page.evaluate("navigator.userAgent")

            session: SessionData = {
                "cookies": {c["name"]: c["value"] for c in cookies},
                "headers": {
                    "User-Agent": ua,
                },
            }

            with open(SESSION_FILE, "w", encoding="utf-8") as f:
                json.dump(session, f, indent=2)

            browser.close()
            return session
    finally:
        if chrome_process.poll() is None:
            chrome_process.terminate()
            try:
                chrome_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                chrome_process.kill()


if __name__ == "__main__":
    refresh_session()
