import json
import subprocess
import time
from typing import Dict, TypedDict

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

SESSION_FILE = "wb_session.json"


SessionHeaders = TypedDict(
    "SessionHeaders",
    {
        "User-Agent": str,
    },
)


class SessionData(TypedDict):
    cookies: Dict[str, str]
    headers: SessionHeaders


def start_chrome():
    subprocess.Popen(
        [
            "google-chrome",
            "--remote-debugging-port=9222",
            "--user-data-dir=/tmp/chrome-cdp-profile",
            "--disable-blink-features=AutomationControlled",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # даём Chrome время запуститься
    time.sleep(3)


def refresh_session() -> SessionData:
    start_chrome()

    with Stealth().use_sync(sync_playwright()) as p:
        browser = p.chromium.connect_over_cdp("http://localhost:9222")

        context = browser.contexts[0]

        page = context.new_page()

        page.goto(
            "https://www.wildberries.ru/catalog/0/search.aspx?search=пальто"
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

        return session
    
if __name__ == "__main__":
    refresh_session()