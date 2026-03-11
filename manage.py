import argparse
import os
import signal
import subprocess
import sys
import time
import venv
from pathlib import Path


def load_env(path=".env"):
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)
    except FileNotFoundError:
        pass


ROOT = Path(__file__).resolve().parent
VENV_DIR = ROOT / ".venv"

JSONL_OUTPUT = os.getenv("JSONL_OUTPUT", "products.jsonl")
XLSX_OUTPUT = os.getenv("XLSX_OUTPUT", "products.xlsx")

REDIS_QUEUE_KEY = os.getenv("REDIS_QUEUE_KEY", "wb:card_tasks")
REDIS_DONE_KEY = os.getenv("REDIS_DONE_KEY", "wb:done")
IN_PROGRESS_KEY = os.getenv("IN_PROGRESS_KEY", "wb:cards:in_progress")


def get_venv_python() -> Path:
    if os.name == "nt":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"


def get_python() -> str:
    venv_python = get_venv_python()
    return str(venv_python if venv_python.exists() else Path(sys.executable))


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, cwd=ROOT)


def run_async(cmd: list[str]) -> subprocess.Popen:
    return subprocess.Popen(cmd, cwd=ROOT)


def help_cmd() -> None:
    print("""
Available commands:

venv            create virtual environment (.venv)
bootstrap       create venv + install Python deps + Playwright Chromium

setup           install Python deps and Playwright Chromium
check           run environment checks

redis-up        start Redis via Docker Compose
redis-down      stop Redis
redis-logs      show Redis logs

refresh         refresh Wildberries session

crawl-wb        run producer spider
crawl-cards     run consumer spider -> JSONL
convert         convert JSONL -> XLSX

run             full pipeline (redis-up -> refresh -> crawl-wb + crawl-cards -> convert)
clean-output    remove output files
reset-redis     clear Redis queue keys
print-env       print redis environment variables

quickstart      bootstrap + run pipeline
help            show this help
""")


def create_venv() -> None:
    if VENV_DIR.exists():
        print(f"Virtual environment already exists: {VENV_DIR}")
        return

    print(f"Creating virtual environment: {VENV_DIR}")
    venv.create(VENV_DIR, with_pip=True)
    print("Virtual environment created")


def setup() -> None:
    python_bin = get_python()

    print(f"Using Python: {python_bin}")
    print("Installing Python dependencies...")
    run([python_bin, "-m", "pip", "install", "-r", "requirements.txt"])

    print("Installing Playwright Chromium...")
    run([python_bin, "-m", "playwright", "install", "chromium"])


def bootstrap() -> None:
    create_venv()
    setup()


def check() -> None:
    python_bin = get_python()
    run([python_bin, "scripts/check_env.py"])


def redis_up() -> None:
    run(["docker", "compose", "-f", "compose.yml", "up", "-d"])


def redis_down() -> None:
    run(["docker", "compose", "-f", "compose.yml", "down"])


def redis_logs() -> None:
    run(["docker", "compose", "-f", "compose.yml", "logs", "-f"])


def refresh() -> None:
    python_bin = get_python()
    run([python_bin, "-m", "src.refresh_session"])


def crawl_wb_cmd() -> list[str]:
    python_bin = get_python()
    return [python_bin, "-m", "scrapy", "crawl", "wb"]


def crawl_cards_cmd() -> list[str]:
    python_bin = get_python()
    return [python_bin, "-m", "scrapy", "crawl", "wb_cards", "-O", JSONL_OUTPUT]


def convert_cmd() -> list[str]:
    python_bin = get_python()
    return [
        python_bin,
        "-m",
        "src.scripts.jsonl_to_xlsx",
        JSONL_OUTPUT,
        XLSX_OUTPUT,
    ]


def crawl_wb() -> None:
    run(crawl_wb_cmd())


def crawl_cards() -> None:
    run(crawl_cards_cmd())


def convert() -> None:
    run(convert_cmd())


def terminate_process(proc: subprocess.Popen, name: str, timeout: float = 10) -> int | None:
    if proc.poll() is not None:
        return proc.returncode

    print(f"Stopping {name}...")

    try:
        if os.name == "nt":
            proc.terminate()
        else:
            proc.send_signal(signal.SIGTERM)

        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print(f"{name} did not stop gracefully, killing...")
        proc.kill()
        return proc.wait()


def run_pipeline() -> None:
    redis_up()
    reset_redis()
    refresh()

    print("Starting wb producer...")
    wb_proc = run_async(crawl_wb_cmd())

    print("Starting wb_cards consumer...")
    cards_proc = run_async(crawl_cards_cmd())

    try:
        wb_returncode = wb_proc.wait()
        if wb_returncode != 0:
            terminate_process(cards_proc, "wb_cards")
            raise subprocess.CalledProcessError(wb_returncode, crawl_wb_cmd())

        print("Producer finished successfully")
        print("Waiting for consumer to finish remaining tasks...")

        cards_returncode = cards_proc.wait()
        if cards_returncode != 0:
            raise subprocess.CalledProcessError(cards_returncode, crawl_cards_cmd())

        print("Converting JSONL to XLSX...")
        convert()

        print(f"\nPipeline completed. JSONL: {JSONL_OUTPUT}, XLSX: {XLSX_OUTPUT}")

    except KeyboardInterrupt:
        print("\nInterrupted by user")
        terminate_process(wb_proc, "wb")
        terminate_process(cards_proc, "wb_cards")
        raise


def clean_output() -> None:
    removed = False

    for filename in (JSONL_OUTPUT, XLSX_OUTPUT):
        path = ROOT / filename
        if path.exists():
            path.unlink()
            print(f"Removed: {filename}")
            removed = True

    if not removed:
        print("Output files not found")


def reset_redis() -> None:
    run([
        "docker", "compose", "-f", "compose.yml",
        "exec", "redis",
        "redis-cli",
        "DEL",
        REDIS_QUEUE_KEY,
        REDIS_DONE_KEY,
        IN_PROGRESS_KEY,
    ])


def print_env() -> None:
    print("REDIS_QUEUE_KEY =", REDIS_QUEUE_KEY)
    print("REDIS_DONE_KEY =", REDIS_DONE_KEY)
    print("IN_PROGRESS_KEY =", IN_PROGRESS_KEY)
    print("PYTHON =", get_python())
    print("JSONL_OUTPUT =", JSONL_OUTPUT)
    print("XLSX_OUTPUT =", XLSX_OUTPUT)


def quickstart() -> None:
    bootstrap()
    run_pipeline()


COMMANDS = {
    "help": help_cmd,
    "venv": create_venv,
    "bootstrap": bootstrap,
    "setup": setup,
    "check": check,
    "redis-up": redis_up,
    "redis-down": redis_down,
    "redis-logs": redis_logs,
    "refresh": refresh,
    "crawl-wb": crawl_wb,
    "crawl-cards": crawl_cards,
    "convert": convert,
    "run": run_pipeline,
    "clean-output": clean_output,
    "reset-redis": reset_redis,
    "print-env": print_env,
    "quickstart": quickstart,
}


def main() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("cmd", nargs="?", default="help")
    args = parser.parse_args()

    if args.cmd in COMMANDS:
        COMMANDS[args.cmd]()
    else:
        print(f"Unknown command: {args.cmd}\n")
        help_cmd()


if __name__ == "__main__":
    load_env()
    main()