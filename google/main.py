from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Iterable, List

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from export_utils import generate_job_id, write_jobs_csv  # noqa: E402
from google.parser import parse_jobs  # noqa: E402

GOOGLE_DIR = Path(__file__).resolve().parent
DEFAULT_URL = "https://careers.google.com/jobs/results/"
DEFAULT_RAW_PATH = GOOGLE_DIR / "data" / "ds1_listings.json"
DEFAULT_CSV_PATH = GOOGLE_DIR / "jobs.csv"
NEXT_BUTTON_SELECTORS = [
    "button[aria-label='Next page']",
    "div[role='button'][aria-label='Next page']",
    "button:has-text(\"Next\")",
]

CHUNK_CAPTURE_INIT = """
(() => {
    const store = [];
    Object.defineProperty(window, "__dsChunkStore", {
        value: store,
        writable: false,
        configurable: false,
    });

    const cloneChunk = (chunk) => {
        try {
            return JSON.parse(JSON.stringify(chunk));
        } catch (err) {
            return null;
        }
    };

    const capture = (chunk) => {
        if (!chunk || !chunk.key || !chunk.data) return;
        const copy = cloneChunk(chunk);
        if (copy) {
            store.push(copy);
        }
    };

    const wrapCallback = (fn) => {
        if (typeof fn !== "function") {
            return function(chunk) {
                capture(chunk);
            };
        }
        return function(...args) {
            capture(args[0]);
            return fn.apply(this, args);
        };
    };

    let activeCallback = wrapCallback(window.AF_initDataCallback);
    Object.defineProperty(window, "AF_initDataCallback", {
        configurable: true,
        get() {
            return activeCallback;
        },
        set(fn) {
            activeCallback = wrapCallback(fn);
        },
    });

    const queue = Array.isArray(window.AF_initDataChunkQueue)
        ? window.AF_initDataChunkQueue
        : (window.AF_initDataChunkQueue = []);
    for (const chunk of queue) {
        capture(chunk);
    }
    const originalPush = queue.push;
    queue.push = function(...args) {
        for (const chunk of args) {
            capture(chunk);
        }
        return originalPush.apply(this, args);
    };
})();
"""


def _rel_path(path: Path) -> Path:
    try:
        return path.relative_to(ROOT_DIR)
    except ValueError:
        return path


async def _wait_for_ds_chunk(page, chunk_key: str, timeout_ms: int) -> Any:
    js = """
    (chunkKey) => {
        const store = globalThis.__dsChunkStore || [];
        for (const entry of store) {
            if (entry && entry.key === chunkKey && entry.data && !entry.__consumed) {
                entry.__consumed = true;
                return entry;
            }
        }

        const queue = globalThis.AF_initDataChunkQueue;
        if (Array.isArray(queue)) {
            for (const entry of queue) {
                if (entry && entry.key === chunkKey && entry.data && !entry.__consumed) {
                    entry.__consumed = true;
                    return entry;
                }
            }
        }

        const requests = globalThis.AF_dataServiceRequests || {};
        const candidate = requests[chunkKey];
        if (candidate && candidate.data && !candidate.__consumed) {
            candidate.__consumed = true;
            return { key: chunkKey, data: candidate.data };
        }
        return null;
    }
    """
    handle = await page.wait_for_function(js, arg=chunk_key, timeout=timeout_ms)
    chunk = await handle.json_value()
    if not chunk or "data" not in chunk:
        raise RuntimeError(f"Chunk {chunk_key} did not include data")
    return chunk["data"]


async def _click_next_page(page) -> bool:
    for selector in NEXT_BUTTON_SELECTORS:
        locator = page.locator(selector)
        try:
            if await locator.count() == 0:
                continue
            target = locator.first
            if not await target.is_enabled():
                continue
            await target.click()
            await page.wait_for_timeout(500)
            return True
        except Exception:
            continue
    return False


async def fetch_ds1_payloads(
    url: str,
    chunk_key: str,
    timeout: float,
    headless: bool,
    max_pages: int | None = None,
) -> List[Any]:
    timeout_ms = int(timeout * 1000)
    payloads: List[Any] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129 Safari/537.36"
        ))
        page = await context.new_page()
        await page.add_init_script(CHUNK_CAPTURE_INIT)
        try:
            await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            while True:
                data = await _wait_for_ds_chunk(page, chunk_key, timeout_ms)
                payloads.append(data)

                if max_pages and len(payloads) >= max_pages:
                    break

                has_next = await _click_next_page(page)
                if not has_next:
                    break
        finally:
            await context.close()
            await browser.close()
    return payloads


def build_csv_rows(jobs: Iterable[dict[str, str]]) -> List[dict[str, str]]:
    rows: List[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for job in jobs:
        url = job.get("url", "")
        ats_id = job.get("ats_id", "")
        key = (ats_id, url)
        if not any(key):
            continue
        if key in seen:
            continue
        seen.add(key)
        job_id = generate_job_id("google", url, ats_id)
        rows.append({**job, "id": job_id})
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Google Careers listings via Playwright")
    parser.add_argument("--url", default=DEFAULT_URL, help="Jobs results URL to load")
    parser.add_argument("--chunk", default="ds:1", help="AF_initData chunk key to read")
    parser.add_argument("--timeout", type=float, default=25.0, help="Timeout in seconds")
    parser.add_argument(
        "--raw",
        default=str(DEFAULT_RAW_PATH),
        help="Path to store raw ds:1 JSON (multi-page runs suffix _pageN)",
    )
    parser.add_argument("--csv", default=str(DEFAULT_CSV_PATH), help="Output CSV path")
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Launch browser in headed mode (default headless)",
    )
    parser.add_argument(
        "--from-file",
        type=str,
        default=None,
        help="Skip Playwright and parse an existing ds:1 JSON file",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Maximum number of pages to visit (0 = no limit)",
    )
    return parser.parse_args()


async def run(args: argparse.Namespace) -> None:
    raw_path = Path(args.raw)
    raw_path.parent.mkdir(parents=True, exist_ok=True)

    if args.from_file:
        ds1_payloads = [json.loads(Path(args.from_file).read_text())]
    else:
        max_pages = args.max_pages if args.max_pages > 0 else None
        try:
            ds1_payloads = await fetch_ds1_payloads(
                args.url,
                args.chunk,
                args.timeout,
                headless=not args.headed,
                max_pages=max_pages,
            )
        except PlaywrightTimeoutError as exc:
            raise SystemExit(f"Timed out waiting for ds chunk: {exc}") from exc

        if not ds1_payloads:
            raise SystemExit("No ds:1 payloads fetched")

        if len(ds1_payloads) == 1:
            raw_path.write_text(json.dumps(ds1_payloads[0], indent=2))
            print(f"Saved raw ds:1 payload to {_rel_path(raw_path)}")
        else:
            for idx, payload in enumerate(ds1_payloads, start=1):
                page_path = raw_path.with_name(f"{raw_path.stem}_page{idx}{raw_path.suffix}")
                page_path.write_text(json.dumps(payload, indent=2))
                print(f"Saved page {idx} raw payload to {_rel_path(page_path)}")

    jobs: List[dict[str, str]] = []
    for idx, page_payload in enumerate(ds1_payloads, start=1):
        page_jobs = parse_jobs(page_payload)
        print(f"Parsed {len(page_jobs)} jobs from page {idx}")
        jobs.extend(page_jobs)

    print(f"Parsed total of {len(jobs)} jobs across {len(ds1_payloads)} page(s)")

    rows = build_csv_rows(jobs)
    csv_path = Path(args.csv)
    diff_path = write_jobs_csv(csv_path, rows)
    print(f"Wrote {len(rows)} rows to {_rel_path(csv_path)}")
    if diff_path:
        print(f"Diff written to {_rel_path(diff_path)}")


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
