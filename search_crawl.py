"""High-throughput Goodreads book search scraper.

Reads a parquet dataset of books, searches each on Goodreads, and extracts
descriptions and genres. Uses multiprocessing with Scrapy for throughput.

Usage:
    python search_crawl.py --input books.parquet --output results.csv --workers 10
    python search_crawl.py --input books.parquet --output results.csv --workers 10 --resume
    python search_crawl.py --input books.parquet --output results.csv --workers 10 --debug
"""

import argparse
import csv
import multiprocessing
import os
import shutil

import pyarrow.parquet as pq
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn


def clean_directories(checkpoint_dir, log_dir, output_file):
    """Delete and recreate checkpoint/log dirs; remove previous output CSV."""
    for d in (checkpoint_dir, log_dir):
        if os.path.isdir(d):
            shutil.rmtree(d)
        os.makedirs(d)
    if os.path.exists(output_file):
        os.remove(output_file)


def load_checkpoint(checkpoint_dir):
    """Load all completed row indices from checkpoint files."""
    completed = set()
    if not os.path.isdir(checkpoint_dir):
        return completed
    for fname in os.listdir(checkpoint_dir):
        if fname.startswith("checkpoint_") and fname.endswith(".txt"):
            path = os.path.join(checkpoint_dir, fname)
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        completed.add(int(line))
    return completed


def read_books_from_parquet(parquet_path):
    """Read books from parquet, returning list of (row_idx, title, author_or_None)."""
    table = pq.read_table(parquet_path)
    columns = table.column_names

    titles = table.column("title").to_pylist()

    # Try common author column names
    author_col = None
    for candidate in ("author", "name", "author_name", "authors"):
        if candidate in columns:
            author_col = candidate
            break

    if author_col:
        authors = table.column(author_col).to_pylist()
    else:
        authors = [None] * len(titles)

    books = []
    for i, (title, author) in enumerate(zip(titles, authors)):
        if title:
            # Handle list-type author fields
            if isinstance(author, list):
                author = author[0] if author else None
            books.append((i, str(title), str(author) if author else None))

    return books


def run_worker(worker_id, books, output_file, checkpoint_file, debug=False):
    """Run a Scrapy crawler in a separate process."""
    # Import here to avoid Twisted reactor issues
    from scrapy.crawler import CrawlerProcess

    settings = {
        "BOT_NAME": "GoodreadsScraper",
        "SPIDER_MODULES": ["GoodreadsScraper.spiders"],
        "NEWSPIDER_MODULE": "GoodreadsScraper.spiders",
        "CONCURRENT_REQUESTS": 32,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 32,
        "DOWNLOAD_DELAY": 0,
        "ROBOTSTXT_OBEY": False,
        "RETRY_TIMES": 2,
        "DOWNLOAD_TIMEOUT": 15,
        "ITEM_PIPELINES": {
            "GoodreadsScraper.pipelines.CsvSearchResultPipeline": 300,
        },
        "SEARCH_OUTPUT_FILE": output_file,
        "SEARCH_CHECKPOINT_FILE": checkpoint_file,
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "TELNETCONSOLE_ENABLED": False,
        "REDIRECT_ENABLED": True,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 522, 524, 408, 429, 403],
        "HTTPERROR_ALLOWED_CODES": [301, 302, 303, 307, 308, 403, 404, 429],
        "COOKIES_ENABLED": True,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        },
    }

    if debug:
        # Full debug: verbose logging to console + HTML response dumps
        settings.update({
            "LOG_LEVEL": "DEBUG",
            # No LOG_FILE — logs flow to the subprocess's stdout/stderr (visible in terminal)
            "DOWNLOADER_MIDDLEWARES": {
                "GoodreadsScraper.middlewares.ResponseDebugMiddleware": 543,
            },
            "DEBUG_ENABLED": True,
            "DEBUG_DUMP_DIR": os.path.join("logs", f"debug_responses_worker_{worker_id}"),
            "DEBUG_MAX_SNIPPET": 600,
        })
    else:
        # Quiet mode: only WARNING+ goes to a log file; console stays clean
        settings.update({
            "LOG_LEVEL": "WARNING",
            "LOG_FILE": os.path.join("logs", f"worker_{worker_id}.log"),
            "DEBUG_ENABLED": False,
        })

    process = CrawlerProcess(settings)
    process.crawl("search", books=books)
    process.start()


CSV_COLUMNS = ["title", "author", "genres", "description", "publishedYear", "goodreads_url", "status"]


def merge_csv_files(worker_files, final_output):
    """Merge per-worker CSV files into a single output CSV."""
    with open(final_output, "w", newline="", encoding="utf-8") as outf:
        writer = csv.DictWriter(outf, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for wf in worker_files:
            if not os.path.exists(wf):
                continue
            with open(wf, "r", encoding="utf-8") as inf:
                reader = csv.DictReader(inf)
                for row in reader:
                    writer.writerow({col: row.get(col, "") for col in CSV_COLUMNS})


def main():
    parser = argparse.ArgumentParser(description="Search Goodreads for book descriptions and genres.")
    parser.add_argument("--input", required=True, help="Input parquet file path")
    parser.add_argument("--output", default="search_results.csv", help="Output CSV file path")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker processes")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoints (skips clean)")
    parser.add_argument("--debug", action="store_true", help="Verbose logging + HTML response dumps")
    args = parser.parse_args()

    CHECKPOINT_DIR = "checkpoints"
    LOG_DIR = "logs"

    if args.resume:
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        os.makedirs(LOG_DIR, exist_ok=True)
    else:
        print("Cleaning previous run data (checkpoints, logs, output)...")
        clean_directories(CHECKPOINT_DIR, LOG_DIR, args.output)

    print(f"Reading parquet: {args.input}")
    books = read_books_from_parquet(args.input)
    print(f"Total books in parquet: {len(books):,}")

    if args.resume:
        completed = load_checkpoint(CHECKPOINT_DIR)
        print(f"Already completed: {len(completed):,} books")
        books = [(idx, t, a) for idx, t, a in books if idx not in completed]
        print(f"Remaining: {len(books):,} books")

    if not books:
        print("No books to process.")
        return

    num_workers = min(args.workers, len(books))
    partitions = [[] for _ in range(num_workers)]
    for i, book in enumerate(books):
        partitions[i % num_workers].append(book)

    worker_csv_files = []
    processes = []

    print(
        f"Launching {num_workers} worker(s)  |  "
        f"debug={'on' if args.debug else 'off'}  |  "
        f"{len(books):,} books total"
    )

    for worker_id in range(num_workers):
        output_file = os.path.join(CHECKPOINT_DIR, f"worker_{worker_id}.csv")
        checkpoint_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_{worker_id}.txt")
        worker_csv_files.append(output_file)

        p = multiprocessing.Process(
            target=run_worker,
            args=(worker_id, partitions[worker_id], output_file, checkpoint_file),
            kwargs={"debug": args.debug},
        )
        processes.append(p)
        p.start()

    total_books = len(books)
    next_snapshot_pct = 10

    with Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeRemainingColumn(),
        "/",
        TimeElapsedColumn(),
        TextColumn("{task.completed}/{task.total} books"),
    ) as progress:
        task = progress.add_task(
            f"[cyan]Searching Goodreads...  [white]{num_workers} workers / 0 failed",
            total=total_books,
        )

        while any(p.is_alive() for p in processes):
            completed_count = 0
            for worker_id in range(num_workers):
                cp_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_{worker_id}.txt")
                if os.path.exists(cp_file):
                    with open(cp_file) as f:
                        completed_count += sum(1 for line in f if line.strip())

            active = sum(1 for p in processes if p.is_alive())
            failed = sum(1 for p in processes if not p.is_alive() and p.exitcode != 0)

            progress.update(
                task,
                completed=completed_count,
                description=(
                    f"[cyan]Searching Goodreads...  "
                    f"[white]{active} workers active  "
                    f"{'[red]' if failed else '[white]'}{failed} failed"
                ),
            )

            if total_books > 0:
                pct_done = (completed_count / total_books) * 100
                if pct_done >= next_snapshot_pct:
                    snapshot_file = os.path.join(CHECKPOINT_DIR, f"snapshot_{next_snapshot_pct}pct.csv")
                    merge_csv_files(worker_csv_files, snapshot_file)
                    progress.console.print(f"[green]Saved data snapshot: {snapshot_file}")
                    next_snapshot_pct += 10

            for p in processes:
                p.join(timeout=2)
                if not any(p.is_alive() for p in processes):
                    break

        # Final checkpoint count
        completed_count = 0
        for worker_id in range(num_workers):
            cp_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_{worker_id}.txt")
            if os.path.exists(cp_file):
                with open(cp_file) as f:
                    completed_count += sum(1 for line in f if line.strip())
        progress.update(task, completed=completed_count)

    # Report any failures
    failed_workers = [(i, p.exitcode) for i, p in enumerate(processes) if p.exitcode != 0]
    if failed_workers:
        for i, code in failed_workers:
            log_hint = f"  → check logs/worker_{i}.log" if not args.debug else ""
            print(f"[warn] Worker {i} exited with code {code}{log_hint}")
    else:
        print(f"All {num_workers} workers finished successfully.")

    print(f"Merging results into {args.output}...")
    merge_csv_files(worker_csv_files, args.output)

    if os.path.exists(args.output):
        with open(args.output) as f:
            row_count = sum(1 for _ in f) - 1  # minus header
        print(f"Done. {row_count:,} results written to {args.output}")
    else:
        print("Done. No output file generated.")


if __name__ == "__main__":
    main()
