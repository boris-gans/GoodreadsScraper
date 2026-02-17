"""High-throughput Goodreads book search scraper.

Reads a parquet dataset of books, searches each on Goodreads, and extracts
descriptions and genres. Uses multiprocessing with Scrapy for throughput.

Usage:
    python search_crawl.py --input books.parquet --output results.csv --workers 10
    python search_crawl.py --input books.parquet --output results.csv --workers 10 --resume
"""

import argparse
import csv
import multiprocessing
import os
import sys
import logging

import pyarrow.parquet as pq
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn


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


def run_worker(worker_id, books, output_file, checkpoint_file, log_file):
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
        "COOKIES_ENABLED": False,
        "RETRY_TIMES": 2,
        "DOWNLOAD_TIMEOUT": 15,
        "LOG_FILE": log_file,
        "LOG_LEVEL": "WARNING",
        "ITEM_PIPELINES": {
            "GoodreadsScraper.pipelines.CsvSearchResultPipeline": 300,
        },
        "SEARCH_OUTPUT_FILE": output_file,
        "SEARCH_CHECKPOINT_FILE": checkpoint_file,
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
    }

    process = CrawlerProcess(settings)
    process.crawl("search", books=books)
    process.start()


def merge_csv_files(worker_files, final_output):
    """Merge per-worker CSV files into a single output CSV."""
    columns = ["title", "author", "genres", "description", "goodreads_url", "status"]
    with open(final_output, "w", newline="", encoding="utf-8") as outf:
        writer = csv.DictWriter(outf, fieldnames=columns)
        writer.writeheader()
        for wf in worker_files:
            if not os.path.exists(wf):
                continue
            with open(wf, "r", encoding="utf-8") as inf:
                reader = csv.DictReader(inf)
                for row in reader:
                    writer.writerow(row)


def main():
    parser = argparse.ArgumentParser(description="Search Goodreads for book descriptions and genres.")
    parser.add_argument("--input", required=True, help="Input parquet file path")
    parser.add_argument("--output", default="search_results.csv", help="Output CSV file path")
    parser.add_argument("--workers", type=int, default=10, help="Number of worker processes")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoints")
    args = parser.parse_args()

    # Create output directories
    os.makedirs("checkpoints", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    print(f"Reading parquet: {args.input}")
    books = read_books_from_parquet(args.input)
    print(f"Total books in parquet: {len(books)}")

    # Filter out completed books on resume
    if args.resume:
        completed = load_checkpoint("checkpoints")
        print(f"Already completed: {len(completed)} books")
        books = [(idx, t, a) for idx, t, a in books if idx not in completed]
        print(f"Remaining: {len(books)} books")

    if not books:
        print("No books to process.")
        return

    # Partition books across workers
    num_workers = min(args.workers, len(books))
    partitions = [[] for _ in range(num_workers)]
    for i, book in enumerate(books):
        partitions[i % num_workers].append(book)

    worker_csv_files = []
    processes = []

    print(f"Launching {num_workers} workers...")

    for worker_id in range(num_workers):
        output_file = os.path.join("checkpoints", f"worker_{worker_id}.csv")
        checkpoint_file = os.path.join("checkpoints", f"checkpoint_{worker_id}.txt")
        log_file = os.path.join("logs", f"worker_{worker_id}.log")
        worker_csv_files.append(output_file)

        p = multiprocessing.Process(
            target=run_worker,
            args=(worker_id, partitions[worker_id], output_file, checkpoint_file, log_file),
        )
        processes.append(p)
        p.start()

    # Wait for all workers with progress display
    with Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeRemainingColumn(),
        "/",
        TimeElapsedColumn(),
        TextColumn("{task.completed}/{task.total} books"),
    ) as progress:
        task = progress.add_task("[cyan]Searching Goodreads...", total=len(books))

        while any(p.is_alive() for p in processes):
            # Count completed items from checkpoint files
            completed_count = 0
            for worker_id in range(num_workers):
                cp_file = os.path.join("checkpoints", f"checkpoint_{worker_id}.txt")
                if os.path.exists(cp_file):
                    with open(cp_file) as f:
                        completed_count += sum(1 for line in f if line.strip())
            progress.update(task, completed=completed_count)

            # Poll every 2 seconds
            for p in processes:
                p.join(timeout=2)
                if not any(p.is_alive() for p in processes):
                    break

        # Final count
        completed_count = 0
        for worker_id in range(num_workers):
            cp_file = os.path.join("checkpoints", f"checkpoint_{worker_id}.txt")
            if os.path.exists(cp_file):
                with open(cp_file) as f:
                    completed_count += sum(1 for line in f if line.strip())
        progress.update(task, completed=completed_count)

    # Check for worker failures
    for i, p in enumerate(processes):
        if p.exitcode != 0:
            print(f"Warning: Worker {i} exited with code {p.exitcode}. Check logs/worker_{i}.log")

    # Merge per-worker CSVs
    print(f"Merging results into {args.output}...")
    merge_csv_files(worker_csv_files, args.output)

    # Count final results
    if os.path.exists(args.output):
        with open(args.output) as f:
            row_count = sum(1 for _ in f) - 1  # minus header
        print(f"Done. {row_count} results written to {args.output}")
    else:
        print("Done. No output file generated.")


if __name__ == "__main__":
    main()
