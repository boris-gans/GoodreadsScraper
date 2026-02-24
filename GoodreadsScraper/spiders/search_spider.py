"""Spider to search Goodreads for books and extract description + genres."""

import logging
import urllib.parse

import scrapy

logger = logging.getLogger(__name__)

from ..items import BookItem, BookLoader, SearchResultItem


class SearchSpider(scrapy.Spider):
    """Search Goodreads for books by title/author, then extract description and genres.

    Accepts a list of (row_idx, title, author_or_None) tuples via the `books` argument.
    For each book:
      1. Searches Goodreads: /search?q={title}+{author}
      2. Follows the first book result link
      3. Extracts description and genres from __NEXT_DATA__ JSON
      4. Yields a SearchResultItem
    """

    name = "search"

    def __init__(self, books=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.books = books or []

    def start_requests(self):
        for row_idx, title, author in self.books:
            if author:
                query = f"{title} {author}"
            else:
                query = title
            search_url = (
                "https://www.goodreads.com/search?q="
                + urllib.parse.quote_plus(query)
            )
            yield scrapy.Request(
                search_url,
                callback=self.parse_search,
                cb_kwargs={"row_idx": row_idx, "title": title, "author": author},
                dont_filter=True,
            )

    def parse_search(self, response, row_idx, title, author):
        logger.debug("[SEARCH] Search results page %s — status=%s len=%d", response.url, response.status, len(response.body))
        book_link = response.css('a[href*="/book/show/"]::attr(href)').get()
        if not book_link:
            logger.warning("[SEARCH] No book link found for '%s' by '%s' on %s", title, author, response.url)
            yield SearchResultItem(
                row_idx=row_idx,
                title=title,
                author=author or "",
                description="",
                genres="",
                goodreads_url="",
                status="not_found",
            )
            return

        book_url = response.urljoin(book_link)
        logger.info("[SEARCH] Found book link for '%s': %s", title, book_url)
        yield scrapy.Request(
            book_url,
            callback=self.parse_book,
            cb_kwargs={"row_idx": row_idx, "title": title, "author": author},
            dont_filter=True,
        )

    def parse_book(self, response, row_idx, title, author):
        logger.debug("[SEARCH] Book page %s — status=%s len=%d", response.url, response.status, len(response.body))

        next_data_raw = response.css('script#__NEXT_DATA__::text').get()
        if next_data_raw:
            logger.info("[SEARCH] __NEXT_DATA__ found (%d chars) on %s", len(next_data_raw), response.url)
        else:
            logger.warning("[SEARCH] __NEXT_DATA__ NOT FOUND on %s — fields will be empty", response.url)

        loader = BookLoader(BookItem(), response=response)
        loader.add_css("description", "script#__NEXT_DATA__::text")
        loader.add_css("genres", "script#__NEXT_DATA__::text")
        loader.add_css("publishedYear", "script#__NEXT_DATA__::text")
        item = loader.load_item()

        description = item.get("description", "") or ""
        genres = item.get("genres", []) or []
        published_year = item.get("publishedYear", "") or ""

        logger.info(
            "[SEARCH] Yielding SearchResultItem for '%s' — description=%d chars, genres=%s, year=%s",
            title, len(description), genres, published_year,
        )
        yield SearchResultItem(
            row_idx=row_idx,
            title=title,
            author=author or "",
            description=description,
            genres=";".join(genres) if isinstance(genres, list) else str(genres),
            publishedYear=published_year,
            goodreads_url=response.url,
            status="found",
        )
