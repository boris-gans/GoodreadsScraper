"""Spider to search Goodreads for books and extract description + genres."""

import urllib.parse

import scrapy

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
        book_link = response.css("a.bookTitle::attr(href)").get()
        if not book_link:
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
        yield scrapy.Request(
            book_url,
            callback=self.parse_book,
            cb_kwargs={"row_idx": row_idx, "title": title, "author": author},
            dont_filter=True,
        )

    def parse_book(self, response, row_idx, title, author):
        loader = BookLoader(BookItem(), response=response)
        loader.add_css("description", "script#__NEXT_DATA__::text")
        loader.add_css("genres", "script#__NEXT_DATA__::text")
        item = loader.load_item()

        description = item.get("description", "") or ""
        genres = item.get("genres", []) or []

        yield SearchResultItem(
            row_idx=row_idx,
            title=title,
            author=author or "",
            description=description,
            genres=";".join(genres) if isinstance(genres, list) else str(genres),
            goodreads_url=response.url,
            status="found",
        )
