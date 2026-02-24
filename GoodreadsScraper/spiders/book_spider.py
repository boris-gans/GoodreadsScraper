"""Spider to extract information from a /book/show type page on Goodreads"""

import logging
import scrapy

from .author_spider import AuthorSpider
from ..items import BookItem, BookLoader, DEBUG

logger = logging.getLogger(__name__)

class BookSpider(scrapy.Spider):
    """Extract information from a /book/show type page on Goodreads

        Technically, this is not a Spider in the sense that
        it is never initialized by scrapy. Consequently,
         - its from_crawler method is never invoked
         - its `crawler` attribute is not set
         - it does not have a list of start_urls or start_requests
         - running this spider with scrapy crawl will do nothing
    """
    name = "book"

    def __init__(self):
        super().__init__()
        self.author_spider = AuthorSpider()

    def parse(self, response, loader=None):
        logger.debug("[BOOK] Parsing %s — status=%s", response.request.url, response.status)

        next_data_raw = response.css('script#__NEXT_DATA__::text').get()
        if next_data_raw:
            logger.info("[BOOK] __NEXT_DATA__ found (%d chars) on %s", len(next_data_raw), response.request.url)
            logger.debug("[BOOK] __NEXT_DATA__ snippet: %.300s", next_data_raw)
        else:
            logger.warning("[BOOK] __NEXT_DATA__ NOT FOUND on %s — extraction will yield no fields", response.request.url)

        if not loader:
            loader = BookLoader(BookItem(), response=response)

        loader.add_value('url', response.request.url)

        # The new Goodreads page sends JSON in a script tag
        # that has these values

        loader.add_css('title', 'script#__NEXT_DATA__::text')
        loader.add_css('titleComplete', 'script#__NEXT_DATA__::text')
        loader.add_css('description', 'script#__NEXT_DATA__::text')
        loader.add_css('imageUrl', 'script#__NEXT_DATA__::text')
        loader.add_css('genres', 'script#__NEXT_DATA__::text')
        loader.add_css('asin', 'script#__NEXT_DATA__::text')
        loader.add_css('isbn', 'script#__NEXT_DATA__::text')
        loader.add_css('isbn13', 'script#__NEXT_DATA__::text')
        loader.add_css('publisher', 'script#__NEXT_DATA__::text')
        loader.add_css('series', 'script#__NEXT_DATA__::text')
        loader.add_css('author', 'script#__NEXT_DATA__::text')
        loader.add_css('publishDate', 'script#__NEXT_DATA__::text')
        loader.add_css('publishedYear', 'script#__NEXT_DATA__::text')

        loader.add_css('characters', 'script#__NEXT_DATA__::text')
        loader.add_css('places', 'script#__NEXT_DATA__::text')
        loader.add_css('ratingHistogram', 'script#__NEXT_DATA__::text')
        loader.add_css("ratingsCount", 'script#__NEXT_DATA__::text')
        loader.add_css("reviewsCount", 'script#__NEXT_DATA__::text')
        loader.add_css('numPages', 'script#__NEXT_DATA__::text')
        loader.add_css("format", 'script#__NEXT_DATA__::text')

        loader.add_css('language', 'script#__NEXT_DATA__::text')
        loader.add_css("awards", 'script#__NEXT_DATA__::text')

        item = loader.load_item()

        if DEBUG:
            all_fields = list(BookItem.fields.keys())
            found = [f for f in all_fields if f in item and item[f]]
            missing = [f for f in all_fields if f not in item or not item[f]]
            logger.info(
                "Book %s — found: [%s] | missing: [%s]",
                response.request.url,
                ", ".join(found),
                ", ".join(missing),
            )

        logger.info("[BOOK] Yielding BookItem for %s with fields: %s", response.request.url, list(item.keys()))
        yield item

        author_url = response.css('a.ContributorLink::attr(href)').extract_first()
        if author_url:
            logger.debug("[BOOK] Following author URL: %s", author_url)
            yield response.follow(author_url, callback=self.author_spider.parse)
        else:
            logger.warning("[BOOK] No author URL found on %s", response.request.url)
