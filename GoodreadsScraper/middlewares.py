# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals


class GoodreadsscraperSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

import os
import re
import hashlib
from datetime import datetime


class ResponseDebugMiddleware:
    """Logs request/response details and saves HTML to disk.

    Only active when DEBUG_ENABLED=True in Scrapy settings.
    In quiet mode the middleware is still registered but becomes a pass-through.
    """

    def __init__(self, enabled=True, dump_dir="debug_responses", max_snippet=500):
        self.enabled = enabled
        self.dump_dir = dump_dir
        self.max_snippet = max_snippet
        if enabled:
            os.makedirs(self.dump_dir, exist_ok=True)

    @classmethod
    def from_crawler(cls, crawler):
        enabled = crawler.settings.getbool("DEBUG_ENABLED", True)
        dump_dir = crawler.settings.get("DEBUG_DUMP_DIR", "debug_responses")
        max_snippet = crawler.settings.getint("DEBUG_MAX_SNIPPET", 500)
        return cls(enabled=enabled, dump_dir=dump_dir, max_snippet=max_snippet)

    def process_request(self, request, spider):
        if self.enabled:
            spider.logger.debug(f"[REQ] {request.method} {request.url} meta={dict(request.meta)}")
        return None

    def process_response(self, request, response, spider):
        if not self.enabled:
            return response

        ct = response.headers.get(b"Content-Type", b"").decode(errors="ignore")
        body_len = len(response.body or b"")
        spider.logger.debug(
            f"[RESP] {response.status} {request.url} "
            f"len={body_len} content-type={ct}"
        )

        try:
            txt = response.text
        except Exception:
            txt = (response.body or b"").decode("utf-8", errors="ignore")

        snippet = re.sub(r"\s+", " ", txt)[: self.max_snippet]
        spider.logger.debug(f"[RESP-SNIP] {response.status} {request.url} :: {snippet}")

        # Save every HTML response to disk for inspection
        if "text/html" in ct:
            h = hashlib.md5(request.url.encode("utf-8")).hexdigest()[:10]
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            fname = os.path.join(self.dump_dir, f"{ts}_{response.status}_{h}.html")
            with open(fname, "wb") as f:
                f.write(response.body or b"")
            spider.logger.warning(f"[DUMP] Saved response to {fname}")

        return response
