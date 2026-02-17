# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import os

from scrapy.exporters import JsonLinesItemExporter
from scrapy import signals

from .items import SearchResultItem


class CsvSearchResultPipeline:
    """Writes SearchResultItem rows to a per-worker CSV file with checkpointing."""

    CSV_COLUMNS = ["title", "author", "genres", "description", "goodreads_url", "status"]

    @classmethod
    def from_crawler(cls, crawler):
        output_file = crawler.settings.get("SEARCH_OUTPUT_FILE", "search_results.csv")
        checkpoint_file = crawler.settings.get("SEARCH_CHECKPOINT_FILE", "checkpoint.txt")
        return cls(output_file, checkpoint_file)

    def __init__(self, output_file, checkpoint_file):
        self.output_file = output_file
        self.checkpoint_file = checkpoint_file
        self.items_since_checkpoint = 0
        self.completed_indices = []

    def open_spider(self, spider):
        file_exists = os.path.exists(self.output_file)
        self.file = open(self.output_file, "a", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.file, fieldnames=self.CSV_COLUMNS)
        if not file_exists:
            self.writer.writeheader()

    def close_spider(self, spider):
        self._save_checkpoint()
        self.file.close()

    def process_item(self, item, spider):
        if not isinstance(item, SearchResultItem):
            return item

        row = {col: item.get(col, "") for col in self.CSV_COLUMNS}
        self.writer.writerow(row)
        self.file.flush()

        self.completed_indices.append(item["row_idx"])
        self.items_since_checkpoint += 1
        if self.items_since_checkpoint >= 1000:
            self._save_checkpoint()
            self.items_since_checkpoint = 0

        return item

    def _save_checkpoint(self):
        if not self.completed_indices:
            return
        with open(self.checkpoint_file, "a") as f:
            for idx in self.completed_indices:
                f.write(f"{idx}\n")
        self.completed_indices = []


class JsonLineItemSegregator(object):
    @classmethod
    def from_crawler(cls, crawler):
        output_file_suffix = crawler.settings.get("OUTPUT_FILE_SUFFIX", default="")
        return cls(crawler, output_file_suffix)

    def __init__(self, crawler, output_file_suffix):
        self.types = {"book", "author"}
        self.output_file_suffix = output_file_suffix
        self.files = set()
        crawler.signals.connect(self.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_opened(self, spider):
        self.files = {name: open(name + "_" + self.output_file_suffix + '.jl', 'a+b') for name in self.types}
        self.exporters = {name: JsonLinesItemExporter(self.files[name]) for name in self.types}

        for e in self.exporters.values():
            e.start_exporting()

    def spider_closed(self, spider):
        for e in self.exporters.values():
            e.finish_exporting()

        for f in self.files.values():
            f.close()

    def process_item(self, item, spider):
        item_type = type(item).__name__.replace("Item", "").lower()
        if item_type in self.types:
            self.exporters[item_type].export_item(item)
        return item
