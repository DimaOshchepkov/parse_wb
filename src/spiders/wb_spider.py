import json
from urllib.parse import urlencode

import scrapy
from scrapy.http import Response

from src.redis_client import get_redis


class WbSpider(scrapy.Spider):
    name = "wb"
    base_url = "https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search"

    custom_settings = {
        "LOG_LEVEL": "INFO",
    }
    
    def __init__(self, query="пальто из натуральной шерсти", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_params = {
            "ab_testing": "false",
            "appType": 1,
            "curr": "rub",
            "dest": -3390592,
            "hide_vflags": 4294967296,
            "lang": "ru",
            "query": query,
            "resultset": "catalog",
            "sort": "popular",
            "spp": 30,
            "suppressSpellcheck": "false",
        }

    async def start(self):
        self.redis = get_redis(self.settings)

        self.redis_queue_key = self.settings.get("REDIS_QUEUE_KEY")
        self.redis_done_key = self.settings.get("REDIS_DONE_KEY")
        self.redis_seen_key = self.settings.get("REDIS_SEEN_KEY")

        self.redis.delete(self.redis_done_key)

        yield self.make_request(1)

    def make_request(self, page: int) -> scrapy.Request:
        params = self.api_params.copy()
        params["page"] = page
        url = self.base_url + "?" + urlencode(params)

        return scrapy.Request(
            url=url,
            callback=self.parse,
            meta={"page": page},
        )

    async def parse(self, response: Response):
        data = response.json()
        products = data.get("products", [])

        page = response.meta["page"]
        self.logger.info("page %s products %s", page, len(products))

        if products:
            tasks = []

            for item in products:
                nm_id = item.get("id")
                if not nm_id:
                    continue

                task = {
                    "nm_id": nm_id,
                    "source_item": item,
                }

                tasks.append(json.dumps(task, ensure_ascii=False))

            if tasks:
                self.redis.lpush(self.redis_queue_key, *tasks)

            yield self.make_request(page + 1)

        else:
            self.logger.info("Поиск завершён, ставлю флаг done")
            self.redis.set(self.redis_done_key, "1")

    async def closed(self, reason):
        if hasattr(self, "redis"):
            self.redis.set(self.redis_done_key, "1")
            self.redis.close()