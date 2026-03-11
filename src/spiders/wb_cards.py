import json

import scrapy
from scrapy.http import Response
from scrapy_redis.spiders import RedisSpider
from twisted.python.failure import Failure

from scrapy import signals
from scrapy.exceptions import CloseSpider, DontCloseSpider

from src.WbBasketResolver import WbBasketResolver
from src.redis_client import get_redis


class WbCardSpider(RedisSpider):
    name = "wb_cards"
    redis_key = "wb:card_tasks"
    redis_done_key = "wb:done"
    in_progress_key = "wb:cards:in_progress"

    custom_settings = {
        "CONCURRENT_REQUESTS": 128,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 64,

        "DOWNLOAD_DELAY": 0,
        "AUTOTHROTTLE_ENABLED": False,

        "RETRY_TIMES": 0,
        "DOWNLOAD_TIMEOUT": 4,

        "LOG_LEVEL": "INFO",
        "COOKIES_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "REDIRECT_ENABLED": False,

        "REACTOR_THREADPOOL_MAXSIZE": 32,

        "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
        "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
        "SCHEDULER_PERSIST": True,
        "REDIS_URL": "redis://localhost:6379",
    }

    def start_requests(self):
        self.redis = get_redis(self.settings)
        self.logger.warning(
            "Spider запущен. Ожидание задач из Redis: key=%s",
            self.redis_key,
        )
        yield from super().start_requests()



    def make_request_from_data(self, data: bytes | str):
        try:
            if isinstance(data, bytes):
                data = data.decode("utf-8")

            task_data = json.loads(data)
        except Exception:
            self.logger.exception("Не удалось распарсить задачу из Redis: %r", data)
            return None

        nm_id = task_data["nm_id"]
        source_item = task_data.get("source_item")
        url = self.build_card_url(nm_id)

        self.redis.incr(self.in_progress_key)

        return scrapy.Request(
            url=url,
            callback=self.parse_card,
            errback=self.errback_card,
            dont_filter=True,
            meta={
                "nm_id": nm_id,
                "source_item": source_item,
                "card_url": url,
            },
        )
        
    def finish_task(self):
        try:
            self.redis.decr(self.in_progress_key)
        except Exception:
            self.logger.exception("Не удалось уменьшить in_progress")



    def parse_card(self, response: Response):
        nm_id = response.meta["nm_id"]
        source_item = response.meta.get("source_item")

        try:
            if response.status == 404:
                self.logger.warning("404 card.json → fallback: nm_id=%s", nm_id)
                result = {
                    "id": nm_id,
                    "card_url": response.url,
                    "card_data": self.build_fallback(source_item),
                    "search_item": source_item,
                    "status": 404,
                    "is_fallback": True,
                }
            else:
                try:
                    card_data = response.json()  # type: ignore[attr-defined]
                    result = {
                        "id": nm_id,
                        "card_url": response.url,
                        "card_data": card_data,
                        "search_item": source_item,
                        "status": response.status,
                        "is_fallback": False,
                    }
                except Exception:
                    self.logger.warning("JSON parse error → fallback: %s", response.url)
                    result = {
                        "id": nm_id,
                        "card_url": response.url,
                        "card_data": self.build_fallback(source_item),
                        "search_item": source_item,
                        "status": response.status,
                        "is_fallback": True,
                    }

            yield result
        finally:
            self.finish_task()

    def errback_card(self, failure: Failure):
        request = failure.request  # type: ignore[attr-defined]

        nm_id = request.meta["nm_id"]
        source_item = request.meta.get("source_item")
        url = request.meta["card_url"]

        try:
            self.logger.warning(
                "Request error → fallback: nm_id=%s error=%s",
                nm_id,
                failure.value,
            )

            yield {
                "id": nm_id,
                "card_url": url,
                "card_data": self.build_fallback(source_item),
                "search_item": source_item,
                "error": str(failure.value),
                "is_fallback": True,
            }
        finally:
            self.finish_task()

    # ----------------------------
    # Fallback builder
    # ----------------------------

    def build_fallback(self, source_item: dict | None) -> dict | None:
        if not source_item:
            return None

        return {
            "_fallback": True,
            "id": source_item.get("id"),
            "name": source_item.get("name"),
            "brand": source_item.get("brand"),
            "brandId": source_item.get("brandId"),
            "supplier": source_item.get("supplier"),
            "supplierId": source_item.get("supplierId"),
            "rating": source_item.get("rating"),
            "feedbacks": source_item.get("feedbacks"),
            "totalQuantity": source_item.get("totalQuantity"),
            "sizes": source_item.get("sizes"),
            "colors": source_item.get("colors"),
            "pics": source_item.get("pics"),
        }

    # ----------------------------
    # URL builder
    # ----------------------------

    def build_card_url(self, nm_id: int) -> str:
        vol = nm_id // 100000
        part = nm_id // 1000

        basket = WbBasketResolver.get_basket_number(vol)

        return (
            f"https://basket-{basket:02d}.wbbasket.ru/"
            f"vol{vol}/part{part}/{nm_id}/info/ru/card.json"
        )
        
    def idle(self):
        queue_size = self.redis.llen(self.redis_key)
        done = self.redis.get(self.redis_done_key)

        self.logger.warning(
            "idle check: queue_size=%s done=%r",
            queue_size,
            done,
        )

        if queue_size == 0 and done == "1":
            self.logger.warning("Очередь пуста и producer завершён. Закрываю паука.")
            self.crawler.engine.close_spider(self, reason="finished")
            
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.redis = get_redis(crawler.settings)
        crawler.signals.connect(spider.idle, signal=signals.spider_idle)
        return spider
