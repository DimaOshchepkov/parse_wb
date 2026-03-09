import json
import logging

import scrapy
from redis import Redis
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from twisted.internet import defer, task
from twisted.internet.threads import deferToThread

from src.redis_client import get_redis
from src.spiders.wb_cards import WbCardSpider

logger = logging.getLogger(__name__)


class RedisConsumerExtension:
    def __init__(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        self.redis: Redis | None = None
        self.looping_call: task.LoopingCall | None = None
        self.stopping = False

        self.queue_key = self.settings["REDIS_QUEUE_KEY"]
        self.done_key = self.settings["REDIS_DONE_KEY"]
        self.batch_size = self.settings.getint("REDIS_CONSUMER_BATCH_SIZE", 10)
        self.poll_interval = self.settings.getfloat("REDIS_CONSUMER_POLL_INTERVAL", 1.0)

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler)

        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)

        logger.info("RedisConsumerExtension подключён")
        return ext

    async def spider_opened(self, spider: WbCardSpider):
        if spider.name != "wb_cards":
            return

        self.redis = get_redis(self.settings)

        self.looping_call = task.LoopingCall(self._tick, spider)
        d = self.looping_call.start(self.poll_interval, now=True)
        d.addErrback(self._looping_call_errback, spider)

        spider.logger.info(
            "RedisConsumerExtension: polling запущен, interval=%s, batch_size=%s",
            self.poll_interval,
            self.batch_size,
        )

    def spider_idle(self, spider: WbCardSpider):
        if spider.name != "wb_cards":
            return

        if not self.stopping:
            raise DontCloseSpider

    async def spider_closed(self, spider: WbCardSpider, reason: str):
        if spider.name != "wb_cards":
            return

        if self.looping_call and self.looping_call.running:
            self.looping_call.stop()

        if self.redis is not None:
            self.redis.close()

    def _looping_call_errback(self, failure, spider: WbCardSpider):
        spider.logger.error(
            "RedisConsumerExtension: LoopingCall crashed:\n%s",
            failure.getTraceback(),
        )

    def _tick(self, spider: WbCardSpider):
        return defer.ensureDeferred(self._tick_async(spider))

    def _pop_batch(self) -> list[str]:
        assert self.redis is not None

        items = self.redis.rpop(self.queue_key, self.batch_size)
        if not items:
            return []

        if isinstance(items, str):
            return [items]

        return items

    def _get_done_and_queue_size(self) -> tuple[str | None, int]:
        assert self.redis is not None

        pipe = self.redis.pipeline()
        pipe.get(self.done_key)
        pipe.llen(self.queue_key)
        done, queue_size = pipe.execute()
        return done, queue_size

    async def _tick_async(self, spider: WbCardSpider):
        try:
            if self.redis is None or self.stopping:
                return

            raw_tasks = await deferToThread(self._pop_batch)

            if raw_tasks:
                for raw_task in raw_tasks:
                    task_data = json.loads(raw_task)

                    nm_id = task_data["nm_id"]
                    source_item = task_data.get("source_item", {})
                    url = spider.build_card_url(nm_id)

                    request = scrapy.Request(
                        url=url,
                        callback=spider.parse_card,
                        errback=spider.errback_card,
                        dont_filter=True,
                        meta={
                            "source_item": source_item,
                            "nm_id": nm_id,
                            "card_url": url,
                        },
                    )

                    self.crawler.engine.crawl(request)

                spider.logger.info(
                    "RedisConsumerExtension: добавлено задач из Redis: %s",
                    len(raw_tasks),
                )
                return

            done, queue_size = await deferToThread(self._get_done_and_queue_size)

            if done == "1" and queue_size == 0:
                spider.logger.info(
                    "RedisConsumerExtension: очередь пуста и producer завершён. Закрываю паука."
                )
                self.stopping = True
                self.crawler.engine.close_spider(
                    spider,
                    reason="redis_queue_drained",
                )

        except Exception:
            spider.logger.exception("RedisConsumerExtension: ошибка в _tick_async")
            raise