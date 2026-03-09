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

        spider.logger.info("RedisConsumerExtension: spider_opened")

        self.redis = get_redis(self.settings)

        spider.logger.info(
            "RedisConsumerExtension: подключение к Redis, queue=%s",
            self.queue_key,
        )

        self.looping_call = task.LoopingCall(self._tick, spider)
        d = self.looping_call.start(self.poll_interval, now=True)
        d.addErrback(self._looping_call_errback, spider)

        spider.logger.info(
            "RedisConsumerExtension: polling запущен, interval=%s",
            self.poll_interval,
        )

    def spider_idle(self, spider: WbCardSpider):
        if spider.name != "wb_cards":
            return

        spider.logger.debug("RedisConsumerExtension: spider_idle")

        if not self.stopping:
            raise DontCloseSpider

    async def spider_closed(self, spider: WbCardSpider, reason: str):
        if spider.name != "wb_cards":
            return

        spider.logger.info(
            "RedisConsumerExtension: spider_closed, reason=%s",
            reason,
        )

        if self.looping_call and self.looping_call.running:
            spider.logger.info("RedisConsumerExtension: stopping LoopingCall")
            self.looping_call.stop()

        if self.redis is not None:
            spider.logger.info("RedisConsumerExtension: closing Redis connection")
            self.redis.close()

    def _looping_call_errback(self, failure, spider: WbCardSpider):
        spider.logger.error(
            "RedisConsumerExtension: LoopingCall crashed:\n%s",
            failure.getTraceback(),
        )

    def _tick(self, spider: WbCardSpider):
        spider.logger.debug("RedisConsumerExtension: tick")
        return defer.ensureDeferred(self._tick_async(spider))

    async def _tick_async(self, spider: WbCardSpider):
        try:
            if self.redis is None or self.stopping:
                spider.logger.debug(
                    "RedisConsumerExtension: redis=None или stopping=True"
                )
                return

            scheduled = 0

            spider.logger.debug("RedisConsumerExtension: читаю Redis очередь")

            for _ in range(self.batch_size):
                result = await deferToThread(self.redis.rpop, self.queue_key)

                if not result:
                    spider.logger.debug("RedisConsumerExtension: очередь пуста")
                    break

                task_data = json.loads(result)

                nm_id = task_data["nm_id"]
                source_item = task_data.get("source_item", {})

                spider.logger.debug(
                    "RedisConsumerExtension: задача nm_id=%s",
                    nm_id,
                )

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

                spider.logger.debug(
                    "RedisConsumerExtension: добавляю request %s",
                    url,
                )

                self.crawler.engine.crawl(request)
                scheduled += 1

            if scheduled:
                spider.logger.info(
                    "RedisConsumerExtension: добавлено задач из Redis: %s",
                    scheduled,
                )
                return

            done = await deferToThread(self.redis.get, self.done_key)
            queue_size = await deferToThread(self.redis.llen, self.queue_key)

            spider.logger.debug(
                "RedisConsumerExtension: done=%s queue_size=%s",
                done,
                queue_size,
            )

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