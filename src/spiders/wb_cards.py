from bisect import bisect_right

import scrapy
from scrapy.http import Response, TextResponse
from twisted.python.failure import Failure
from math import inf

from src.WbBasketResolver import WbBasketResolver


class WbCardSpider(scrapy.Spider):
    name = "wb_cards"

    custom_settings = {
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_DELAY": 0.1,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.2,
        "AUTOTHROTTLE_MAX_DELAY": 5,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4.0,
        "RETRY_TIMES": 1,
        "DOWNLOAD_TIMEOUT": 10,
        "HTTPERROR_ALLOWED_CODES": [404],
        "LOG_LEVEL": "ERROR",
    }

    async def start(self):
        # extension сам будет подкидывать реквесты
        return
        yield

    def parse_card(self, response: Response):
        source_item = response.meta["source_item"]
        nm_id = response.meta["nm_id"]

        try:
            card_data = response.json() # type: ignore[attr-defined]
        except Exception:
            card_data = {
                "_raw_text": response.text,
            }

        yield {
            "id": nm_id,
            "card_url": response.url,
            "search_item": source_item,
            "card_data": card_data,
            "status": response.status,
        }

    def errback_card(self, failure: Failure):
        request = failure.request  # type: ignore[attr-defined]
        source_item = request.meta["source_item"]
        nm_id = request.meta["nm_id"]
        card_url = request.meta["card_url"]

        self.logger.error("Ошибка при загрузке %s: %s", card_url, failure.value)

        yield {
            "id": nm_id,
            "card_url": card_url,
            "search_item": source_item,
            "error": str(failure.value),
        }

    def build_card_url(self, nm_id: int) -> str:
        vol = nm_id // 100000
        part = nm_id // 1000
        basket_host = self.get_basket_host(vol)

        return (
            f"https://{basket_host}/"
            f"vol{vol}/part{part}/{nm_id}/info/ru/card.json"
        )

    RANGES = (
        (0, 143), (144, 287), (288, 431), (432, 719), (720, 1007),
        (1008, 1061), (1062, 1115), (1116, 1169), (1170, 1313),
        (1314, 1601), (1602, 1655), (1656, 1919), (1920, 2045),
        (2046, 2189), (2190, 2405), (2406, 2621), (2622, 2837),
        (2838, 3053), (3054, 3269), (3270, 3485), (3486, 3701),
        (3702, 3917), (3918, 4133), (4134, 4349), (4350, 4565),
        (4566, 4781), (4782, 999999),
    )

    def get_basket_host(self, vol: int) -> str:
        basket = WbBasketResolver.get_basket_number(vol)
        return f"basket-{basket:02d}.wbbasket.ru"