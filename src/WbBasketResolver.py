from bisect import bisect_right


class WbBasketResolver:
    RANGES = (
        (0, 143),
        (144, 287),
        (288, 431),
        (432, 719),
        (720, 1007),
        (1008, 1061),
        (1062, 1115),
        (1116, 1169),
        (1170, 1313),
        (1314, 1601),
        (1602, 1655),
        (1656, 1919),
        (1920, 2045),
        (2046, 2189),
        (2190, 2405),
        (2406, 2621),
        (2622, 2837),
        (2838, 3053),
        (3054, 3269),
        (3270, 3485),
        (3486, 3701),
        (3702, 3917),
        (3918, 4133),
        (4134, 4349),
        (4350, 4565),
        (4566, 4877),
        (4878, 5189),
        (5190, 5501),
        (5502, 5813),
        (5814, 6125),
        (6126, 6437),
        (6438, 6749),
        (6750, 7061),
        (7062, 7373),
        (7374, 7685),
        (7686, 7997),
        (7998, 8309),
        (8310, 8741),
        (8742, 9173),
        (9174, 9605),
        (9606, 999999),
    )  # Нашел через devtools, функция volHostV2(

    @classmethod
    def get_basket_number(cls, vol: int) -> int:
        idx = bisect_right(cls.RANGES, vol, key=lambda r: r[0]) - 1

        if idx >= 0:
            start, end = cls.RANGES[idx]
            if start <= vol <= end:
                return idx + 1

        raise ValueError(f"Unknown basket for volume: {vol}")

    @classmethod
    def get_host(cls, nm_id: int) -> str:
        vol = nm_id // 100000
        basket = cls.get_basket_number(vol)
        return f"basket-{basket:02d}.wbbasket.ru"

    @classmethod
    def get_base_url(cls, nm_id: int) -> str:
        vol = nm_id // 100000
        part = nm_id // 1000
        host = cls.get_host(nm_id)
        return f"https://{host}/vol{vol}/part{part}/{nm_id}"
    
if __name__ == "__main__":
    resolver = WbBasketResolver()
    nm_id = 694038423
    print(resolver.get_base_url(nm_id))