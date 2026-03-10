from bisect import bisect_right
from math import inf


class WbBasketResolver:
    RANGES = (
        (0, 143), (144, 287), (288, 431), (432, 719), (720, 1007),
        (1008, 1061), (1062, 1115), (1116, 1169), (1170, 1313),
        (1314, 1601), (1602, 1655), (1656, 1919), (1920, 2045),
        (2046, 2189), (2190, 2405), (2406, 2621), (2622, 2837),
        (2838, 3053), (3054, 3269), (3270, 3485), (3486, 3701),
        (3702, 3917), (3918, 4133), (4134, 4349), (4350, 4565),
        (4566, 4781), (4782, 999999),
    )

    @classmethod
    def get_basket_number(cls, vol: int) -> int:
        idx = bisect_right(cls.RANGES, (vol, inf)) - 1

        if idx >= 0:
            _, end = cls.RANGES[idx]
            if vol <= end:
                return idx + 1

        raise ValueError(f"Unknown basket for volume: {vol}")