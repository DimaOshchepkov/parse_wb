import json
import re
import sys
from pathlib import Path

import pandas as pd

from src.WbBasketResolver import WbBasketResolver


def money_from_wb(value):
    if value is None:
        return None
    try:
        return value / 100
    except Exception:
        return None


def build_product_url(nm_id: int) -> str:
    return f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx"


def build_seller_url(supplier_id):
    if not supplier_id:
        return ""
    return f"https://www.wildberries.ru/seller/{supplier_id}"


def extract_photo_count(record: dict) -> int:
    card_data = record.get("card_data") or {}
    search_item = record.get("search_item") or {}

    media = card_data.get("media") or {}
    photo_count = media.get("photo_count")

    if isinstance(photo_count, int) and photo_count > 0:
        return photo_count

    pics = search_item.get("pics")
    if isinstance(pics, int) and pics > 0:
        return pics

    return 0


def build_image_links(nm_id: int, photo_count: int) -> str:
    if not nm_id or photo_count <= 0:
        return ""

    vol = nm_id // 100000
    part = nm_id // 1000
    basket = WbBasketResolver.get_basket_number(vol)

    base = f"https://basket-{basket:02d}.wbcontent.net/vol{vol}/part{part}/{nm_id}/images/big"
    return ",".join(f"{base}/{i}.webp" for i in range(1, photo_count + 1))


def extract_price(search_item: dict):
    sizes = search_item.get("sizes") or []
    prices = []

    for size in sizes:
        price = (size or {}).get("price") or {}
        product_price = money_from_wb(price.get("product"))
        if product_price is not None:
            prices.append(product_price)

    if prices:
        return min(prices)

    return None


def extract_sizes(record: dict) -> str:
    card_data = record.get("card_data") or {}
    search_item = record.get("search_item") or {}

    sizes_table = card_data.get("sizes_table") or {}
    values = sizes_table.get("values") or []

    result = []
    seen = set()

    for item in values:
        tech_size = (item or {}).get("tech_size")
        if not tech_size:
            continue

        normalized = tech_size.split(" (")[0].strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)

    if result:
        return ",".join(result)

    for item in search_item.get("sizes") or []:
        name = (item or {}).get("name")
        if name and name not in seen:
            seen.add(name)
            result.append(name)

    return ",".join(result)


def extract_characteristics(record: dict) -> str:
    card_data = record.get("card_data") or {}
    grouped_options = card_data.get("grouped_options") or []
    options = card_data.get("options") or []

    if grouped_options:
        lines = []
        for group in grouped_options:
            group_name = (group or {}).get("group_name", "Без группы")
            lines.append(f"[{group_name}]")

            for opt in (group or {}).get("options") or []:
                name = (opt or {}).get("name", "")
                value = (opt or {}).get("value", "")
                lines.append(f"{name}: {value}")

            lines.append("")

        return "\n".join(lines).strip()

    return "\n".join(
        f'{(opt or {}).get("name", "")}: {(opt or {}).get("value", "")}'
        for opt in options
    ).strip()


def extract_seller_name(record: dict) -> str:
    search_item = record.get("search_item") or {}
    card_data = record.get("card_data") or {}

    return (
        search_item.get("supplier")
        or (card_data.get("selling") or {}).get("brand_name")
        or search_item.get("brand")
        or ""
    )


def extract_seller_id(record: dict):
    search_item = record.get("search_item") or {}
    card_data = record.get("card_data") or {}

    return (
        search_item.get("supplierId")
        or (card_data.get("selling") or {}).get("supplier_id")
    )


def record_to_row(record: dict) -> dict:
    search_item = record.get("search_item") or {}
    card_data = record.get("card_data") or {}

    nm_id = record.get("id") or card_data.get("nm_id") or search_item.get("id")
    seller_id = extract_seller_id(record)
    photo_count = extract_photo_count(record)

    row = {
        "Ссылка на товар": build_product_url(nm_id) if nm_id else "",
        "Артикул": nm_id,
        "Название": search_item.get("name") or card_data.get("imt_name") or "",
        "Цена": extract_price(search_item),
        "Описание": card_data.get("description", ""),
        "Ссылки на изображения через запятую": build_image_links(nm_id, photo_count),
        "Все характеристики с сохранением их структуры": extract_characteristics(record),
        "Название селлера": extract_seller_name(record),
        "Ссылка на селлера": build_seller_url(seller_id),
        "Размеры товара через запятую": extract_sizes(record),
        "Остатки по товару (число)": search_item.get("totalQuantity"),
        "Рейтинг": (
            search_item.get("reviewRating")
            or search_item.get("nmReviewRating")
            or search_item.get("rating")
        ),
        "Количество отзывов": (
            search_item.get("feedbacks")
            or search_item.get("nmFeedbacks")
        ),
    }

    return row


def convert_jsonl_to_xlsx(input_path: str, output_path: str):
    rows = []

    with open(input_path, "r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
                rows.append(record_to_row(record))
            except Exception as e:
                print(f"Ошибка в строке {line_number}: {e}")

    df = pd.DataFrame(rows)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="products")

        ws = writer.sheets["products"]

        widths = {
            "A": 28,
            "B": 14,
            "C": 40,
            "D": 12,
            "E": 70,
            "F": 100,
            "G": 60,
            "H": 30,
            "I": 30,
            "J": 25,
            "K": 22,
            "L": 12,
            "M": 18,
        }

        for col, width in widths.items():
            ws.column_dimensions[col].width = width

        from openpyxl.styles import Alignment

        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(vertical="top", wrap_text=True)

    print(f"Готово: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Использование: python script.py input.jsonl output.xlsx")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    convert_jsonl_to_xlsx(input_file, output_file)