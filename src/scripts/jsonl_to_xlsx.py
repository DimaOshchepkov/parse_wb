import json
import sys

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


def is_fallback_card(card_data: dict) -> bool:
    return bool(card_data and card_data.get("_fallback"))


def get_card_data(record: dict) -> dict:
    return record.get("card_data") or {}


def get_search_item(record: dict) -> dict:
    return record.get("search_item") or {}


def first_non_empty(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def extract_nm_id(record: dict):
    card_data = get_card_data(record)
    search_item = get_search_item(record)

    return (
        record.get("id")
        or card_data.get("nm_id")
        or card_data.get("id")
        or search_item.get("id")
    )


def extract_photo_count(record: dict) -> int:
    card_data = get_card_data(record)
    search_item = get_search_item(record)

    media = card_data.get("media") or {}
    photo_count = media.get("photo_count")
    if isinstance(photo_count, int) and photo_count > 0:
        return photo_count

    pics = card_data.get("pics")
    if isinstance(pics, int) and pics > 0:
        return pics

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


def extract_price_from_sizes(sizes) -> float | None:
    prices = []

    for size in sizes or []:
        price = (size or {}).get("price") or {}
        product_price = money_from_wb(price.get("product"))
        if product_price is not None:
            prices.append(product_price)

    if prices:
        return min(prices)

    return None


def extract_price(record: dict):
    card_data = get_card_data(record)
    search_item = get_search_item(record)

    price = extract_price_from_sizes(card_data.get("sizes"))
    if price is not None:
        return price

    price = extract_price_from_sizes(search_item.get("sizes"))
    if price is not None:
        return price

    return None


def extract_sizes(record: dict) -> str:
    card_data = get_card_data(record)
    search_item = get_search_item(record)

    result = []
    seen = set()

    sizes_table = card_data.get("sizes_table") or {}
    values = sizes_table.get("values") or []

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

    for item in card_data.get("sizes") or []:
        name = (item or {}).get("name") or (item or {}).get("origName")
        if name and name not in seen:
            seen.add(name)
            result.append(name)

    if result:
        return ",".join(result)

    for item in search_item.get("sizes") or []:
        name = (item or {}).get("name") or (item or {}).get("origName")
        if name and name not in seen:
            seen.add(name)
            result.append(name)

    return ",".join(result)


def extract_characteristics(record: dict) -> str:
    card_data = get_card_data(record)
    search_item = get_search_item(record)

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
                if name or value:
                    lines.append(f"{name}: {value}")

            lines.append("")

        return "\n".join(lines).strip()

    if options:
        return "\n".join(
            f'{(opt or {}).get("name", "")}: {(opt or {}).get("value", "")}'
            for opt in options
            if (opt or {}).get("name") or (opt or {}).get("value")
        ).strip()

    fallback_lines = []

    brand = first_non_empty(card_data.get("brand"), search_item.get("brand"))
    if brand:
        fallback_lines.append(f"Бренд: {brand}")

    supplier = first_non_empty(card_data.get("supplier"), search_item.get("supplier"))
    if supplier:
        fallback_lines.append(f"Продавец: {supplier}")

    colors = card_data.get("colors") or search_item.get("colors") or []
    color_names = [c.get("name") for c in colors if isinstance(c, dict) and c.get("name")]
    if color_names:
        fallback_lines.append(f"Цвет: {', '.join(color_names)}")

    sizes = extract_sizes(record)
    if sizes:
        fallback_lines.append(f"Размеры: {sizes}")

    total_quantity = first_non_empty(card_data.get("totalQuantity"), search_item.get("totalQuantity"))
    if total_quantity is not None:
        fallback_lines.append(f"Остаток: {total_quantity}")

    return "\n".join(fallback_lines).strip()


def extract_seller_name(record: dict) -> str:
    search_item = get_search_item(record)
    card_data = get_card_data(record)

    return (
        first_non_empty(
            search_item.get("supplier"),
            card_data.get("supplier"),
            (card_data.get("selling") or {}).get("brand_name"),
            search_item.get("brand"),
            card_data.get("brand"),
        )
        or ""
    )


def extract_seller_id(record: dict):
    search_item = get_search_item(record)
    card_data = get_card_data(record)

    return first_non_empty(
        search_item.get("supplierId"),
        card_data.get("supplierId"),
        (card_data.get("selling") or {}).get("supplier_id"),
    )


def extract_name(record: dict) -> str:
    search_item = get_search_item(record)
    card_data = get_card_data(record)

    return (
        first_non_empty(
            search_item.get("name"),
            card_data.get("name"),
            card_data.get("imt_name"),
        )
        or ""
    )


def extract_description(record: dict) -> str:
    card_data = get_card_data(record)

    description = card_data.get("description")
    if isinstance(description, str) and description.strip():
        return description

    if is_fallback_card(card_data):
        return ""

    return ""


def extract_stock(record: dict):
    card_data = get_card_data(record)
    search_item = get_search_item(record)

    return first_non_empty(
        card_data.get("totalQuantity"),
        search_item.get("totalQuantity"),
    )


def extract_rating(record: dict):
    card_data = get_card_data(record)
    search_item = get_search_item(record)

    return first_non_empty(
        search_item.get("reviewRating"),
        search_item.get("nmReviewRating"),
        search_item.get("rating"),
        card_data.get("reviewRating"),
        card_data.get("rating"),
    )


def extract_feedbacks(record: dict):
    card_data = get_card_data(record)
    search_item = get_search_item(record)

    return first_non_empty(
        search_item.get("feedbacks"),
        search_item.get("nmFeedbacks"),
        card_data.get("feedbacks"),
    )


def record_to_row(record: dict) -> dict:
    nm_id = extract_nm_id(record)
    seller_id = extract_seller_id(record)
    photo_count = extract_photo_count(record)

    row = {
        "Ссылка на товар": build_product_url(nm_id) if nm_id else "",
        "Артикул": nm_id,
        "Название": extract_name(record),
        "Цена": extract_price(record),
        "Описание": extract_description(record),
        "Ссылки на изображения через запятую": build_image_links(nm_id, photo_count),
        "Все характеристики с сохранением их структуры": extract_characteristics(record),
        "Название селлера": extract_seller_name(record),
        "Ссылка на селлера": build_seller_url(seller_id),
        "Размеры товара через запятую": extract_sizes(record),
        "Остатки по товару (число)": extract_stock(record),
        "Рейтинг": extract_rating(record),
        "Количество отзывов": extract_feedbacks(record),
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