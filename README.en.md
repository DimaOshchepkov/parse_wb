# Wildberries Product Parser

Parser for **Wildberries** products that collects search results and product cards, then generates an **Excel catalog**.

Russian version: [README.md](./README.md)

The project consists of two crawlers:

1. **Producer** collects products from WB search results.
2. **Consumer** loads product cards through `card.json`.

The final result is merged and exported to **Excel**.

---

## Output

After the pipeline finishes, the file `products.xlsx` is created.

It contains:

- product link
- article number
- title
- price
- description
- image links
- specifications
- seller name
- seller link
- sizes
- stock data
- rating
- review count

---

## Requirements

Before running, install the following:

### 1. Google Chrome

Used to obtain **Wildberries cookies** through Playwright.

Without it, the WB API may return errors or block requests.

### 2. Docker Desktop

Used to run **Redis** as a task queue.

### 3. Python 3.10+

---

## Quick Start

Run from the project root:

```bash
python manage.py quickstart
```

This command will:

- create the `.venv` virtual environment
- install Python dependencies
- install Playwright and Chromium
- start Redis through Docker
- refresh WB cookies
- run the crawlers
- convert the result to Excel

After completion, the file `products.xlsx` will appear.

---

## Project Architecture

The project is built around a Redis task queue.

```text
WB Search Spider
        |
        v
Redis Queue (nm_id)
        |
        v
WB Card Spider
        |
        v
JSONL
        |
        v
Excel
```

### Producer spider

Gets products from Wildberries search results and pushes tasks into Redis.

Each task contains:

```text
nm_id
source_item
```

### Consumer spider

Takes tasks from Redis and loads:

```text
https://basket-XX.wbbasket.ru/.../card.json
```

Results are saved to JSONL.

### Converter

The script `src/scripts/jsonl_to_xlsx.py` converts JSONL into Excel.

---

## Project Features

### 1. Direct work with JSON API

The parser does not use HTML pages.

All data is taken from Wildberries internal APIs:

```text
search API
card.json
```

This makes parsing:

- faster
- more stable
- less sensitive to layout changes

### 2. Basket server resolution

Wildberries stores products on different servers. The server is determined by product ID.

The project implements a fast algorithm:

```text
nm_id -> vol -> basket
```

The correct URL is resolved through a range table without brute-forcing servers.

### 3. Redis task queue

The following scheme is used:

```text
Producer -> Redis -> Consumer
```

This allows you to:

- scale parsing
- separate search collection and card collection
- run multiple workers

### 4. Fallback data

If `card.json` returns `404`, data is taken from the search result (`source_item`).

This helps avoid losing products.

---

## Tech Stack

### Language

- Python 3

### Parsing

- Scrapy
- `scrapy-redis`

### Browser automation

- Playwright

Used to obtain cookies.

### Task queue

- Redis

### Containerization

- Docker
- Docker Compose

### Data processing

- pandas
- openpyxl

### Export

- XLSX
