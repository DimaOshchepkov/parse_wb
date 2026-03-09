# parse_wb

Проект парсит Wildberries в два этапа:
1. `wb` собирает товары из поисковой выдачи и складывает задачи в Redis.
2. `wb_cards` читает задачи из Redis и загружает карточки товаров (`card.json`).

`wb_session.json` генерируется через Playwright с подключением к браузеру по CDP debug (`http://localhost:9222`).

## Подготовка
1. Создайте виртуальное окружение:

```powershell
python -m venv .venv
```

2. Активируйте окружение:

```powershell
.\.venv\Scripts\Activate.ps1
```

3. Установите зависимости:

```powershell
pip install -r requirements.txt
```

4. Установите браузер Playwright:

```bash
python -m playwright install chromium
```

5. Скопируйте переменные окружения:

```bash
cp .env.example .env
```

## Как запустить
Запуск полного пайплайна одной командой:

```powershell
python run_pipeline.py
```

Скрипт `run_pipeline.py` последовательно делает:
1. Получает cookies/headers и сохраняет `wb_session.json` (через Chrome CDP debug).
2. Поднимает Redis через Docker Compose (`compose.yml`).
3. Запускает `scrapy crawl wb`.
4. Запускает `scrapy crawl wb_cards -O cards.jsonl`.

Результат сохраняется в `cards.jsonl`.
