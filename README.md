# Автообновляемый план пошива (Ozon + WB + 1C)

Проект собирает данные из Ozon, WB и 1C, нормализует их в единый SKU-слой, рассчитывает приоритет пошива и генерирует Excel-отчёт `output/production_plan.xlsx`.

## Установка

Базовый запуск не требует внешних пакетов (адаптировано под офлайн-среды).

```bash
python -m venv .venv
source .venv/bin/activate
python -m pytest -q
```

Если у вас есть интернет и нужны full-зависимости, используйте `requirements.txt`.

## Настройка `.env`

1. Скопируйте `.env.example` в `.env`.
2. Заполните токены и URL API.
3. Если креды отсутствуют — работает demo-режим с mock-данными.

## Команды CLI

```bash
python main.py sync
python main.py validate
python main.py export
python main.py run
python main.py backfill --days 90
```

- `sync` — загрузка/обновление данных в кэш.
- `validate` — запуск проверок и печать результатов.
- `export` — сборка `output/production_plan.xlsx`.
- `run` — единичный запуск + планировщик.
- `backfill` — запись исторических mock-снэпшотов в sqlite.

## Кэш и устойчивость

- Хранилище: sqlite (`CACHE_DB_PATH`).
- Сохраняются RAW snapshots и результаты проверок.
- При недоступности источника используются последние успешные данные (stale-флаг уходит в лист `Настройки`).

## Изменение бизнес-логики без кода

Все пороги и правила находятся в `config/business_rules.yaml`.
Склады — `config/warehouses.yaml`.
Маппинг/alias SKU — `config/sku_aliases.yaml` и `config/field_mapping_1c.yaml`.

## Листы итогового файла

- `Приоритетность пошива`
- `Сводка`
- `Проверки`
- `Неразобранные SKU`
- `RAW_1C`
- `RAW_WB`
- `RAW_Ozon`
- `Настройки`
- `Лог`


> `output/production_plan.xlsx` генерируется командой `python main.py export` и не коммитится в репозиторий.
