# lokilog

Lightweight, non-blocking Python logging handler for **Grafana Loki**.

## Features
- **Non-blocking**: Uses a background thread to ensure logging never stalls your application
- **Binary Format**: Supports Protobuf + Snappy compression
- **Smart Labels**: Promotes specific fields from `extra` to Loki labels to avoid high cardinality issues.

## Dependencies
- `requests`
- `cramjam` (for Snappy compression)

## Quick Start

### Basic Usage
```python
from lokilog import make_loki_handler
import logging

handler, listener = make_loki_handler(
    url="http://localhost:3100",
    labels={"app": "my_service", "env": "dev"},
    extracted_labels=["user_id"]
)
listener.start()

logger = logging.getLogger("my_service")
logger.addHandler(handler)

logger.info("User logged in", extra={"user_id": "123"})
```

### Advanced Config (dictConfig)
```python
logging.config.dictConfig({
    "version": 1,
    "handlers": {
        "loki": {"()": lambda: handler},
    },
    "loggers": {
        "app": {"handlers": ["loki"], "level": "DEBUG"},
    },
})
```

---

# lokilog (RU)

Легковесный неблокирующий логгер для **Grafana Loki**.

## Особенности
- **Не блокирует поток**: Вся сетевая работа вынесена в фоновый поток
- **Бинарный протокол**: Поддержка Protobuf + Snappy.
- **Умные лейблы**: Возможность прокидывать поля из `extra` напрямую в индексы Loki.

## Зависимости
- `requests`
- `cramjam` (для сжатия Snappy)

## Быстрый старт

### Простой пример
```python
from lokilog import make_loki_handler
import logging

handler, listener = make_loki_handler(
    url="http://localhost:3100",
    labels={"app": "my_service", "env": "dev"},
    extracted_labels=["user_id"]
)
listener.start()

logger = logging.getLogger("my_service")
logger.addHandler(handler)

logger.info("Пользователь вошел", extra={"user_id": "123"})
```
