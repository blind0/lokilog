import logging
import logging.config
from lokilog import make_loki_handler

LOKI_URL = "http://localhost:3100"

handler, listener = make_loki_handler(
    url=LOKI_URL,
    labels={"app": "sync_demo", "env": "dev"},
    extracted_labels=["component"],
    batch_timeout=1.0,
)
listener.start()

logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "loki": {"()": lambda: handler},
        "console": {"class": "logging.StreamHandler", "formatter": "simple"},
    },
    "formatters": {
        "simple": {"format": "%(asctime)s %(levelname)s %(name)s: %(message)s"},
    },
    "loggers": {
        "sync_demo": {"handlers": ["loki", "console"], "level": "DEBUG", "propagate": False},
    },
})

logger = logging.getLogger("sync_demo")

def process(name: str) -> None:
    logger.debug("Processing %s", name, extra={"component": "processor"})
    logger.info("Finished %s", name, extra={"component": "processor"})

if __name__ == "__main__":
    for i in range(10):
        process(f"item_{i}")
    import time; time.sleep(2)
    listener.stop()
