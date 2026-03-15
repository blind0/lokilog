import asyncio
import logging
from lokilog import make_loki_handler

LOKI_URL = "http://localhost:3100"

handler, listener = make_loki_handler(
    url=LOKI_URL,
    labels={"app": "async_demo", "env": "dev"},
    extracted_labels=["user_id", "request_id"],
    batch_timeout=1.0,
)
listener.start()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("async_demo")
logger.addHandler(handler)
logger.propagate = False

async def handle_request(user_id: str, request_id: str, path: str) -> None:
    logger.info("Request started", extra={"user_id": user_id, "request_id": request_id, "path": path})
    await asyncio.sleep(0.05)
    logger.info("Request finished", extra={"user_id": user_id, "request_id": request_id, "status": 200})

async def main() -> None:
    tasks = [handle_request(f"user_{i}", f"req_{i:04d}", f"/api/resource/{i}") for i in range(20)]
    await asyncio.gather(*tasks)
    await asyncio.sleep(2)
    listener.stop()

if __name__ == "__main__":
    asyncio.run(main())
