from contextlib import contextmanager
import logging
import time

logger = logging.getLogger(__name__)

@contextmanager
def track_latency(step, request_id, **kwargs):
    start = time.time()
    try:
        yield
        success = True
    except Exception as e:
        success = False
        logger.error("step_latency", extra={"step": step, "request_id": request_id, "error":str(e), **kwargs})
        raise
    finally:
        latency = time.time() - start
        logger.info("step_latency", extra={"step": step, "request_id": request_id, "latency_ms": round(latency * 1000, 2), "success":success, **kwargs})