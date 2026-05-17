import logging
import sys
import json
from core.context import request_id_var
from dotenv import load_dotenv

import os

load_dotenv()

log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
logging_level = getattr(logging, log_level_name, logging.INFO)

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "function": record.funcName,
            "file": record.filename,
            "line": record.lineno,
        }

        request_id = request_id_var.get()
        if request_id:
            log_record["request_id"] = request_id

        standard_keys = {"message", "asctime", "levelname", "name", "filename", "lineno", "funcName", "created", "thread", "threadName", "process", "processName", "pathname", "module", "exc_info", "exc_text", "stack_info", "msecs", "relativeCreated", "levelno", "msg", "args"}
        for key, value in record.__dict__.items():
            if key not in standard_keys and not key.startswith("_"):
                log_record[key] = value

        return json.dumps(log_record)


class MaxLevelFilter(logging.Filter):
    def __init__(self, max_level):
        self.max_level = max_level

    def filter(self, record):
        return record.levelno < self.max_level


def setup_logging():
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(JsonFormatter())
    stdout_handler.addFilter(MaxLevelFilter(logging.WARNING))

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(JsonFormatter())
    stderr_handler.setLevel(logging.WARNING)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)
    root_logger.handlers = [h for h in root_logger.handlers if not (isinstance(h, logging.StreamHandler) and h.stream in (sys.stdout, sys.stderr))]
    root_logger.addHandler(stdout_handler)
    root_logger.addHandler(stderr_handler)
    return root_logger