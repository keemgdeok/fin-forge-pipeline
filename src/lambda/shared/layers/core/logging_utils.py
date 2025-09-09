"""
Centralized logging utilities for Transform pipeline

일관된 로깅 형식과 구조화된 로그를 제공합니다.
"""

import logging
import json
import sys
from datetime import datetime


def get_logger(name: str = __name__, level: str = "INFO") -> logging.Logger:
    """
    구조화된 로거 인스턴스를 반환합니다.

    Args:
        name: 로거 이름
        level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        설정된 로거 인스턴스
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        # 핸들러 설정
        handler = logging.StreamHandler(sys.stdout)

        # JSON 형식 포매터
        formatter = StructuredFormatter()
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(getattr(logging, level.upper()))
        logger.propagate = False

    return logger


class StructuredFormatter(logging.Formatter):
    """구조화된 JSON 로그 포매터"""

    def format(self, record: logging.LogRecord) -> str:
        """로그 레코드를 JSON 형식으로 포매팅"""
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # 추가 필드들
        if hasattr(record, "extra"):
            log_data.update(record.extra)

        # 예외 정보
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


def log_execution_context(func):
    """함수 실행 컨텍스트를 로깅하는 데코레이터"""

    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        logger.info(
            f"Starting {func.__name__}",
            extra={"function": func.__name__, "args_count": len(args), "kwargs_keys": list(kwargs.keys())},
        )

        try:
            result = func(*args, **kwargs)
            logger.info(f"Completed {func.__name__}", extra={"function": func.__name__, "status": "success"})
            return result
        except Exception as e:
            logger.error(
                f"Failed {func.__name__}", extra={"function": func.__name__, "status": "error", "error": str(e)}
            )
            raise

    return wrapper
