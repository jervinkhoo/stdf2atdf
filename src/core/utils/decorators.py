import time
from functools import wraps
import logging

logger = logging.getLogger(__name__)

def timing_decorator(func):
    """Decorator to measure function execution time."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        processing_time = end_time - start_time
        message = f"{func.__name__} took {processing_time:.6f} seconds"
        logger.info(message)
        return result
    return wrapper
