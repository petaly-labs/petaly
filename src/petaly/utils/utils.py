# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
logger = logging.getLogger(__name__)


def sanitize_sensitive_data(data, sensitive_keys=None):
    """
    Recursively sanitizes sensitive fields in data structures by masking their values.
    
    This function masks sensitive fields like passwords, secrets, and API keys
    to prevent them from being logged or exposed.
    
    Args:
        data: Dictionary, list, or other data structure to sanitize
        sensitive_keys: List of keys to mask (defaults to common sensitive fields)
    
    Returns:
        Sanitized data structure with sensitive values masked as "***"
    
    Example:
        >>> config = {'user': 'john', 'password': 'secret123'}
        >>> sanitize_sensitive_data(config)
        {'user': 'john', 'password': '***'}
    """
    if sensitive_keys is None:
        sensitive_keys = [
            'database_password',
            'aws_access_key_id',
            'aws_secret_access_key',
            'password',
            'secret',
            'secret_key',
            'access_key',
            'api_key',
            'token',
            'credential'
        ]
    
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            # Check if key contains any sensitive keyword (case-insensitive)
            key_lower = key.lower()
            is_sensitive = any(sensitive_key.lower() in key_lower for sensitive_key in sensitive_keys)
            
            if is_sensitive:
                sanitized[key] = "***"
            elif isinstance(value, (dict, list)):
                sanitized[key] = sanitize_sensitive_data(value, sensitive_keys)
            else:
                sanitized[key] = value
        return sanitized
    elif isinstance(data, list):
        return [sanitize_sensitive_data(item, sensitive_keys) for item in data]
    else:
        return data


def measure_time(func):
    """This decorator return the execution time for the decorated function."""
    import time
    from functools import wraps

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        logger.debug(f"Function {func.__module__}.{func.__name__}()  ran in {round(end - start, 2)}s")

        return result

    return wrapper

class FormatDict(dict):
    """ With help of this class the function str.format_map() will ignore a key which wasn't specified in the parameter section

    """
    def __missing__(self, key):
        return '{' + str(key) + '}'

