from functools import wraps
from time import sleep


def retry(retries: int = 5, delay: float = 3):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == retries:
                        print(f"Error: {repr(e)}")
                        print(f'"{func.__name__}()" 执行失败，已重试{retries}次')
                        raise Exception('{}执行失败！'.format(func.__name__))
                    else:
                        print(
                            f"Error: {repr(e)}，{delay}秒后第[{i+1}/{retries}]次重试..."
                        )
                        sleep(delay)
        return wrapper
    return decorator