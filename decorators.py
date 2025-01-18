import inspect
import functools
from alert.discord import send_alert


def try_alert(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func_return = func(*args, **kwargs)
            return func_return
        except Exception as e:
            func_module = inspect.getmodule(func)
            send_alert(msg=f"There was an error when running "
                           f"function `{func.__name__}` in `{func_module.__file__}`. "
                           f"\n\n {str(e)[:1000]}",
                       channel='Error')

    return wrapper
