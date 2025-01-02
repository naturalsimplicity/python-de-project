from functools import wraps
from typing import Callable, Annotated, get_type_hints, get_origin


class Injectable:
    def __init__(self, dependency) -> None:
        self.dependency = dependency

def inject(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        type_hints = get_type_hints(func, include_extras=True)
        injected_kwargs = {}
        for arg, hint in type_hints.items():
            if get_origin(hint) is Annotated and isinstance(hint.__metadata__[0], Injectable):
                sub_dependency = inject(hint.__metadata__[0].dependency)
                if arg not in kwargs:
                    injected_kwargs.update({arg: sub_dependency()})
        kwargs.update(injected_kwargs)
        return func(*args, **kwargs)
    return wrapper
