import inspect

def filter_kwargs(func, kwargs):
    sig = inspect.signature(func)
    allowed_params = sig.parameters
    return {
        k: v for k, v in kwargs.items()
        if k in allowed_params
    }