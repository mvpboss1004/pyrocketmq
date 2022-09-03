from typing import Any

def java_get_set_is(obj:Any, attr:str, value:Any):
    getattr(obj, 'set'+attr)(value)
    if isinstance(value, bool):
        assert(getattr(obj, 'is'+attr)() == value)
    else:
        assert(getattr(obj, attr[0].lower()+attr[1:]) == value)
