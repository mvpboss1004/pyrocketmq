import os
from enum import Enum
from typing import List, Optional, Union

class LogLevel(Enum):
    DEBUG = 'debug'
    INFO = 'info'
    WARN = 'warn'
    ERROR = 'error'

# return log args to start jvm
def basicConfig(
    useSlf4j:Optional[bool] = None,
    root:Optional[str] = None,
    fileMaxIndex:Optional[int] = None,
    fileMaxSize:Optional[int] = None,
    level:Union[LogLevel,str,None] = None,
    fileName:Optional[str] = None,
) -> List[str]:
    for _type, keys in [
        (bool, ('useSlf4j',)),
        (str, ('root','fileName')),
        (int, ('fileMaxIndex','fileMaxSize')),
        (lambda x: LogLevel(str(x).lower()).value, ('level',)),
    ]:
        args = []
        for key in keys:
            if eval(key) is not None:
                value = eval(key)
            else:
                value = os.environ.get('ROCKETMQ_LOG_' + key.upper(), None)
            if value is not None:
                args.append(f'-Drocketmq.client.log{key[0].upper()}{key[1:]}={_type(value)}')
    return args