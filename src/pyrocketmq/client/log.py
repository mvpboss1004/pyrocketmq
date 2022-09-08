import os
from typing import Optional, Union

from java.lang import System
from org.apache.rocketmq import InternalLogger as JInternalLogger
from org.apache.rocketmq.client.log import ClientLogger

from ..common.common import Throwable

class LogLevel:
    DEBUG = 'debug'
    INFO = 'info'
    WARN = 'warn'
    ERROR = 'error'

class InternalLogger:
    def __self__(self, logger:JInternalLogger):
        self. this = logger
    
    @property
    def name(self) -> str:
        return str(self.this.getName())
    
    def log(self, level:Union[LogLevel,str], var1:str, *args):
        msg = str(var1)
        if isinstance(level, str):
            level = level.lower()
        func = getattr(self.this, LogLevel(level).value)
        if len(args) == 0:
            func(msg)
        elif len(args) == 1:
            if isinstance(args[0], Throwable):
                func(msg, args[0].this)
            else:
                func(msg, args[0])
        elif len(args) == 2:
            func(msg, args[0], args[1])
        else:
            raise Exception('At most 3 args in rocketmq internal logging')

    def debug(self, var1:str, *args):
        self.log(LogLevel.DEBUG, var1, *args)
    
    def info(self, var1:str, *args):
        self.log(LogLevel.INFO, var1, *args)
    
    def warn(self, var1:str, *args):
        self.log(LogLevel.WARN, var1, *args)
    
    def error(self, var1:str, *args):
        self.log(LogLevel.ERROR, var1, *args)

def basicConfig(
    useSlf4j:Optional[bool] = None,
    root:Optional[str] = None,
    fileMaxIndex:Optional[int] = None,
    fileMaxSize:Optional[int] = None,
    level:Union[LogLevel,str,None] = None,
    fileName:Optional[str] = None,
):
    java_prefix = 'rocketmq.client.log'
    env_prefix = 'CLIENT_LOG_'
    for _type, keys in [
        (bool, ('useSlf4j',)),
        (str, ('root','fileName')),
        (int, ('fileMaxIndex','fileMaxSize')),
        (lambda x: LogLevel(str(x).lower()).value, ('level',)),
    ]:
        for key in keys:
            if eval(key) is not None:
                value = eval(key)
            else:
                value = os.environ.get(env_prefix + (key[3:].upper()), None)
            if value is not None:
                System.setProperty(java_prefix + key.capitalize(), str(_type(value)))

def getLogger() -> InternalLogger:
    return InternalLogger(ClientLogger.getLog())