from enum import Enum
from typing import Optional, Union

from org.apache.rocketmq.logging import InternalLogger as JInternalLogger
from org.apache.rocketmq.client.log import ClientLogger

from ..common.common import Throwable

class LogLevel(Enum):
    DEBUG = 'debug'
    INFO = 'info'
    WARN = 'warn'
    ERROR = 'error'

    @staticmethod
    def LEVEL(level):
        if isinstance(level, LogLevel):
            return level
        else:
            return LogLevel(str(level).lower())

class InternalLogger:
    def __init__(self, logger:JInternalLogger):
        self. this = logger
    
    @property
    def name(self) -> str:
        return str(self.this.getName())
    
    def log(self, level:Union[LogLevel,str], var1:str, var2:Optional[Throwable]=None):
        msg = str(var1)
        func = getattr(self.this, LogLevel.LEVEL(level).value)
        if var2 is None:
            func(msg)
        elif isinstance(var2, Throwable):
            func(msg, var2.this)
        else:
            func(msg, var2)

    def debug(self, var1:str, var2:Optional[Throwable]=None):
        self.log(LogLevel.DEBUG, var1, var2)
    
    def info(self, var1:str, var2:Optional[Throwable]=None):
        self.log(LogLevel.INFO, var1, var2)
    
    def warn(self, var1:str, var2:Optional[Throwable]=None):
        self.log(LogLevel.WARN, var1, var2)
    
    def error(self, var1:str, var2:Optional[Throwable]=None):
        self.log(LogLevel.ERROR, var1, var2)

def getLogger() -> InternalLogger:
    return InternalLogger(ClientLogger.getLog())