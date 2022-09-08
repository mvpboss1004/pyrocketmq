from typing import Union

from org.apache.rocketmq.logging import InternalLogger as JInternalLogger
from org.apache.rocketmq.client.log import ClientLogger

from ...common.common import Throwable

from .__init__ import LogLevel

class InternalLogger:
    def __init__(self, logger:JInternalLogger):
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

def getLogger() -> InternalLogger:
    return InternalLogger(ClientLogger.getLog())