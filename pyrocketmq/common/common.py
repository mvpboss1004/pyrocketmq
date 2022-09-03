from enum import Enum
from typing import Optional

from java.io import ByteArrayOutputStream, PrintStream
from java.lang import Throwable as JThrowable
from org.apache.rocketmq.common.consumer import ConsumeFromWhere as JConsumeFromWhere
from org.apache.rocketmq.common.filter import ExpressionType as JExpressionType
from org.apache.rocketmq.common.protocol.heartbeat import MessageModel as JMessageModel
from org.apache.rocketmq.remoting.protocol import LanguageCode as JLanguageCode

class ConsumeFromWhere(Enum):
    CONSUME_FROM_LAST_OFFSET = JConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
    CONSUME_FROM_FIRST_OFFSET = JConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
    CONSUME_FROM_TIMESTAMP = JConsumeFromWhere.CONSUME_FROM_TIMESTAMP

class ExpressionType(Enum):
    SQL92 = str(JExpressionType.SQL92)
    TAG = str(JExpressionType.TAG)
     
class MessageModel(Enum):
    BROADCASTING = JMessageModel.BROADCASTING
    CLUSTERING = JMessageModel.CLUSTERING

    @property
    def modeCN(self):
        return self.value.getModeCN()

class LanguageCode(Enum):
    JAVA = JLanguageCode.JAVA # 0
    CPP = JLanguageCode.CPP # 1
    DOTNET = JLanguageCode.DOTNET # 2
    PYTHON = JLanguageCode.PYTHON # 3
    DELPHI = JLanguageCode.DELPHI # 4
    ERLANG = JLanguageCode.ERLANG # 5
    RUBY = JLanguageCode.RUBY # 6
    OTHER = JLanguageCode.OTHER # 7
    HTTP = JLanguageCode.HTTP # 8
    GO = JLanguageCode.GO # 9
    PHP = JLanguageCode.PHP # 10
    OMS = JLanguageCode.OMS # 11

    @staticmethod
    def valueOf(code:int):
        return LanguageCode(JLanguageCode.valueOf(code))

    @property
    def code(self) -> int:
        return int(self.value.getCode())

class Throwable:
    def __init__(self, throwable:Optional[JThrowable]=None, message:Optional[str]=None, cause=None):
        if throwable is not None and (message is not None or cause is not None):
            raise Exception('At most one of throwable and message+cause should be specified')
        elif throwable is not None:
            self.this = throwable
        elif message is not None and cause is not None:
            self.this = JThrowable(message, cause.this)
        elif message is not None:
            self.this = JThrowable(message)
        elif cause is not None:
            self.this = JThrowable(cause.this)
        else:
            self.this = JThrowable()
    
    @property
    def message(self) -> str:
        return str(self.this.getMessage())
    
    def printStackTrace(self) -> str:
        out = ByteArrayOutputStream()
        self.this.printStackTrace(PrintStream(out))
        return str(out.toString())
