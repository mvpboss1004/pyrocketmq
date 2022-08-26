from enum import Enum
from socket import inet_aton, inet_ntoa
from typing import Dict, List, Optional, Tuple

import jpype.imports
from java.lang import Throwable as JThrowable
from java.net import InetAddress, InetSocketAddress
from org.apache.rocketmq.client import QueryResult as JQueryResult
from org.apache.rocketmq.common.message import Message as JMessage
from org.apache.rocketmq.common.message import MessageBatch as JMessageBatch
from org.apache.rocketmq.common.message import MessageExt as JMessageExt
from org.apache.rocketmq.common.message import MessageQueue as JMessageQueue
from org.apache.rocketmq.common.protocol.heartbeat import MessageModel as JMessageModel

def socket2tuple(sock:InetSocketAddress) -> Tuple[str,int]:
    ip = inet_ntoa(bytes([i%256 for i in sock.getAddress().getAddress()]))
    port = sock.getPort()
    return (ip,port)

def tuple2socket(addr:Tuple[str,int]) -> InetSocketAddress:
    ip = InetAddress.getByAddress(inet_aton(addr))
    port = addr[1]
    return InetSocketAddress(ip,port)

class ToStringMixin:
    def __repr__(self) -> str:
        return self.this.toString()

class MessageModel(Enum):
    BROADCASTING = JMessageModel.BROADCASTING
    CLUSTERING = JMessageModel.CLUSTERING

class Throwable(ToStringMixin):
    def __init__(self, throwable:JThrowable):
        self.this = throwable
    
    @property
    def message(self) -> str:
        return self.this.getMessage()
    
    def printStackTrace(self):
        self.this.printStackTrace()

class Message:
    def __init__(self, message:Optional[JMessage]=None, topic:Optional[str]=None, body:Optional[bytes]=None,
        tags:str='', keys:str='', flag:int=0, waitStoreMsgOK:int=True, *args, **kwargs
    ):
        if message is None:
            if topic and body:
                self.this = JMessage(topic, tags, keys, flag, body, waitStoreMsgOK)
            else:
                raise Exception('Both topic and body must be specified when creating message')
        else:
            self.this = message

    def setKeys(self, keys:str):
        self.this.setKeys(keys)

    def putProperty(self, name:str, value:str):
        self.this.putProperty(name, value)

    def getProperty(self, name:str):
        self.this.getProperty(name)

    def clearProperty(self, name:str):      
        self.this.clearProperty(name)

    def putUserProperty(self, name:str, value:str):
        self.this.putUserProperty(name, value)

    def getUserProperty(self, name:str) -> str:
        return self.this.getUserProperty(name)

    @property
    def topic(self) -> str:
        return self.this.getTopic()
    
    def setTopic(self, topic:str):
        self.this.setTopic(topic)
    
    @property
    def tags(self) -> str:
        return self.this.getTags()

    def setTags(self, tags:str):
        self.this.setTags(tags)

    @property
    def keys(self) -> str:
        return self.this.getKeys()

    def setKeys(self, keys:List[str]):
        self.this.setKeys(keys)

    @property
    def delayTimeLevel(self) -> int:
        self.this.getDelayTimeLevel()
    
    def setDelayTimeLevel(self, level:int):
        self.this.setDelayTimeLevel(level)

    def isWaitStoreMsgOK(self) -> bool:
        return self.this.isWaitStoreMsgOK()

    def setWaitStoreMsgOK(self, waitStoreMsgOK:bool):
        self.this.setWaitStoreMsgOK(waitStoreMsgOK)

    @property
    def flag(self) -> int:
        return self.this.getFlag()
    
    def setFlag(self, flag:int):
        self.this.setFlag(flag)
    
    @property
    def body(self) -> bytes:
        return self.this.getBody()

    def setBody(self, body:bytes):
        self.this.setBody(body)

    @property
    def properties(self) -> Dict[str,str]:
        return self.this.getProperties()
    
    def setProperties(self, properties:Dict[str,str]):
        self.this.setProperties(properties)

    @property
    def buyerId(self) -> str:
        return self.getBuyerId()

    def setBuyerId(self, buyerId:str):
        self.this.setBuyerId(buyerId)

    @property
    def transactionId(self) -> str:
        return self.this.getTransactionId()

    def setTransactionId(self, transactionId:str):
        self.this.setTransactionId(transactionId)

class MessageBatch(list, ToStringMixin):
    def __init__(self, message_batch:JMessage):
        self.this = message_batch
        list.__init__(self, [Message(msg) for msg in self.this.iterator()])
    
    @staticmethod
    def generateFromList(messages:List[Message]):
        return MessageBatch(JMessageBatch.generateFromList([msg.this for msg in messages]))

    def encode(self) -> bytes:
        return self.this.encode()

class MessageExt(ToStringMixin):
    def __init__(self,
        message_ext:Optional[JMessageExt] = None,
        queueId:Optional[int] = None,
        bornTimestamp:Optional[int] = None,
        bornHost:Optional[Tuple[str,int]] = None,
        storeTimestamp:Optional[int] = None,
        storeHost:Optional[Tuple[str,int]] = None,
        msgId:Optional[str] = None,
        *args, **kwargs):
        self.this = JMessageExt() if message_ext is None else message_ext
        if queueId is not None:
            self.setQueueId(queueId)
        if bornTimestamp is not None:
            self.setBornTimestamp(bornTimestamp)
        if bornHost is not None:
            self.setBornHost(bornHost)
        if storeTimestamp is not None:
            self.setStoreTimestamp(storeTimestamp)
        if storeHost is not None:
            self.setStoreHost(storeHost)
        if msgId is not None:
            self.setMsgId(msgId)

    @property
    def queueId(self) -> int:
        return self.this.getQueueId()

    def setQueueId(self, queueId:int):
        self.this.setQueueId(queueId)

    @property
    def bornTimestamp(self) -> int:
        return self.this.getBornTimestamp()

    def setBornTimestamp(self, bornTimestamp:int):
        self.this.setBornTimestamp(bornTimestamp)

    @property
    def bornHost(self) -> Tuple[str,int]:
        return socket2tuple(self.this.getBornHost())

    def setBornHost(self, bornHost:Tuple[str,int]):
        self.this.setBornHost(tuple2socket(bornHost))

    @property
    def bornHostString(self) -> str:
        return self.this.getBornHostString()

    @property
    def bornHostNameString(self) -> str:
        return self.this.getBornHostNameString()

    @property
    def storeTimestamp(self) -> int:
        return self.this.getStoreTimestamp()

    def setStoreTimestamp(self, storeTimestamp:int):
        self.this.setStoreTimestamp(storeTimestamp)

    @property
    def storeHost(self) -> Tuple[str,int]:
        return socket2tuple(self.this.getStoreHost())

    def setStoreHost(self, storeHost:Tuple[str,int]):
        self.this.setStoreHost(tuple2socket(storeHost))

    @property
    def msgId(self) -> str:
        return self.this.getMsgId()

    def setMsgId(self, msgId:str):
        self.this.setMsgId(msgId)

    @property
    def sysFlag(self) -> int:
        return self.this.getSysFlag()

    def setSysFlag(self, sysFlag:int):
        self.this.setSysFlag(sysFlag)

    @property
    def bodyCRC(self) -> int:
        return self.this.getBodyCRC()

    def setBodyCRC(self, bodyCRC:int):
        self.this.setBodyCRC(bodyCRC)

    @property
    def queueOffset(self) -> int:
        return self.this.getQueueOffset()

    def setQueueOffset(self, queueOffset:int):
        self.this.setQueueOffset(queueOffset)

    @property
    def commitLogOffset(self) -> int:
        return self.this.getCommitLogOffset()

    def setCommitLogOffset(self, physicOffset:int):
        self.this.setCommitLogOffset(physicOffset)

    @property
    def storeSize(self) -> int:
        return self.this.getStoreSize()

    def setStoreSize(self, storeSize:int):
        self.this.setStoreSize(storeSize)

    @property
    def reconsumeTimes(self) -> int:
        return self.this.getReconsumeTimes()

    def setReconsumeTimes(self, reconsumeTimes:int):
        self.this.setReconsumeTimes(reconsumeTimes)

    @property
    def preparedTransactionOffset(self) -> int:
        return self.this.getPreparedTransactionOffset()

    def setPreparedTransactionOffset(self, preparedTransactionOffset:int):
        self.this.setPreparedTransactionOffset(preparedTransactionOffset)

class QueryResult(list, ToStringMixin):
    def __init__(self, query_result:JQueryResult):
        self.this = query_result
        list.__init__([MessageExt(msg) for msg in self.this.getMessageList()])

    @property
    def indexLastUpdateTimestamp(self) -> int:
        return self.this.getIndexLastUpdateTimestamp()

class MessageQueue(ToStringMixin):
    def __init__(self,
        message_queue:Optional[JMessageQueue] = None,
        topic:Optional[str] = None,
        brokerName:Optional[str] = None,
        queueId:Optional[int] = None,
        *args, **kwargs):
        if message_queue is None:
            self.this = JMessageQueue()
        else:
            self.this = message_queue
        if topic:
            self.this.setTopic(topic)
        if brokerName:
            self.this.setBrokerName(brokerName)
        if queueId:
            self.this.setQueueId(queueId)

    @property
    def topic(self) -> Optional[str]:
        return self.this.getTopic()

    @property
    def brokerName(self) -> Optional[str]:
        return self.this.getBrokerName()

    @property
    def queueId(self) -> Optional[int]:
        return self.this.getQueueId()

class BaseClient(ToStringMixin):
    def __init__(self, ClientClass, group:Optional[str]=None):
        if group is None:
            self.this = ClientClass()
        else:
            self.this = ClientClass(group)
    
    def start(self):
        self.this.start()
    
    def shutdown(self):
        self.this.shutdown()

    def createTopic(self, key:str, newTopic:str, queueNum:int, topicSysFlag:Optional[int]=0):
        self.this.createTopic(key, newTopic, queueNum, topicSysFlag)
    
    def searchOffset(self, mq:MessageQueue, timestamp:int) -> int:
        return self.this.searchOffset(mq.this, timestamp)

    def maxOffset(self, mq:MessageQueue) -> int:
        return self.this.maxOffset(mq.this)

    def minOffset(self, mq:MessageQueue) -> int:
        return self.this.minOffset(mq)

    def earliestMsgStoreTime(self, mq:MessageQueue) -> int:
        return self.this.earliestMsgStoreTime(mq)

    def viewMessage(self, *,
        offsetMsgId:Optional[str]=None,
        topic:Optional[str]=None, msgId:Optional[str]=None,
        **Kwargs) -> MessageExt:
        if offsetMsgId is not None and (topic is not None or msgId is not None):
            raise Exception('Use one of offsetMsgId or topic+msgId, not both')
        elif offsetMsgId is not None:
            ret = self.this.viewMessage(offsetMsgId)
        elif topic is None or msgId is None:
            raise Exception('Both of topic and msgId must be specified')
        else:
            ret = self.this.viewMessage(topic, msgId)
        return MessageExt(ret)

    def queryMessage(self, topic:str, key:str, maxNum:int, begin:int, end:int) -> QueryResult:
        return QueryResult(self.this.queryMessage(topic, key, maxNum, begin, end))