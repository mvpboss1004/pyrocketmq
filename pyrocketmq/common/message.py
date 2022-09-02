from socket import inet_ntoa
from typing import Any, Dict, List, Optional, Tuple, Union

from java.net import InetSocketAddress
from java.util import ArrayList
from org.apache.rocketmq.common.message import Message as JMessage
from org.apache.rocketmq.common.message import MessageBatch as JMessageBatch
from org.apache.rocketmq.common.message import MessageExt as JMessageExt
from org.apache.rocketmq.common.message import MessageQueue as JMessageQueue


def socket2tuple(sock:InetSocketAddress) -> Tuple[str,int]:
    ip = inet_ntoa(bytes([i%256 for i in sock.getAddress().getAddress()]))
    port = sock.getPort()
    return (ip,port)

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

    def putUserProperty(self, name:str, value:str):
        self.this.putUserProperty(name, value)

    def getUserProperty(self, name:str) -> str:
        return str(self.this.getUserProperty(name))

    @property
    def topic(self) -> str:
        return str(self.this.getTopic())
    
    def setTopic(self, topic:str):
        self.this.setTopic(topic)
    
    @property
    def tags(self) -> str:
        return str(self.this.getTags())

    def setTags(self, tags:str):
        self.this.setTags(tags)

    @property
    def keys(self) -> str:
        return str(self.this.getKeys())

    def setKeys(self, keys:List[str]):
        self.this.setKeys(keys)

    @property
    def delayTimeLevel(self) -> int:
        return int(self.this.getDelayTimeLevel())
    
    def setDelayTimeLevel(self, level:int):
        self.this.setDelayTimeLevel(level)

    def isWaitStoreMsgOK(self) -> bool:
        return bool(self.this.isWaitStoreMsgOK())

    def setWaitStoreMsgOK(self, waitStoreMsgOK:bool):
        self.this.setWaitStoreMsgOK(waitStoreMsgOK)

    @property
    def flag(self) -> int:
        return int(self.this.getFlag())
    
    def setFlag(self, flag:int):
        self.this.setFlag(flag)
    
    @property
    def body(self) -> bytes:
        return bytes(self.this.getBody())

    def setBody(self, body:bytes):
        self.this.setBody(body)

    @property
    def properties(self) -> Dict[str,str]:
        return {str(k):str(v) for k,v in self.this.getProperties().items()}

    @property
    def buyerId(self) -> str:
        return str(self.this.getBuyerId())

    def setBuyerId(self, buyerId:str):
        self.this.setBuyerId(buyerId)

    @property
    def transactionId(self) -> str:
        return str(self.this.getTransactionId())

    def setTransactionId(self, transactionId:str):
        self.this.setTransactionId(transactionId)

class MessageBatch(list):
    def __init__(self, message_batch:JMessageBatch):
        self.this = message_batch
        list.__init__(self, [Message(msg) for msg in self.this.iterator()])
    
    @staticmethod
    def generateFromList(messages:Union[ArrayList, List[Message]]):
        msgs = messages if isinstance(messages,ArrayList) else ArrayList([msg.this for msg in messages])
        return MessageBatch(JMessageBatch.generateFromList(msgs))

    def encode(self) -> bytes:
        return bytes(self.this.encode())

class MessageExt(Message):
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
        return int(self.this.getQueueId())

    def setQueueId(self, queueId:int):
        self.this.setQueueId(queueId)

    @property
    def bornTimestamp(self) -> int:
        return int(self.this.getBornTimestamp())

    def setBornTimestamp(self, bornTimestamp:int):
        self.this.setBornTimestamp(bornTimestamp)

    @property
    def bornHost(self) -> Tuple[str,int]:
        return socket2tuple(self.this.getBornHost())

    def setBornHost(self, bornHost:Tuple[str,int]):
        self.this.setBornHost(InetSocketAddress(bornHost[0],bornHost[1]))

    @property
    def bornHostString(self) -> str:
        return str(self.this.getBornHostString())

    @property
    def bornHostNameString(self) -> str:
        return str(self.this.getBornHostNameString())

    @property
    def storeTimestamp(self) -> int:
        return int(self.this.getStoreTimestamp())

    def setStoreTimestamp(self, storeTimestamp:int):
        self.this.setStoreTimestamp(storeTimestamp)

    @property
    def storeHost(self) -> Tuple[str,int]:
        return socket2tuple(self.this.getStoreHost())

    def setStoreHost(self, storeHost:Tuple[str,int]):
        self.this.setStoreHost(InetSocketAddress(storeHost[0],storeHost[1]))

    @property
    def msgId(self) -> str:
        return str(self.this.getMsgId())

    def setMsgId(self, msgId:str):
        self.this.setMsgId(msgId)

    @property
    def sysFlag(self) -> int:
        return int(self.this.getSysFlag())

    def setSysFlag(self, sysFlag:int):
        self.this.setSysFlag(sysFlag)

    @property
    def bodyCRC(self) -> int:
        return int(self.this.getBodyCRC())

    def setBodyCRC(self, bodyCRC:int):
        self.this.setBodyCRC(bodyCRC)

    @property
    def queueOffset(self) -> int:
        return int(self.this.getQueueOffset())

    def setQueueOffset(self, queueOffset:int):
        self.this.setQueueOffset(queueOffset)

    @property
    def commitLogOffset(self) -> int:
        return int(self.this.getCommitLogOffset())

    def setCommitLogOffset(self, physicOffset:int):
        self.this.setCommitLogOffset(physicOffset)

    @property
    def storeSize(self) -> int:
        return int(self.this.getStoreSize())

    def setStoreSize(self, storeSize:int):
        self.this.setStoreSize(storeSize)

    @property
    def reconsumeTimes(self) -> int:
        return int(self.this.getReconsumeTimes())

    def setReconsumeTimes(self, reconsumeTimes:int):
        self.this.setReconsumeTimes(reconsumeTimes)

    @property
    def preparedTransactionOffset(self) -> int:
        return int(self.this.getPreparedTransactionOffset())

    def setPreparedTransactionOffset(self, preparedTransactionOffset:int):
        self.this.setPreparedTransactionOffset(preparedTransactionOffset)

class MessageQueue:
    def __init__(self,
        message_queue:Optional[JMessageQueue] = None,
        topic:Optional[str] = None,
        brokerName:Optional[str] = None,
        queueId:Optional[int] = None,
        *args, **kwargs):
        if message_queue is None:
            self.this = JMessageQueue()
            if topic:
                self.setTopic(topic)
            if brokerName:
                self.setBrokerName(brokerName)
            if queueId:
                self.setQueueId(queueId)
        else:
            self.this = message_queue

    def __eq__(self, obj:Any) -> bool:
        return isinstance(obj,MessageQueue) and bool(self.this.equals(obj.this))
    
    def __ne__(self, obj:Any) -> bool:
        return not self==obj
    
    def __lt__(self, obj:Any) -> bool:
        return int(self.this.compareTo(obj.this)) == -1
    
    def __gt__(self, obj:Any) -> bool:
        return int(self.this.compareTo(obj.this)) == 1

    def __le__(self, obj:Any) -> bool:
        return self<obj or self==obj
    
    def __ge__(self, obj:Any) -> bool:
        return self>obj or self==obj
    
    def __hash__(self) -> int:
        return int(self.this.hashCode())

    @property
    def topic(self) -> str:
        return str(self.this.getTopic())

    def setTopic(self, topic:str):
        self.this.setTopic(topic)

    @property
    def brokerName(self) -> str:
        return str(self.this.getBrokerName())

    def setBrokerName(self, brokerName:str):
        self.this.setBrokerName(brokerName)

    @property
    def queueId(self) -> int:
        return int(self.this.getQueueId())

    def setQueueId(self, queueId:int):
        self.this.setQueueId(queueId)