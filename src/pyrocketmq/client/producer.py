from abc import abstractmethod
from enum import Enum
from typing import Any, List, Optional, Union

from jpype import JImplements, JOverride
from java.lang import Object as JObject
from java.lang import Throwable as JThrowable
from java.util import List as JList
from org.apache.rocketmq.client.producer import DefaultMQProducer
from org.apache.rocketmq.client.producer import MessageQueueSelector as JMessageQueueSelector
from org.apache.rocketmq.client.producer import SendCallback as JSendCallback
from org.apache.rocketmq.client.producer import SendResult as JSendResult
from org.apache.rocketmq.client.producer import SendStatus as JSendStatus
from org.apache.rocketmq.common.message import Message as JMessage

from ..common.common import Throwable
from ..common.message import Message, MessageQueue
from .client import BaseClient

class SendStatus(Enum):
    SEND_OK = JSendStatus.SEND_OK
    FLUSH_DISK_TIMEOUT = JSendStatus.FLUSH_DISK_TIMEOUT
    FLUSH_SLAVE_TIMEOUT = JSendStatus.FLUSH_SLAVE_TIMEOUT
    SLAVE_NOT_AVAILABLE = JSendStatus.SLAVE_NOT_AVAILABLE

class SendResult:
    def __init__(self,
        send_result:Optional[JSendResult] = None,
        sendStatus:Optional[SendStatus] = None,
        msgId:Optional[str] = None,
        offsetMsgId:Optional[str] = None,
        messageQueue:Optional[MessageQueue] = None,
        queueOffset:Optional[int] = None,
        *args, **kwargs):
        if send_result is None:
            self.this = JSendResult()
            if sendStatus is not None:
                self.setSendStatus(sendStatus.value)
            if msgId is not None:
                self.setMsgId(msgId)
            if offsetMsgId is not None:
                self.setOffsetMsgId(offsetMsgId)
            if messageQueue is not None:
                self.setMessageQueue(messageQueue.this)
            if queueOffset is not None:
                self.setQueueOffset(queueOffset)
        else:
            self.this = send_result

    @staticmethod
    def encoderSendResultToJson(obj) -> str:
        return str(JSendResult.encoderSendResultToJson(obj.this))

    @staticmethod
    def decoderSendResultFromJson(js:str):
        return SendResult(JSendResult.decoderSendResultFromJson(js))

    def isTraceOn(self) -> bool:
        return bool(self.this.isTraceOn())

    def setTraceOn(self, traceOn:bool):
        self.this.setTraceOn(traceOn)

    @property
    def regionId(self) -> str:
        return str(self.this.getRegionId())

    def setRegionId(self, regionId:str):
        self.this.setRegionId(regionId)

    @property
    def msgId(self) -> str:
        return str(self.this.getMsgId())

    def setMsgId(self, msgId:str):
        self.this.setMsgId(msgId)

    @property
    def sendStatus(self) -> SendStatus:
        return SendStatus(self.this.getSendStatus())

    def setSendStatus(self, sendStatus:SendStatus):
        self.this.setSendStatus(sendStatus.value)

    @property
    def messageQueue(self) -> MessageQueue:
        return MessageQueue(self.this.getMessageQueue())

    def setMessageQueue(self, messageQueue:MessageQueue):
        self.this.setMessageQueue(messageQueue.this)

    @property
    def queueOffset(self) -> int:
        return int(self.this.getQueueOffset())

    def setQueueOffset(self, queueOffset:int):
        self.this.setQueueOffset(queueOffset)

    @property
    def transactionId(self) -> str:
        return str(self.this.getTransactionId())

    def setTransactionId(self, transactionId:str):
        self.this.setTransactionId(transactionId)

    @property
    def offsetMsgId(self) -> str:
        return str(self.this.getOffsetMsgId())

    def setOffsetMsgId(self, offsetMsgId:str):
        self.this.setOffsetMsgId(offsetMsgId)

@JImplements(JSendCallback)
class SendCallback:
    @JOverride
    def onSuccess(self, sendResult:JSendResult):
        self._onSuccess(SendResult(sendResult))

    @JOverride
    def onException(self, e:JThrowable):
        self._onException(Throwable(e))

    @abstractmethod
    def _onSuccess(self, send_result:SendResult):
        pass

    @abstractmethod
    def _onException(self, e:Throwable):
        pass

@JImplements(JMessageQueueSelector)
class MessageQueueSelector:
    @JOverride
    def select(self, mqs:JList, msg:JMessage, arg:JObject):
        return self._select([MessageQueue(mq) for mq in mqs], Message(msg), arg).this

    @abstractmethod
    def _select(self, mqs:List[MessageQueue], msg:Message, arg:Any) -> MessageQueue:
        pass

class Producer(BaseClient):
    def __init__(self, producerGroup:Optional[str]=None):
        BaseClient.__init__(self, DefaultMQProducer, producerGroup)

    def fetchPublishMessageQueues(self, topic:str) -> List[MessageQueue]:
        return [MessageQueue(mq) for mq in self.this.fetchPublishMessageQueues(topic)]

    def send(self, msgs:Union[Message, List[Message]], *,
        mq:Optional[MessageQueue]=None,
        selector:Optional[MessageQueueSelector]=None, arg:Optional[Any]=None,
        send_callback:Optional[SendCallback]=None, timeout:Optional[int]=None,
        **kwargs) -> Optional[SendResult]:
        if mq is not None and selector is not None:
            raise Exception('Use at most one of mq or selector, not both')
        args = []
        if isinstance(msgs, Message):
            args.append(msgs.this)
        else:
            if selector is not None:
                raise Exception('Sending batch msgs mode is not supported using selector+arg mode')
            elif send_callback is not None:
                raise Exception('Sending batch msgs mode is not supported using send_callback')
            else:
                args.append([msg.this for msg in msgs])
        if mq is not None:
            args.append(mq.this)
        if selector is not None:
            args.append(selector)
            args.append(arg)
        if send_callback is not None:
            args.append(send_callback)
        if timeout is not None:
            args.append(timeout)

        if len(args) == 1:
            ret = self.this.send(args[0])
        elif len(args) == 2:
            ret = self.this.send(args[0],args[1])
        elif len(args) == 3:
            ret = self.this.send(args[0],args[1],args[2])
        elif len(args) == 4:
            ret = self.this.send(args[0],args[1],args[2],args[3])
        else:
            ret = self.this.send(args[0],args[1],args[2],args[3],args[4])
            
        if send_callback is None:
            return SendResult(ret)

    def sendOneway(self, msg:Message, *,
        mq:Optional[MessageQueue]=None,
        selector:Optional[MessageQueueSelector]=None, arg:Optional[Any]=None,
        **kwargs):
        if mq is not None and selector is not None:
            raise Exception('Use at most one of mq or selector, not both')
        elif mq is not None:
            self.this.sendOneway(msg.this, mq.this)
        elif selector is not None:
            self.this.sendOneway(msg.this, selector, arg)
        else:
            self.this.sendOneway(msg.this)

    @property
    def producerGroup(self) -> str:
        return str(self.this.getProducerGroup())

    def setProducerGroup(self, producerGroup:str):
        self.this.setProducerGroup(producerGroup)

    @property
    def createTopicKey(self) -> str:
        return str(self.this.getCreateTopicKey())

    def setCreateTopicKey(self, createTopicKey:str):
        self.this.setCreateTopicKey(createTopicKey)

    @property
    def sendMsgTimeout(self) -> int:
        return int(self.this.getSendMsgTimeout())

    def setSendMsgTimeout(self, sendMsgTimeout:int):
        self.this.setSendMsgTimeout(sendMsgTimeout)

    @property
    def compressMsgBodyOverHowmuch(self) -> int:
        return int(self.this.getCompressMsgBodyOverHowmuch())

    def setCompressMsgBodyOverHowmuch(self, compressMsgBodyOverHowmuch:int):
        self.this.setCompressMsgBodyOverHowmuch(compressMsgBodyOverHowmuch)

    def isRetryAnotherBrokerWhenNotStoreOK(self) -> bool:
        return bool(self.this.isRetryAnotherBrokerWhenNotStoreOK())

    def setRetryAnotherBrokerWhenNotStoreOK(self, retryAnotherBrokerWhenNotStoreOK:bool):
        self.this.setRetryAnotherBrokerWhenNotStoreOK(retryAnotherBrokerWhenNotStoreOK)

    @property
    def maxMessageSize(self) -> int:
        return int(self.this.getMaxMessageSize())

    def setMaxMessageSize(self, maxMessageSize:int):
        self.this.setMaxMessageSize(maxMessageSize)
    
    @property
    def defaultTopicQueueNums(self) -> int:
        return int(self.this.getDefaultTopicQueueNums())

    def setDefaultTopicQueueNums(self, defaultTopicQueueNums:int):
        self.this.setDefaultTopicQueueNums(defaultTopicQueueNums)

    @property
    def retryTimesWhenSendFailed(self) -> int:
        return int(self.this.getRetryTimesWhenSendFailed())

    def setRetryTimesWhenSendFailed(self, retryTimesWhenSendFailed:int):
        self.this.setRetryTimesWhenSendFailed(retryTimesWhenSendFailed)

    def isSendMessageWithVIPChannel(self) -> bool:
        return bool(self.this.isSendMessageWithVIPChannel())

    def setSendMessageWithVIPChannel(self, sendMessageWithVIPChannel:bool):
        self.this.setSendMessageWithVIPChannel(sendMessageWithVIPChannel)

    @property
    def notAvailableDuration(self) -> List[int]:
        return [int(i) for i in self.this.getNotAvailableDuration()]

    def setNotAvailableDuration(self, notAvailableDuration:List[int]):
        self.this.setNotAvailableDuration(notAvailableDuration)

    @property
    def latencyMax(self) -> List[int]:
        return [int(i) for i in self.this.getLatencyMax()]

    def setLatencyMax(self, latencyMax:List[int]):
        self.this.setLatencyMax(latencyMax)

    def isSendLatencyFaultEnable(self) -> bool:
        return bool(self.this.isSendLatencyFaultEnable())

    def setSendLatencyFaultEnable(self, sendLatencyFaultEnable:bool):
        self.this.setSendLatencyFaultEnable(sendLatencyFaultEnable)

    @property
    def retryTimesWhenSendAsyncFailed(self) -> int:
        return int(self.this.getRetryTimesWhenSendAsyncFailed())

    def setRetryTimesWhenSendAsyncFailed(self, retryTimesWhenSendAsyncFailed:int):
        self.this.setRetryTimesWhenSendAsyncFailed(retryTimesWhenSendAsyncFailed)
