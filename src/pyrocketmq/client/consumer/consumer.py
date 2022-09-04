from abc import abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Union

from jpype import JImplements, JOverride
from java.lang import Throwable as JThrowable
from java.util import ArrayList, HashSet
from java.util import List as JList
from org.apache.rocketmq.client.consumer import AllocateMessageQueueStrategy as JAllocateMessageQueueStrategy
from org.apache.rocketmq.client.consumer import DefaultMQPullConsumer, DefaultMQPushConsumer
from org.apache.rocketmq.client.consumer import MessageQueueListener as JMessageQueueListener
from org.apache.rocketmq.client.consumer import MessageSelector as JMessageSelector
from org.apache.rocketmq.client.consumer import PullCallback as JPullCallback
from org.apache.rocketmq.client.consumer import PullResult as JPullResult
from org.apache.rocketmq.client.consumer import PullStatus as JPullStatus

from ...common.common import ConsumeFromWhere, ExpressionType, MessageModel, Throwable
from ...common.message import MessageExt, MessageQueue
from ..client import BaseClient
from .listener import MessageListenerConcurrently, MessageListenerOrderly
from .rebalance import AllocateMessageQueueStrategyMap, BaseAllocateMessageQueueStrategy
from .store import OffsetStore

class PullStatus(Enum):
    FOUND = JPullStatus.FOUND
    NO_NEW_MSG = JPullStatus.NO_NEW_MSG
    NO_MATCHED_MSG = JPullStatus.NO_MATCHED_MSG
    OFFSET_ILLEGAL = JPullStatus.OFFSET_ILLEGAL

class MessageQueueListener:
    def __init__(self, message_queue_listener:JMessageQueueListener):
        self.this = message_queue_listener
    
    def messageQueueChanged(self, topic:str, mqAll:List[MessageQueue], mqDivided:List[MessageQueue]):
        self.this.messageQueueChanged(topic, [mq.this for mq in mqAll], [mq.this for mq in mqDivided])

class MessageSelector:
    def __init__(self, message_selector:JMessageSelector):
        self.this = message_selector

    @staticmethod
    def bySql(sql:str):
        return MessageSelector(JMessageSelector.bySql(sql))

    @staticmethod
    def byTag(tag:str):
        return MessageSelector(JMessageSelector.byTag(tag))

    @property
    def expressionType(self) -> ExpressionType:
        return ExpressionType(str(self.this.getExpressionType()))
    
    @property
    def expression(self) -> str:
        return str(self.this.getExpression())

class PullResult(list):
    def __init__(self, 
        pull_result:Optional[JPullResult] = None,
        pullStatus:Optional[PullStatus] = None, 
        nextBeginOffset:Optional[int] = None,
        minOffset:Optional[int] = None, 
        maxOffset:Optional[int] = None,
        msgFoundList:Union[ArrayList, List[MessageExt], None] = None,
        *args, **kwargs):
        if pull_result is None == (pullStatus is None or nextBeginOffset is None or minOffset is None or maxOffset is None or msgFoundList is None):
            raise Exception('Exactly one of pull_result and nextBeginOffset+minOffset+maxOffset+msgFoundList must be specified')
        elif pull_result is not None:
            self.this = pull_result
        else:
            self.this = JPullResult(pullStatus.value, nextBeginOffset, minOffset, maxOffset,
                msgFoundList if isinstance(msgFoundList,ArrayList) else ArrayList([m.this for m in msgFoundList])
            )
        list.__init__(self, [MessageExt(msg) for msg in self.this.getMsgFoundList()])

    @property
    def pullStatus(self) -> PullStatus:
        return PullStatus(self.this.getPullStatus())
    
    @property
    def nextBeginOffset(self) -> int:
        return int(self.this.getNextBeginOffset())

    @property    
    def minOffset(self) -> int:
        return int(self.this.getMinOffset())

    @property
    def maxOffset(self) -> int:
        return int(self.this.getMaxOffset())

@JImplements(JPullCallback)
class PullCallback:
    @JOverride
    def onSuccess(self, pullResult:JPullResult):
        self._onSuccess(PullResult(pullResult))

    @JOverride
    def onException(self, e:JThrowable):
        self._onException(Throwable(e))

    @abstractmethod
    def _onSuccess(self, pull_result:PullResult):
        pass

    @abstractmethod
    def _onException(self, e:Throwable):
        pass

@JImplements(JAllocateMessageQueueStrategy)
class AllocateMessageQueueStrategy:
    @JOverride
    def allocate(self, consumerGroup:str, currentCID:str, mqAll:JList, cidAll:JList):
        return [mq.this for mq in 
            self._allocate(str(consumerGroup), str(currentCID), [mq.this for mq in mqAll], [str(cid) for cid in cidAll])
        ]

    @JOverride
    def getName(self):
        return self._getName()

    @abstractmethod
    def _allocate(self, consumerGroup:str, currentCID:str, mqAll:List[MessageQueue], cidAll:List[str]) -> List[MessageQueue]:
        pass
    
    @abstractmethod
    def _getName(self) -> str:
        pass

class BaseConsumer(BaseClient):
    @property
    def allocateMessageQueueStrategy(self) -> BaseAllocateMessageQueueStrategy:
        allocate_message_queue_strategy = self.this.getAllocateMessageQueueStrategy()
        return AllocateMessageQueueStrategyMap\
            .get(type(allocate_message_queue_strategy), BaseAllocateMessageQueueStrategy)\
            (allocate_message_queue_strategy)
        
    def setAllocateMessageQueueStrategy(self, allocateMessageQueueStrategy:BaseAllocateMessageQueueStrategy):
        self.this.setAllocateMessageQueueStrategy(allocateMessageQueueStrategy.this)
    
    @property
    def consumerGroup(self) -> str:
        return str(self.this.getConsumerGroup())
    
    def setConsumerGroup(self, consumerGroup:str):
        self.this.setConsumerGroup(consumerGroup)

    @property
    def messageModel(self) -> MessageModel:
        return MessageModel(self.this.getMessageModel())
    
    def setMessageModel(self, messageModel:MessageModel):
        self.this.setMessageModel(messageModel.value)
    
    def sendMessageBack(self, msg:MessageExt, delayLevel:int, brokerName:Optional[str]=None):
        self.this.sendMessageBack(msg.this, delayLevel, brokerName)

    def fetchSubscribeMessageQueues(self, topic:str) -> List[MessageQueue]:
        return [MessageQueue(mq) for mq in self.this.fetchSubscribeMessageQueues(topic)]

    @property
    def offsetStore(self) -> OffsetStore:
        return OffsetStore(self.this.getOffsetStore())
    
    def setOffsetStore(self, offsetStore:OffsetStore):
        self.this.setOffsetStore(offsetStore.this)

    def isUnitMode(self) -> bool:
        return bool(self.this.isUnitMode())
    
    def setUnitMode(self, isUnitMode:bool):
        self.this.setUnitMode(isUnitMode)

    @property
    def maxReconsumeTimes(self) -> int:
        return int(self.this.getMaxReconsumeTimes())
    
    def setMaxReconsumeTimes(self, maxReconsumeTimes:int):
        self.this.setMaxReconsumeTimes(maxReconsumeTimes)

class PullConsumer(BaseConsumer):
    def __init__(self, consumerGroup:Optional[str]=None):
        BaseClient.__init__(self, DefaultMQPullConsumer, consumerGroup)
    
    @property
    def messageQueueListener(self) -> MessageQueueListener:
        return MessageQueueListener(self.this.getMessageQueueListener())
    
    def setMessageQueueListener(self, messageQueueListener:MessageQueueListener):
        self.this.setMessageQueueListener(messageQueueListener.this)

    @property
    def brokerSuspendMaxTimeMillis(self) -> int:
        return int(self.this.getBrokerSuspendMaxTimeMillis())
    
    def setBrokerSuspendMaxTimeMillis(self, brokerSuspendMaxTimeMillis:int):
        self.this.setBrokerSuspendMaxTimeMillis(brokerSuspendMaxTimeMillis)

    @property
    def consumerPullTimeoutMillis(self) -> int:
        return int(self.this.getConsumerPullTimeoutMillis())
    
    def setConsumerPullTimeoutMillis(self, consumerPullTimeoutMillis:int):
        self.this.setConsumerPullTimeoutMillis(consumerPullTimeoutMillis)
    
    @property
    def consumerTimeoutMillisWhenSuspend(self) -> int:
        return int(self.this.getConsumerTimeoutMillisWhenSuspend())
    
    def setConsumerTimeoutMillisWhenSuspend(self, consumerTimeoutMillisWhenSuspend:int):
        self.this.setConsumerTimeoutMillisWhenSuspend(consumerTimeoutMillisWhenSuspend)
    
    @property
    def registerTopics(self) -> List[str]:
        return [str(t) for t in self.this.getRegisterTopics()]

    def setRegisterTopics(self, registerTopics:Union[HashSet,List[str]]):
        rt = registerTopics if isinstance(registerTopics,HashSet) else HashSet(registerTopics)
        self.this.setRegisterTopics(rt)
    
    def registerMessageQueueListener(self, topic:str, listener:MessageQueueListener):
        self.this.registerMessageQueueListener(topic, listener.this)
                
    def pull(self, mq:MessageQueue, subExpression:str, offset:int, maxNums:int, pullCallback:Optional[PullCallback]=None, timeout:Optional[int]=None) -> Optional[PullResult]:
        if pullCallback is None:
            if timeout is None:
                res = self.this.pull(mq.this, subExpression, offset, maxNums)
            else:
                res = self.this.pull(mq.this, subExpression, offset, maxNums, timeout)
            return PullResult(res)
        else:
            if timeout is None:
                self.this.pull(mq.this, subExpression, offset, maxNums, pullCallback)
            else:
                self.this.pull(mq.this, subExpression, offset, maxNums, pullCallback, timeout)
    
    def pullBlockIfNotFound(self, mq:MessageQueue, subExpression:str, offset:int, maxNums:int, pullCallback:Optional[PullCallback]=None) -> Optional[PullResult]:
        if pullCallback is None:
            return PullResult(self.this.pullBlockIfNotFound(mq, subExpression, offset, maxNums))
        else:
            self.this.pullBlockIfNotFound(mq, subExpression, offset, maxNums, pullCallback)
    
    def updateConsumeOffset(self, mq:MessageQueue, offset:int):
        self.updateConsumeOffset(mq.this, offset)
    
    def fetchConsumeOffset(self, mq:MessageQueue, fromStore:bool) -> int:
        return max(int(self.this.fetchConsumeOffset(mq.this, fromStore)), 0)
    
    def fetchMessageQueuesInBalance(self, topic:str) -> List[MessageQueue]:
        return [MessageQueue(mq) for mq in self.this.fetchMessageQueuesInBalance(topic)]

class PushConsumer(BaseConsumer):
    def __init__(self, consumerGroup:Optional[str]=None):
        BaseClient.__init__(self, DefaultMQPushConsumer, consumerGroup)

    @property
    def consumeConcurrentlyMaxSpan(self) -> int:
        return int(self.this.getConsumeConcurrentlyMaxSpan())
    
    def setConsumeConcurrentlyMaxSpan(self, consumeConcurrentlyMaxSpan:int):
        self.this.setConsumeConcurrentlyMaxSpan(consumeConcurrentlyMaxSpan)

    @property
    def consumeFromWhere(self) -> ConsumeFromWhere:
        return ConsumeFromWhere(self.this.getConsumeFromWhere())

    def setConsumeFromWhere(self, consumeFromWhere:ConsumeFromWhere):
        self.this.setConsumeFromWhere(consumeFromWhere.value)

    @property
    def consumeMessageBatchMaxSize(self) -> int:
        return int(self.this.getConsumeMessageBatchMaxSize())

    def setConsumeMessageBatchMaxSize(self, consumeMessageBatchMaxSize:int):
        self.this.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize)

    @property
    def consumeThreadMax(self) -> int:
        return int(self.this.getConsumeThreadMax())
    
    def setConsumeThreadMax(self, consumeThreadMax:int):
        return self.this.setConsumeThreadMax(consumeThreadMax)

    @property
    def consumeThreadMin(self) -> int:
        return int(self.this.getConsumeThreadMin())
    
    def setConsumeThreadMin(self, consumeThreadMin:int):
        return self.this.setConsumeThreadMin(consumeThreadMin)

    @property
    def pullBatchSize(self) -> int:
        return int(self.this.getPullBatchSize())

    def setPullBatchSize(self, pullBatchSize:int):
        self.this.setPullBatchSize(pullBatchSize)

    @property
    def pullInterval(self) -> int:
        return int(self.this.getPullInterval())

    def setPullInterval(self, pullInterval:int):
        return self.this.setPullInterval(pullInterval)

    @property
    def pullThresholdForQueue(self) -> int:
        return int(self.this.getPullThresholdForQueue())

    def setPullThresholdForQueue(self, pullThresholdForQueue:int):
        self.this.setPullThresholdForQueue(pullThresholdForQueue)

    @property
    def pullThresholdForTopic(self) -> int:
        return int(self.this.getPullThresholdForTopic())

    def setPullThresholdForTopic(self, pullThresholdForTopic:int):
        self.this.setPullThresholdForTopic(pullThresholdForTopic)

    @property
    def pullThresholdSizeForQueue(self) -> int:
        return int(self.this.getPullThresholdSizeForQueue())

    def setPullThresholdSizeForQueue(self, pullThresholdSizeForQueue:int):
        self.this.setPullThresholdSizeForQueue(pullThresholdSizeForQueue)

    @property
    def pullThresholdSizeForTopic(self) -> int:
        return int(self.this.getPullThresholdSizeForTopic())

    def setPullThresholdSizeForTopic(self, pullThresholdSizeForTopic:int):
        self.this.setPullThresholdSizeForTopic(pullThresholdSizeForTopic)

    @property
    def subscription(self) -> Dict[str,str]:
        return {str(k):str(v) for k,v in self.this.getSubscription().items()}

    def setSubscription(self, subscription:Dict[dict,dict]):
        self.this.setSubscription(subscription)

    def registerMessageListener(self, messageListener:Union[MessageListenerConcurrently,MessageListenerOrderly]):
        self.this.registerMessageListener(messageListener)

    def subscribe(self, topic:str, filter:Union[str,MessageSelector]):
        if isinstance(filter, str):
            self.this.subscribe(topic, filter)
        elif isinstance(filter, MessageSelector):
            self.this.subscribe(topic, filter.this)
        else:
            raise Exception('Filter type must be one of str,MessageSelector')
    
    def unsubscribe(self, topic:str):
        self.this.unsubscribe(topic)

    def updateCorePoolSize(self, corePoolSize:int):
        self.this.updateCorePoolSize(corePoolSize)

    def suspend(self):
        self.this.suspend()

    def resume(self):
        self.this.resume()

    @property
    def consumeTimestamp(self) -> str:
        return str(self.this.getConsumeTimestamp())

    def setConsumeTimestamp(self, consumeTimestamp:str):
        self.this.setConsumeTimestamp(consumeTimestamp)

    def isPostSubscriptionWhenPull(self) -> bool:
        return bool(self.this.isPostSubscriptionWhenPull())

    def setPostSubscriptionWhenPull(self, postSubscriptionWhenPull:bool):
        self.this.setPostSubscriptionWhenPull(postSubscriptionWhenPull)

    @property
    def adjustThreadPoolNumsThreshold(self) -> int:
        return int(self.this.getAdjustThreadPoolNumsThreshold())

    def setAdjustThreadPoolNumsThreshold(self, adjustThreadPoolNumsThreshold:int):
        self.this.setAdjustThreadPoolNumsThreshold(adjustThreadPoolNumsThreshold)
    
    @property
    def suspendCurrentQueueTimeMillis(self) -> int:
        return int(self.this.getSuspendCurrentQueueTimeMillis())

    def setSuspendCurrentQueueTimeMillis(self, suspendCurrentQueueTimeMillis:int):
        self.this.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis)

    @property
    def consumeTimeout(self) -> int:
        return int(self.this.getConsumeTimeout())

    def setConsumeTimeout(self, consumeTimeout:int):
        self.this.setConsumeTimeout(consumeTimeout)
