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
from org.apache.rocketmq.client.consumer import PullResult as JPullResult
from org.apache.rocketmq.client.consumer import PullStatus as JPullStatus
from org.apache.rocketmq.client.consumer import PullCallback as JPullCallback
from org.apache.rocketmq.client.consumer import MessageSelector as JMessageSelector
from org.apache.rocketmq.client.consumer.listener import ConsumeConcurrentlyContext as JConsumeConcurrentlyContext
from org.apache.rocketmq.client.consumer.listener import ConsumeConcurrentlyStatus as JConsumeConcurrentlyStatus
from org.apache.rocketmq.client.consumer.listener import ConsumeOrderlyContext as JConsumeOrderlyContext
from org.apache.rocketmq.client.consumer.listener import ConsumeOrderlyStatus as JConsumeOrderlyStatus
from org.apache.rocketmq.client.consumer.listener import MessageListenerConcurrently as JMessageListenerConcurrently
from org.apache.rocketmq.client.consumer.listener import MessageListenerOrderly as JMessageListenerOrderly
from org.apache.rocketmq.client.consumer.rebalance import AllocateMessageQueueAveragely,AllocateMessageQueueAveragelyByCircle,AllocateMessageQueueByConfig,AllocateMessageQueueByMachineRoom,AllocateMessageQueueConsistentHash
from org.apache.rocketmq.client.consumer.store import OffsetStore as JOffsetStore
from org.apache.rocketmq.client.consumer.store import ReadOffsetType as JReadOffsetType
from org.apache.rocketmq.common.consumer import ConsumeFromWhere as JConsumeFromWhere

from .common import BaseClient, ExpressionType, MessageExt, MessageModel, MessageQueue, Throwable

class PullStatus(Enum):
    FOUND = JPullStatus.FOUND
    NO_NEW_MSG = JPullStatus.NO_NEW_MSG
    NO_MATCHED_MSG = JPullStatus.NO_MATCHED_MSG
    OFFSET_ILLEGAL = JPullStatus.OFFSET_ILLEGAL

class ReadOffsetType(Enum):
    READ_FROM_MEMORY = JReadOffsetType.READ_FROM_MEMORY
    READ_FROM_STORE = JReadOffsetType.READ_FROM_STORE
    MEMORY_FIRST_THEN_STORE = JReadOffsetType.MEMORY_FIRST_THEN_STORE

class ConsumeFromWhere(Enum):
    CONSUME_FROM_LAST_OFFSET = JConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
    CONSUME_FROM_FIRST_OFFSET = JConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET
    CONSUME_FROM_TIMESTAMP = JConsumeFromWhere.CONSUME_FROM_TIMESTAMP

class ConsumeConcurrentlyStatus(Enum):
    CONSUME_SUCCESS = JConsumeConcurrentlyStatus.CONSUME_SUCCESS
    RECONSUME_LATER = JConsumeConcurrentlyStatus.RECONSUME_LATER

class ConsumeOrderlyStatus(Enum):
    SUCCESS = JConsumeOrderlyStatus.SUCCESS
    SUSPEND_CURRENT_QUEUE_A_MOMENT = JConsumeOrderlyStatus.SUCCESS

class AllocateMessageQueueStrategyType(Enum):
    AVG = AllocateMessageQueueAveragely
    AVG_BY_CIRCLE = AllocateMessageQueueAveragelyByCircle
    CONFIG = AllocateMessageQueueByConfig
    MACHINE_ROOM = AllocateMessageQueueByMachineRoom
    CONSISTENT_HASH = AllocateMessageQueueConsistentHash

class AllocateMessageQueueStrategy:
    def __init__(self, allocate_message_queue_strategy:JAllocateMessageQueueStrategy):
        self.this = allocate_message_queue_strategy
    
    def allocate(self, consumerGroup:str, currentCID:str, mqAll:List[MessageQueue], cidAll:List[str]) -> List[MessageQueue]:
        ret = self.this.allocate(consumerGroup, currentCID, [mq.this for mq in mqAll], cidAll)
        return [MessageQueue(mq) for mq in ret]

    @property
    def name(self) -> str:
        return self.this.getName()

class BaseConsumeContext:
    def __init__(self, ConsumeContextClass, consume_context=None, message_queue:Optional[MessageQueue]=None):
        if consume_context is not None:
            self.this = consume_context
        elif message_queue is not None:
            self.this = ConsumeContextClass(message_queue.this)
        else:
            raise Exception('At least one of consume_concurrently_context,message_queue must be specified.')
    
    @property
    def messageQueue(self) -> MessageQueue:
        return MessageQueue(self.this.getMessageQueue())

class ConsumeConcurrentlyContext(BaseConsumeContext):
    def __init__(self, consume_context:Optional[JConsumeConcurrentlyContext]=None, message_queue:Optional[MessageQueue]=None):
        BaseConsumeContext.__init__(self, JConsumeConcurrentlyContext, consume_context, message_queue)

    @property
    def delayLevelWhenNextConsume(self) -> int:
        return self.this.getDelayLevelWhenNextConsume()

    def setDelayLevelWhenNextConsume(self, delayLevelWhenNextConsume:int):
        self.this.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume)

    @property
    def ackIndex(self) -> int:
        return self.this.getAckIndex()

    def setAckIndex(self, ackIndex:int):
        self.this.setAckIndex(ackIndex)

class ConsumeOrderlyContext(BaseConsumeContext):
    def __init__(self, consume_context:Optional[JConsumeOrderlyContext]=None, message_queue:Optional[MessageQueue]=None):
        BaseConsumeContext.__init__(self, JConsumeOrderlyContext, consume_context, message_queue)
    
    def isAutoCommit(self) -> bool:
        return self.this.isAutoCommit()

    def setAutoCommit(self, autoCommit:bool):
        self.this.setAutoCommit(autoCommit)

    @property
    def suspendCurrentQueueTimeMillis(self) -> int:
        return self.this.getSuspendCurrentQueueTimeMillis()

    def setSuspendCurrentQueueTimeMillis(self, suspendCurrentQueueTimeMillis:int):
        self.this.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis)

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
        return ExpressionType(self.this.getExpressionType())
    
    @property
    def expression(self) -> str:
        return str(self.this.getExpression())

class OffsetStore:
    def __init__(self, offset_store:JOffsetStore):
        self.this = offset_store

    def load(self):
        self.this.load()
    
    def updateOffset(self, mq:MessageQueue, offset:int, increaseOnly:bool):
        self.this.updateOffset(mq.this, offset, increaseOnly)

    def readOffset(self, mq:MessageQueue, _type:ReadOffsetType) -> int:
        return int(self.this.readOffset(mq.this, _type.value))

    def persistAll(self, mqs:List[MessageQueue]):
        self.this.persistAll([mq.this for mq in mqs])

    def persist(self, mq:MessageQueue):
        self.this.persist(mq.this)

    def removeOffset(self, mq:MessageQueue):
        self.this.removeOffset(mq.this)

    def cloneOffsetTable(self, topic:str) -> Dict[MessageQueue,int]:
        return {MessageQueue(mq):ofs for mq,ofs in self.cloneOffsetTable(topic).items()}

    def updateConsumeOffsetToBroker(self, mq:MessageQueue, offset:int, isOneway:bool):
        self.this.updateConsumeOffsetToBroker(mq.this, offset, isOneway)

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

@JImplements(JMessageListenerConcurrently)
class MessageListenerConcurrently:
    @JOverride
    def consumeMessage(self, msgs:JList, context:JConsumeConcurrentlyContext):
        self._consumeMessage([MessageExt(msg) for msg in msgs], ConsumeConcurrentlyContext(context)).value
    
    @abstractmethod
    def _consumeMessage(self, msgs:List[MessageExt], context:ConsumeConcurrentlyContext) -> ConsumeConcurrentlyStatus:
        pass

@JImplements(JMessageListenerOrderly)
class MessageListenerOrderly:
    @JOverride
    def consumeMessage(self, msgs:JList, context:JConsumeOrderlyContext):
        self._consumeMessage([MessageExt(msg) for msg in msgs], ConsumeOrderlyContext(context)).value

    @abstractmethod
    def _consumeMessage(self, msgs:List[MessageExt], context:ConsumeOrderlyContext) -> ConsumeOrderlyStatus:
        pass

class BaseConsumer(BaseClient):
    @property
    def allocateMessageQueueStrategy(self) -> AllocateMessageQueueStrategy:
        return AllocateMessageQueueStrategy(self.this.getAllocateMessageQueueStrategy()) 
    
    def setAllocateMessageQueueStrategy(self, allocateMessageQueueStrategy:AllocateMessageQueueStrategy):
        self.this.setAllocateMessageQueueStrategy(allocateMessageQueueStrategy.this)
    
    @property
    def consumerGroup(self) -> str:
        return self.this.getConsumerGroup()
    
    def setConsumerGroup(self, consumerGroup:str):
        self.this.setConsumerGroup(consumerGroup)

    @property
    def messageModel(self) -> MessageModel:
        return MessageModel(self.this.getMessageModel())
    
    def setMessageModel(self, messageModel:MessageModel):
        self.this.setMessageModel(messageModel.value)
    
    def sendMessageBack(self, msg:MessageExt, delayLevel:int, brokerName:Optional[str]=None):
        self.this.sendMessageBack(msg, delayLevel, brokerName)

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
        return list(self.this.getRegisterTopics())

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
    
    def pullBlockIfNotFound(self, mq:MessageQueue, subExpression:str, offset:int, maxNums:int, pullCallback:Optional[PullCallback]=None) -> PullResult:
        if pullCallback is None:
            return PullResult(self.this.pullBlockIfNotFound(mq, subExpression, offset, maxNums))
        else:
            self.this.pullBlockIfNotFound(mq, subExpression, offset, maxNums, pullCallback)
        
    
    def updateConsumeOffset(self, mq:MessageQueue, offset:int):
        self.updateConsumeOffset(mq.this, offset)
    
    def fetchConsumeOffset(self, mq:MessageQueue, fromStore:bool) -> int:
        return int(self.this.fetchConsumeOffset(mq.this, fromStore))
    
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
        return {k:v for k,v in self.this.getSubscription().items()}

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
