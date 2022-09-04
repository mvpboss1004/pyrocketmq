from abc import abstractmethod
from enum import Enum
from typing import List, Optional

from jpype import JImplements, JOverride
from java.util import List as JList
from org.apache.rocketmq.client.consumer.listener import ConsumeConcurrentlyContext as JConsumeConcurrentlyContext
from org.apache.rocketmq.client.consumer.listener import ConsumeConcurrentlyStatus as JConsumeConcurrentlyStatus
from org.apache.rocketmq.client.consumer.listener import ConsumeOrderlyContext as JConsumeOrderlyContext
from org.apache.rocketmq.client.consumer.listener import ConsumeOrderlyStatus as JConsumeOrderlyStatus
from org.apache.rocketmq.client.consumer.listener import MessageListenerConcurrently as JMessageListenerConcurrently
from org.apache.rocketmq.client.consumer.listener import MessageListenerOrderly as JMessageListenerOrderly

from ...common.message import MessageExt, MessageQueue

class ConsumeConcurrentlyStatus(Enum):
    CONSUME_SUCCESS = JConsumeConcurrentlyStatus.CONSUME_SUCCESS
    RECONSUME_LATER = JConsumeConcurrentlyStatus.RECONSUME_LATER

class ConsumeOrderlyStatus(Enum):
    SUCCESS = JConsumeOrderlyStatus.SUCCESS
    SUSPEND_CURRENT_QUEUE_A_MOMENT = JConsumeOrderlyStatus.SUCCESS

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
        return int(self.this.getDelayLevelWhenNextConsume())

    def setDelayLevelWhenNextConsume(self, delayLevelWhenNextConsume:int):
        self.this.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume)

    @property
    def ackIndex(self) -> int:
        return int(self.this.getAckIndex())

    def setAckIndex(self, ackIndex:int):
        self.this.setAckIndex(ackIndex)

class ConsumeOrderlyContext(BaseConsumeContext):
    def __init__(self, consume_context:Optional[JConsumeOrderlyContext]=None, message_queue:Optional[MessageQueue]=None):
        BaseConsumeContext.__init__(self, JConsumeOrderlyContext, consume_context, message_queue)
    
    def isAutoCommit(self) -> bool:
        return bool(self.this.isAutoCommit())

    def setAutoCommit(self, autoCommit:bool):
        self.this.setAutoCommit(autoCommit)

    @property
    def suspendCurrentQueueTimeMillis(self) -> int:
        return int(self.this.getSuspendCurrentQueueTimeMillis())

    def setSuspendCurrentQueueTimeMillis(self, suspendCurrentQueueTimeMillis:int):
        self.this.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis)

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