import sys
from abc import abstractmethod
from os.path import dirname
from typing import List, Optional

sys.path.append(dirname(dirname(dirname(__file__))))

from jpype import JImplements, JOverride
from java.util import ArrayList
from org.apache.rocketmq.client.consumer import AllocateMessageQueueStrategy as JAllocateMessageQueueStrategy
from org.apache.rocketmq.client.consumer.rebalance import AllocateMachineRoomNearby as JAllocateMachineRoomNearby
from org.apache.rocketmq.client.consumer.rebalance import AllocateMessageQueueAveragely as JAllocateMessageQueueAveragely
from org.apache.rocketmq.client.consumer.rebalance import AllocateMessageQueueAveragelyByCircle as JAllocateMessageQueueAveragelyByCircle
from org.apache.rocketmq.client.consumer.rebalance import AllocateMessageQueueByConfig as JAllocateMessageQueueByConfig
from org.apache.rocketmq.client.consumer.rebalance import AllocateMessageQueueByMachineRoom as JAllocateMessageQueueByMachineRoom
from org.apache.rocketmq.client.consumer.rebalance import AllocateMessageQueueConsistentHash as JAllocateMessageQueueConsistentHash
from org.apache.rocketmq.client.consumer.rebalance.AllocateMachineRoomNearby import MachineRoomResolver as JMachineRoomResolver
from org.apache.rocketmq.common.consistenthash import HashFunction as JHashFunction

from common import MessageQueue


@JImplements(JMachineRoomResolver)
class MachineRoomResolver:
    @JOverride
    def brokerDeployIn(self, messageQueue):
        return self._brokerDeployIn(MessageQueue(messageQueue))

    @JOverride
    def consumerDeployIn(self, clientID):
        return self._consumerDeployIn(clientID)

    @abstractmethod
    def _brokerDeployIn(self, messageQueue:MessageQueue) -> str:
        pass

    @abstractmethod
    def _consumerDeployIn(self, clientID:str) -> str:
        pass

class BaseAllocateMessageQueueStrategy:
    def __init__(self, AllocateMessageQueueStrategyClass, allocate_message_queue_strategy:Optional[JAllocateMessageQueueStrategy]=None):
        self.this = AllocateMessageQueueStrategyClass() if allocate_message_queue_strategy is None else allocate_message_queue_strategy
    
    def allocate(self, consumerGroup:str, currentCID:str, mqAll:List[MessageQueue], cidAll:List[str]) -> List[MessageQueue]:
        return [MessageQueue(mq) for mq in self.this.allocate(consumerGroup, currentCID, ArrayList([mq.this for mq in mqAll]), ArrayList(cidAll))]

    @property
    def name(self) -> str:
        return str(self.this.getName())

class AllocateMachineRoomNearby(BaseAllocateMessageQueueStrategy):
    def __init__(self,
        allocate_machine_room_nearby:Optional[JAllocateMachineRoomNearby] = None,
        allocate_message_queue_strategy:Optional[BaseAllocateMessageQueueStrategy] = None,
        machine_room_resolver:Optional[JMachineRoomResolver] = None
    ):
        if allocate_machine_room_nearby is None == (allocate_message_queue_strategy is None or machine_room_resolver is None):
            raise Exception('Exactly one of allocate_machine_room_nearby and allocate_message_queue_strategy+machine_room_resolver must be specified')
        elif allocate_machine_room_nearby is not None:
            self.this = allocate_machine_room_nearby
        else:
            self.this = JAllocateMachineRoomNearby(allocate_message_queue_strategy.this, machine_room_resolver)

class AllocateMessageQueueAveragely(BaseAllocateMessageQueueStrategy):
    def __init__(self, allocate_message_queue_strategy:Optional[JAllocateMessageQueueStrategy]=None):
        BaseAllocateMessageQueueStrategy.__init__(self, JAllocateMessageQueueAveragely, allocate_message_queue_strategy)

class AllocateMessageQueueAveragelyByCircle(BaseAllocateMessageQueueStrategy):
    def __init__(self, allocate_message_queue_strategy:Optional[JAllocateMessageQueueStrategy]=None):
        BaseAllocateMessageQueueStrategy.__init__(self, JAllocateMessageQueueAveragelyByCircle, allocate_message_queue_strategy)

class AllocateMessageQueueByConfig(BaseAllocateMessageQueueStrategy):
    def __init__(self, allocate_message_queue_strategy:Optional[JAllocateMessageQueueStrategy]=None):
        BaseAllocateMessageQueueStrategy.__init__(self, JAllocateMessageQueueByConfig, allocate_message_queue_strategy)
    
    @property
    def messageQueueList(self) -> List[MessageQueue]:
        return [MessageQueue(mq) for mq in self.this.getMessageQueueList()]
    
    def setMessageQueueList(self, messageQueueList:List[MessageQueue]):
        self.this.setMessageQueueList(ArrayList([mq.this for mq in messageQueueList]))

class AllocateMessageQueueByMachineRoom(BaseAllocateMessageQueueStrategy):
    def __init__(self, allocate_message_queue_strategy:Optional[JAllocateMessageQueueStrategy]=None):
        BaseAllocateMessageQueueStrategy.__init__(self, JAllocateMessageQueueByMachineRoom, allocate_message_queue_strategy)
    
    @property
    def consumeridcs(self) -> List[str]:
        return [str(idc) for idc in self.this.getConsumeridcs()]
    
    def setConsumeridcs(self, consumeridcs:List[str]):
        self.this.setConsumeridcs(ArrayList(consumeridcs))

class AllocateMessageQueueConsistentHash(BaseAllocateMessageQueueStrategy):
    def __init__(self,
        allocate_message_queue_strategy:Optional[JAllocateMessageQueueStrategy] = None,
        virtualNodecnt:int = 10,
        customHashFunction:Optional[JHashFunction] = None):
        if allocate_message_queue_strategy is not None == (virtualNodecnt is not None or customHashFunction is not None):
            raise Exception('Exactly one of allocate_message_queue_strategy and virtualNodecnt+customHashFunction must be specified')
        elif allocate_message_queue_strategy is not None:
            self.this = allocate_message_queue_strategy
        else:
            self.this = JAllocateMessageQueueConsistentHash(virtualNodecnt, customHashFunction)

@JImplements(JHashFunction)
class HashFunction:
    @JOverride
    def hash(self, key):
        return self._hash(key)

    @abstractmethod
    def _hash(self, key:str) -> int:
        pass