from murmurhash.mrmr import hash

from .listener import *
from .rebalance import *

class TestRebalance:
    class MyHashFunction(HashFunction):
        def _hash(self, key:str) -> int:
            return hash(key)

    class MyMachineRoomResolver(MachineRoomResolver):
        @staticmethod
        def room_by_last_char(text:str) -> str:
            return 'Room' + str(ord(text[-1])%2)

        def _brokerDeployIn(self, messageQueue:MessageQueue) -> str:
            TestRebalance.MyMachineRoomResolver.room_by_last_char(messageQueue.brokerName)

        def _consumerDeployIn(self, clientID:str) -> str:
            TestRebalance.MyMachineRoomResolver.room_by_last_char(clientID)
    
    def test_AllocateMessageQueueStrategyBase(self):
        text = 'x'
        cidAll = ['a','b']
        mqAll = [MessageQueue(topic=text, brokerName=f"{cidAll[i]}@{i}", queueId=i) for i in range(len(cidAll))]
        
        stg = AllocateMessageQueueAveragelyByCircle()
        assert(stg.allocate(text, cidAll[1], mqAll, cidAll)[0].queueId == 1)
        
        stg = AllocateMessageQueueByConfig()
        stg.setMessageQueueList(mqAll[:1])
        assert(stg.allocate(text, cidAll[1], mqAll, cidAll) == stg.messageQueueList)
        
        stg = AllocateMessageQueueByMachineRoom()
        stg.setConsumeridcs(cidAll[:1])
        assert(stg.consumeridcs == cidAll[:1])
        assert(stg.allocate(text, cidAll[1], mqAll, cidAll)[0].queueId == 0)

        stg = AllocateMessageQueueConsistentHash(
            virtualNodecnt = len(mqAll),
            customHashFunction = TestRebalance.MyHashFunction()
        )

        stg = AllocateMessageQueueAveragely()
        assert(stg.allocate(text, cidAll[0], mqAll, cidAll)[0].queueId == 0)

        assert(stg.allocate(text, cidAll[0], mqAll, cidAll)[0].queueId == 0)
        stg = AllocateMachineRoomNearby(
            allocate_message_queue_strategy = stg, 
            machine_room_resolver = TestRebalance.MyMachineRoomResolver())
        assert(stg.allocate(text, cidAll[0], mqAll, cidAll)[0].queueId == 1)
