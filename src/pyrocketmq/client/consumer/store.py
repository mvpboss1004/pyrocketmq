from enum import Enum
from typing import Dict, List

from org.apache.rocketmq.client.consumer.store import LocalFileOffsetStore as JLocalFileOffsetStore
from org.apache.rocketmq.client.consumer.store import OffsetStore as JOffsetStore
from org.apache.rocketmq.client.consumer.store import ReadOffsetType as JReadOffsetType
from org.apache.rocketmq.client.consumer.store import RemoteBrokerOffsetStore as JRemoteBrokerOffsetStore

from ...common.common import MessageModel
from ...common.message import MessageQueue

class ReadOffsetType(Enum):
    READ_FROM_MEMORY = JReadOffsetType.READ_FROM_MEMORY
    READ_FROM_STORE = JReadOffsetType.READ_FROM_STORE
    MEMORY_FIRST_THEN_STORE = JReadOffsetType.MEMORY_FIRST_THEN_STORE

OffsetStoreMap = Enum('OffsetStoreMap', [
    (MessageModel.BROADCASTING.name, JLocalFileOffsetStore),
    (MessageModel.CLUSTERING.name, JRemoteBrokerOffsetStore),
])

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
        return {MessageQueue(mq):int(ofs) for mq,ofs in self.cloneOffsetTable(topic).items()}

    def updateConsumeOffsetToBroker(self, mq:MessageQueue, offset:int, isOneway:bool):
        self.this.updateConsumeOffsetToBroker(mq.this, offset, isOneway)

class LocalFileOffsetStore(OffsetStore):
    def __init__(self, local_file_offset_store:JLocalFileOffsetStore):
        self.this = local_file_offset_store

class RemoteBrokerOffsetStore(OffsetStore):
    def __init__(self, remote_broker_offset_store:JRemoteBrokerOffsetStore):
        self.this = remote_broker_offset_store
