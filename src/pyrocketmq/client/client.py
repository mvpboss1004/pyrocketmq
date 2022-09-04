from typing import List, Optional, Union

from java.util import ArrayList
from org.apache.rocketmq.client import ClientConfig as JClientConfig
from org.apache.rocketmq.client import QueryResult as JQueryResult

from ..common.common import LanguageCode
from ..common.message import MessageExt, MessageQueue

class QueryResult(list):
    def __init__(self,
        query_result:Optional[JQueryResult] = None,
        indexLastUpdateTimestamp:Optional[int] = None,
        messageList:Union[ArrayList, List[MessageExt], None] = None,
        *args, **kwargs):
        if query_result is not None == indexLastUpdateTimestamp is not None and messageList is not None:
            raise Exception('Exactly one of query_result and indexLastUpdateTimestamp+messageList must be specified')
        elif query_result is not None:
            self.this = query_result
        else:
            self.this = JQueryResult(
                indexLastUpdateTimestamp,
                messageList if isinstance(messageList,ArrayList) else ArrayList([msg.this for msg in messageList])
            )
        list.__init__(self, [MessageExt(msg) for msg in self.this.getMessageList()])

    @property
    def indexLastUpdateTimestamp(self) -> int:
        return int(self.this.getIndexLastUpdateTimestamp())

class ClientConfig:
    def __init__(self, client_config:Optional[JClientConfig]=None):
        self.this = JClientConfig() if client_config is None else client_config

    def buildMQClientId(self) -> str:
        return str(self.this.buildMQClientId())

    @property
    def clientIP(self) -> str:
        return str(self.this.getClientIP())

    def setClientIP(self, clientIP:str):
        self.this.setClientIP(clientIP)

    @property
    def instanceName(self) -> str:
        return str(self.this.getInstanceName())

    def setInstanceName(self, instanceName:str):
        self.this.setInstanceName(instanceName)
    
    def changeInstanceNameToPID(self):
        self.this.changeInstanceNameToPID()

    def resetClientConfig(self, cc):
        self.this.resetClientConfig(cc.this)
    
    def cloneClientConfig(self):
        return ClientConfig(self.this.cloneClientConfig())

    @property
    def namesrvAddr(self) -> str:
        return str(self.this.getNamesrvAddr())

    def setNamesrvAddr(self, namesrvAddr:str):
        self.this.setNamesrvAddr(namesrvAddr)

    @property
    def clientCallbackExecutorThreads(self) -> int:
        return int(self.this.getClientCallbackExecutorThreads())

    def setClientCallbackExecutorThreads(self, clientCallbackExecutorThreads:int):
        self.this.setClientCallbackExecutorThreads(clientCallbackExecutorThreads)

    @property
    def pollNameServerInterval(self) -> int:
        return int(self.this.getPollNameServerInterval())

    def setPollNameServerInterval(self, pollNameServerInterval:int):
        self.this.setPollNameServerInterval(pollNameServerInterval)

    @property
    def heartbeatBrokerInterval(self) -> int:
        return int(self.this.getHeartbeatBrokerInterval())

    def setHeartbeatBrokerInterval(self, heartbeatBrokerInterval:int):
        self.this.setHeartbeatBrokerInterval(heartbeatBrokerInterval)

    @property
    def persistConsumerOffsetInterval(self) -> int:
        return int(self.this.getPersistConsumerOffsetInterval())

    def setPersistConsumerOffsetInterval(self, persistConsumerOffsetInterval:int):
        self.this.setPersistConsumerOffsetInterval(persistConsumerOffsetInterval)
    
    @property
    def unitName(self) -> str:
        return str(self.this.getUnitName())

    def setUnitName(self, unitName:str):
        self.this.setUnitName(unitName)
    
    def isUnitMode(self) -> bool:
        return bool(self.this.isUnitMode())

    def setUnitMode(self, unitMode:bool):
        self.this.setUnitMode(unitMode)

    def isVipChannelEnabled(self) -> bool:
        return bool(self.this.isVipChannelEnabled())

    def setVipChannelEnabled(self, vipChannelEnabled:bool):
        self.this.setVipChannelEnabled(vipChannelEnabled)
     
    def isUseTLS(self) -> bool:
        return bool(self.this.isUseTLS())

    def setUseTLS(self, useTLS:bool):
        self.this.setUseTLS(useTLS)

    @property
    def language(self) -> LanguageCode:
        return LanguageCode(self.this.getLanguage())
    
    def setLanguage(self, language:LanguageCode):
        self.this.setLanguage(language.value)

class BaseClient(ClientConfig):
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
        return int(self.this.searchOffset(mq.this, timestamp))

    def maxOffset(self, mq:MessageQueue) -> int:
        return int(self.this.maxOffset(mq.this))

    def minOffset(self, mq:MessageQueue) -> int:
        return int(self.this.minOffset(mq.this))

    def earliestMsgStoreTime(self, mq:MessageQueue) -> int:
        return int(self.this.earliestMsgStoreTime(mq.this))

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