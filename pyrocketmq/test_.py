import json
import os
from datetime import datetime
from typing import Iterable
from java.lang import Exception as JException
from java.net import InetSocketAddress
from org.apache.rocketmq.common.message import Message as JMessage

from pyrocketmq import *

def print_enums(enums:Iterable):
    print('')
    for e in enums:
        print(','.join([str(i) for i in e]))

def java_get_set_is(obj, attr, value):
    getattr(obj, 'set'+attr)(value)
    if isinstance(value, bool):
        assert(getattr(obj, 'is'+attr)() == value)
    else:
        assert(getattr(obj, attr[0].lower()+attr[1:]) == value)

class TestCommon:
    def test_socket2tuple(self):
        ip = '127.0.0.1'
        port = 9876
        assert(socket2tuple(InetSocketAddress(ip,port)) == (ip,port))
    
    def test_enums(self):
        print_enums((ExpressionType,MessageModel,LanguageCode))
        for lc,code in zip(LanguageCode, range(len(LanguageCode))):
            assert(lc == LanguageCode.valueOf(code))
    
    def test_Throwable(self):
        msg = 'x'
        e = Throwable(JException(msg))
        assert(e.message==msg)
        e.printStackTrace()

    def test_Message(self):
        msg = Message(JMessage())
        text = 'x'
        num = 1
        bl = True
        for value,attrs in [
            (text, ('Tags','Keys','BuyerId','Topic','TransactionId',)),
            (num, ('Flag','DelayTimeLevel')),
            (bl, ('WaitStoreMsgOK',)),
            (text.encode(), ('Body',)),
        ]:
            for attr in attrs:
                java_get_set_is(msg, attr, value)
        msg.putUserProperty(text, text)
        assert(msg.properties == {
            'BUYER_ID':text, 'KEYS':text, text:text, 'TAGS':text, 'DELAY':str(num), 'WAIT':str(bl).lower()
        })
    
    def test_MessageBatch(self):
        text = 'x'
        msgs = MessageBatch.generateFromList([Message(topic=text, body=text.encode())])
        for msg in msgs:
            assert(isinstance(msg, Message))
        print(msgs.encode().replace(b'\x00',b''))
    
    def test_MessageExt(self):
        msg = MessageExt()
        t = datetime(1970,1,1,8,0,0,1)
        num = int(t.timestamp() * 1000)
        text = '127.0.0.1'
        addr = (text,num)
        for value,attrs in [
            (text, ('MsgId',)),
            (num, ('QueueId','BornTimestamp','StoreTimestamp','SysFlag','BodyCRC','QueueOffset','CommitLogOffset','StoreSize','ReconsumeTimes','PreparedTransactionOffset')),
            (addr, ('BornHost','StoreHost'))
        ]:
            for attr in attrs:
                java_get_set_is(msg, attr, value)
        assert(msg.bornHostString == text)
        assert(msg.bornHostNameString == 'localhost')
    
    def test_QueryResult(self):
        num = 1000
        result = QueryResult(indexLastUpdateTimestamp=num, messageList=[MessageExt()])
        assert(result.indexLastUpdateTimestamp == num)
        assert(len(result) == 1)
    
    def test_MessageQueue(self):
        text = 'x'
        num = 1
        mq1 = MessageQueue()
        for value,attrs in [
            (text, ('Topic','BrokerName')),
            (num, ('QueueId',)),
        ]:
            for attr in attrs:
                java_get_set_is(mq1, attr, value)
        num = 2
        mq2 = MessageQueue(topic=text, brokerName=text, queueId=num)
        assert(mq1 == mq1)
        assert(mq1 <= mq1)
        assert(mq1 >= mq1)
        assert(mq1 < mq2)
        assert(mq2 > mq1)
        assert(mq1 != mq2)
        print(mq1.__hash__())
    
    def test_ClientConfig(self):
        text = '127.0.0.1'
        num = 100
        cc = ClientConfig()
        for value,attrs in [
            (text, ('ClientIP','UnitName',)),
            (num, ('ClientCallbackExecutorThreads','PollNameServerInterval','HeartbeatBrokerInterval','PersistConsumerOffsetInterval',)),
            (False, ('UnitMode','VipChannelEnabled','UseTLS',)),
            (f'{text}:{num}', ('NamesrvAddr',)),
            (LanguageCode.PYTHON, ('Language',)),
        ]:
            for attr in attrs:
                java_get_set_is(cc, attr, value)
        cc.changeInstanceNameToPID()
        assert(int(cc.instanceName) == os.getpid())
        cc.resetClientConfig(cc.cloneClientConfig())
        print(cc.buildMQClientId())

class TestProducer:
    def test_enums(self):
        print_enums((SendStatus,))
    
    def test_SendResult(self):
        sr = SendResult()
        sr = SendResult.decoderSendResultFromJson(
            SendResult.encoderSendResultToJson(sr)
        )
        text = 'x'
        num = 1
        for value,attrs in [
            (text, ('RegionId','MsgId','TransactionId','OffsetMsgId')),
            (num, ('QueueOffset',)),
            (True, ('TraceOn',)),
            (SendStatus.SEND_OK, ('SendStatus',)),
            (MessageQueue(topic=text,brokerName=text,queueId=num), ('MessageQueue',)),
        ]:
            for attr in attrs:
                java_get_set_is(sr, attr, value)

    def test_Producer(self):
        prd = Producer()
        for value,attrs in [
            ('x', ('ProducerGroup','CreateTopicKey')),
            (1, ('SendMsgTimeout','CompressMsgBodyOverHowmuch','MaxMessageSize','DefaultTopicQueueNums','RetryTimesWhenSendFailed','RetryTimesWhenSendAsyncFailed')),
            (True, ('RetryAnotherBrokerWhenNotStoreOK','SendMessageWithVIPChannel','SendLatencyFaultEnable')),
            ([1], ('NotAvailableDuration','LatencyMax'))
        ]:
            for attr in attrs:
                java_get_set_is(prd, attr, value)

class TestConsumer:
    def test_enums(self):
        print_enums((PullStatus,ReadOffsetType,ConsumeFromWhere,ConsumeConcurrentlyStatus,ConsumeOrderlyStatus,AllocateMessageQueueStrategyType))
    
    def test_MessageSelector(self):
        text = 'x=1'
        for func,_type in zip((MessageSelector.bySql,MessageSelector.byTag), ExpressionType):
            ms = func(text)
            assert(ms.expression == text)
            assert(ms.expressionType == _type)

    def test_PullResult(self):
        num = 0
        msgFoundList = [MessageExt()]
        nextBeginOffset = num + len(msgFoundList)
        pullStatus = PullStatus.FOUND
        pr = PullResult(pullStatus=pullStatus, nextBeginOffset=nextBeginOffset, minOffset=num, maxOffset=num, msgFoundList=msgFoundList)
        assert(pr.pullStatus == pullStatus)
        assert(pr.nextBeginOffset == nextBeginOffset)
        assert(pr.minOffset == num)
        assert(pr.maxOffset == num)
        assert(len(pr) == len(msgFoundList))

    def test_BaseConsumer(self):
        print('')
        for Class in (PushConsumer,PushConsumer):
            cs = Class()
            for value,attrs in [
                ('x', ('ConsumerGroup',)),
                (MessageModel.BROADCASTING, ('MessageModel',)),
                (True, ('UnitMode',)),
                (1, ('MaxReconsumeTimes',))
            ]:
                for attr in attrs:
                    java_get_set_is(cs, attr, value)
            cs.setAllocateMessageQueueStrategy(cs.allocateMessageQueueStrategy)
            print(cs.offsetStore)

    def test_PullConsumr(self):
        cs = PullConsumer()
        cs.setMessageQueueListener(cs.messageQueueListener)
        for value,attrs in [
            ({'x'}, ('RegisterTopics',)),
            (1, ('BrokerSuspendMaxTimeMillis','ConsumerPullTimeoutMillis','ConsumerTimeoutMillisWhenSuspend'))
            ]:
                for attr in attrs:
                    java_get_set_is(cs, attr, value)
    
    def test_PushConsumer(self):
        cs = PushConsumer()
        for value,attrs in [
            ({'x':'x'}, ('Subscription',)),
            (True, ('PostSubscriptionWhenPull',)),
            (ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, ('ConsumeFromWhere',)),
            ('2000-01-01 00:00:00', ('ConsumeTimestamp',)),
            (1, (
                'ConsumeConcurrentlyMaxSpan','ConsumeMessageBatchMaxSize','ConsumeThreadMax','ConsumeThreadMin','ConsumeTimeout',
                'PullBatchSize','PullInterval','PullThresholdForQueue','PullThresholdForTopic','PullThresholdSizeForQueue','PullThresholdSizeForTopic',
                'AdjustThreadPoolNumsThreshold','SuspendCurrentQueueTimeMillis'
            )),
            ]:
                for attr in attrs:
                    java_get_set_is(cs, attr, value)

class TestIntegration:
    BODY = b'{"name":"Alice", "age":1}'
    TAGS = 'Hello World'

    def test_send(self, namesrv, topic, group):
        pr = Producer(group)
        pr.setNamesrvAddr(namesrv)
        pr.start()
        mqs = pr.fetchPublishMessageQueues(topic)
        msg = Message(topic=topic, body=TestIntegration.BODY, tags=TestIntegration.TAGS)
        for send in (pr.send, pr.sendOneway):
            sr = send(msg)
            assert(sr.sendStatus == SendStatus.SEND_OK)
            for mq in mqs:
                sr = send(msg, mq=mq)
                assert(sr.sendStatus == SendStatus.SEND_OK)
                assert(sr.messageQueue.brokerName==mq.brokerName and sr.messageQueue.queueId==mq.queueId)
        sr = pr.send([msg])
        assert(sr.sendStatus == SendStatus.SEND_OK)
        cb = SendCallback(
            on_success = lambda sr: print(sr.sendStatus),
            on_exception = lambda e: e.printStack()
        )
        sr = pr.send(sr, send_callback=cb)
        assert(sr.sendStatus == SendStatus.SEND_OK)
        pr.shutdown()
    
    def test_pull(self, namesrv, topic, group):
        cs = PullConsumer()
    