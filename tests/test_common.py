import os
from datetime import datetime

import jpype
import jpype.imports
if not jpype.isJVMStarted():
    jpype.startJVM(classpath=os.environ.get('CLASSPATH','').split(','))

from java.lang import Exception as JException
from java.net import InetSocketAddress
from org.apache.rocketmq.common.message import Message as JMessage

from pyrocketmq.common.common import ExpressionType, LanguageCode, MessageModel, Throwable
from pyrocketmq.common.message import socket2tuple, Message, MessageBatch, MessageExt, MessageQueue

from .conftest import java_get_set_is


class TestCommon:
    def test_enums(self):
        print('')
        for e in (ExpressionType,LanguageCode,MessageModel):
            print(','.join([str(i) for i in e]))
        for lc,code in zip(LanguageCode, range(len(LanguageCode))):
            assert(lc == LanguageCode.valueOf(code))

    def test_Throwable(self):
        msg = 'are'
        e = Throwable(JException(msg))
        assert(e.message==msg)
        e = Throwable(message='you', cause=Throwable(message=msg))
        e = Throwable(cause=e)
        print(e.printStackTrace())

class TestMessage:
    def test_socket2tuple(self):
        ip = '127.0.0.1'
        port = 9876
        assert(socket2tuple(InetSocketAddress(ip,port)) == (ip,port))

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
