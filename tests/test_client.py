import os

from pyrocketmq import *
from .conftest import java_get_set_is

class TestClient:
    def test_QueryResult(self):
        num = 1000
        result = QueryResult(indexLastUpdateTimestamp=num, messageList=[MessageExt()])
        assert(result.indexLastUpdateTimestamp == num)
        assert(len(result) == 1)

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