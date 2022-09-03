import os
import pytest
from .client import *

java_test_func = get_java_test_func()

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
                java_test_func(cc, attr, value)
        cc.changeInstanceNameToPID()
        assert(int(cc.instanceName) == os.getpid())
        cc.resetClientConfig(cc.cloneClientConfig())
        print(cc.buildMQClientId())
