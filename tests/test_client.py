import os
from tempfile import NamedTemporaryFile, TemporaryDirectory

import jpype
import jpype.imports
if not jpype.isJVMStarted():
    jpype.startJVM(classpath=os.environ.get('CLASSPATH','').split(','))

from pyrocketmq.client.client import ClientConfig, QueryResult
from pyrocketmq.client.log import basicConfig, getLogger, LogLevel
from pyrocketmq.common.common import LanguageCode, Throwable
from pyrocketmq.common.message import MessageExt

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

class TestLog:
    def test_enums(self):
        print('')
        for e in (LogLevel,):
            print(','.join([str(i) for i in e]))
    
    def test_basicConfig(self):
        with TemporaryDirectory() as root:
            with NamedTemporaryFile(dir=root.name, delete=False) as fileName:
                os.environ['CLIENT_LOG_USESLF4J'] = 'TRUE'
                os.environ['CLIENT_LOG_MAXINDEX'] = '1'
                os.environ['CLIENT_LOG_FILESIZE'] = '1024'
                os.environ['CLIENT_LOG_LEVEL'] = 'WARN'
                basicConfig(root=root.name, fileName=fileName.name)
                logger = getLogger()
                print(logger.name, root.name, fileName.name)
                logger.debug(f'__{LogLevel.DEBUG.value}__')
                logger.info(f'__{LogLevel.INFO.value}__')
                logger.warn(f'__{LogLevel.WARN.value}__')
                msg = 'helloworld'
                logger.error(f'__{LogLevel.ERROR.value}__', Throwable(msg))
                with open(fileName.name) as f:
                    text = f.read()
                for level in (LogLevel.DEBUG, LogLevel.INFO):
                    assert(f'__{level.value}__' not in text)
                for level in (LogLevel.WARN, LogLevel.ERROR):
                    assert(f'__{level.value}__' in text)
                assert(msg in text)