import os
from tempfile import NamedTemporaryFile

with NamedTemporaryFile() as f:
    root = os.path.dirname(f.name)
    fileName = os.path.split(f.name)[-1]
os.environ['ROCKETMQ_LOG_USESLF4J'] = 'TRUE'
os.environ['ROCKETMQ_LOG_FILEMAXINDEX'] = '1'
os.environ['ROCKETMQ_LOG_FILEMAXSIZE'] = '1024'
os.environ['ROCKETMQ_LOG_LEVEL'] = 'WARN'

from pyrocketmq.client.log import basicConfig, LogLevel
args = basicConfig(root=root, fileName=fileName)

import jpype
import jpype.imports
if not jpype.isJVMStarted():
    jpype.startJVM(*args, classpath=os.environ.get('CLASSPATH','').split(','))

from pyrocketmq.client.log.log import getLogger
from pyrocketmq.common.common import Throwable

class TestLog:
    def test_enums(self):
        print('')
        for e in (LogLevel,):
            print(','.join([str(i) for i in e]))
    
    def test_basicConfig(self):
        logger = getLogger()
        print(logger.name)
        logger.debug(f'__{LogLevel.DEBUG.value}__')
        logger.info(f'__{LogLevel.INFO.value}__')
        logger.warn(f'__{LogLevel.WARN.value}__')
        msg = 'helloworld'
        logger.error(f'__{LogLevel.ERROR.value}__', Throwable(msg))
        with open(os.path.join(root, fileName)) as f:
            text = f.read()
        for level in (LogLevel.DEBUG, LogLevel.INFO):
            assert(f'__{level.value}__' not in text)
        for level in (LogLevel.WARN, LogLevel.ERROR):
            assert(f'__{level.value}__' in text)
        assert(msg in text)
