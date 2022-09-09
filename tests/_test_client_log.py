import os
from xml.etree.ElementTree import ElementTree

import jpype
import jpype.imports
conf = 'logback.xml'
args = [
    '-Dlogback.configurationFile=' + conf,
    '-Drocketmq.client.logUseSlf4j=TRUE'
]
jpype.startJVM(*args, classpath=os.environ.get('CLASSPATH','').split(','))

from pyrocketmq.client.log import getLogger, LogLevel

class TestLog:
    def test_enums(self):
        print('')
        for e in (LogLevel,):
            print(','.join([str(i) for i in e]))
    
    def test_log(self):
        logger = getLogger()
        for level in LogLevel:
            getattr(logger, level.value)(f'__{level.value}__')
        
        tree = ElementTree(file=conf)
        log_level = LogLevel.LEVEL(
            tree.find(f"./logger[@name='{logger.name}']/level").attrib['value']
        )
        levels = list(LogLevel)
        for cut in range(len(levels)):
            if log_level == levels[cut]:
                break

        log_path = tree.find(f"./appender[@name='{logger.name}Appender']/file").text
        with open(log_path) as f:
            text = f.read()
        for level in levels[:cut]:
            assert(f'__{level.value}__' not in text)
        for level in levels[cut:]:
            assert(f'__{level.value}__' in text)
