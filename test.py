import sys
import jpype
import jpype.imports
import pytest

if __name__ == '__main__':
    jpype.startJVM(classpath=sys.argv[1].split(','))
    ret = pytest.main(['pyrocketmq']+sys.argv[2:])
    jpype.shutdownJVM()
    sys.exit(ret)
