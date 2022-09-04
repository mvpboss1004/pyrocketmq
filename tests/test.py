import os
import sys
import jpype
import jpype.imports
import pytest

if __name__ == '__main__':
    jpype.startJVM(classpath=os.environ.get('CLASSPATH','').split(','))
    ret = pytest.main(sys.argv[1:])
    jpype.shutdownJVM()
    sys.exit(ret)
