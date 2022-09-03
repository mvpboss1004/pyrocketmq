from typing import Any, Callable
import pytest

def pytest_addoption(parser):
    parser.addoption('--namesrv', dest='namesrv', help='RocketMQ name server')
    parser.addoption('--topic', dest='topic', help='RocketMQ topic', default='test')
    parser.addoption('--group', dest='group', help='RocketMQ producer/consumer group', default='test')

@pytest.fixture
def namesrv(request):
    return request.config.getoption('--namesrv')

@pytest.fixture
def topic(request):
    return request.config.getoption('--topic')

@pytest.fixture
def group(request):
    return request.config.getoption('--group')

def java_get_set_is(obj:Any, attr:str, value:Any):
    getattr(obj, 'set'+attr)(value)
    if isinstance(value, bool):
        assert(getattr(obj, 'is'+attr)() == value)
    else:
        assert(getattr(obj, attr[0].lower()+attr[1:]) == value)

@pytest.fixture
def java_test_func() -> Callable:
    return java_get_set_is
