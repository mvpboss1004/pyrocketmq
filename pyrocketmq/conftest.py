import jpype
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