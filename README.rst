PyRocketMQ
==========

1. Introduce
------------

1.1 Why another python RocketMQ API ?
*************************************

Currently the existing apis are too outdated. For example:

- `The official python API <https://github.com/apache/rocketmq-client-python>`_, which you can install via ``pip install rocketmq-client-python``, is for RocketMQ 2.0 and below, and does not support PullConsumer.
- `An unofficial python API written by messense <https://github.com/messense/rocketmq-python>`_, which you can install via ``pip install rocketmq``, wraps the same cpp dynamic libraries as the official API, does not support RocketMQ 4.0+ either.
- `The official cpp API <https://github.com/apache/rocketmq-client-cpp>`_ now has v5.0-rc1 version, but does not provide pre-compiled binaries, which means you have to compile the project your self. If you're not familiar bazel tool and cpp class export, it may be a little difficult.

1.2 What's the features of this API ?
*************************************

- It's developed and tested on RocketMQ 4.0+, so it supports many recent features. For example, custom queue selector when producing messages, custom message selector by tag/sql when consuming messages, etc.
- It exposes the java API as it is as possible, via `JPype <https://github.com/jpype-project/jpype>`_. You can directly reference the java doc in most cases.
- The get method of any java class are wrapped as property of the coresponding python class. For example, calling ``msg.getTopic()`` in java is equivalent to calling ``msg.topic`` in python.

2. Installation
---------------

- Download RocketMQ binary release, for example: https://archive.apache.org/dist/rocketmq/4.3.2/rocketmq-all-4.3.2-bin-release.zip.
- Extract it to somewhere as you like, for example: /path/to/rocketmq-all-4.3.2-bin-release
- Install pyrocketmq via pip: ``pip install pyrocketmq``
- Make sure the jvm is started **BEFORE** you import and use pyrocketmq, for example:

.. code-block::  python

    import jpype
    import jpype.imports
    jpype.startJVM(classpath=['/path/to/rocketmq-all-4.3.2-bin-release/lib/*',])
    from pyrocketmq import *
    # do something
    jpype.shutdownJVM()


3. QuickStart
-------------

3.1 Producer
************

.. code-block::  python

    import json
    from pyrocketmq.common.message import Message
    from pyrocketmq.client.producer import Producer, SendStatus
    pr = Producer('test_producer')
    pr.setNamesrvAddr('localhost:9876')
    pr.start()
    body = json.dumps({'name':'Alice', 'age':1}).encode('utf-8')
    msg = Message(topic='test_topic', body=body, tags='girl')
    # send, tcp-like, return sendStatus
    sr = pr.send(msg)
    assert(sr.sendStatus == SendStatus.SEND_OK)
    pr.shutdown()

3.2 PullConsumer
****************

.. code-block::  python

    import json
    from pyrocketmq.client.consumer.consumer import PullConsumer, PullStatus
    cs = PullConsumer('test_pull_consumer')
    cs.setNamesrvAddr('localhost:9876')
    topic = 'test_topic'
    cs.start()
    # pull messages from each queue
    mqs = cs.fetchSubscribeMessageQueues(topic)
    for mq in mqs:
        ofs = cs.fetchConsumeOffset(mq, False)
        pr = cs.pull(mq, subExpression='girl', offset=ofs, maxNums=1)
        if pr.pullStatus == PullStatus.FOUND:
            # iterate msg in pull result
            for msg in pr:
                print(json.loads(msg.body))
    cs.shutdown()

3.3 PushConsumer
****************

.. code-block::  python

    import json
    import time
    from typing import List
    from pyrocketmq.client.consumer.listener import ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently
    from pyrocketmq.client.consumer.consumer import MessageSelector, PushConsumer
    from pyrocketmq.common.common import ConsumeFromWhere
    from pyrocketmq.common.message import MessageExt
    
    # subclass MessageListenerConcurrently to write your own consume action
    class MyMessageListenerConcurrently(MessageListenerConcurrently):
        def _consumeMessage(self, msgs:List[MessageExt], context:ConsumeConcurrentlyContext) -> ConsumeConcurrentlyStatus:
            print('Concurrently', context.ackIndex)
            for msg in msgs:
                print(json.loads(msg.body))
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cs = PushConsumer('test_push_consumer')
    cs.setNamesrvAddr('localhost:9876')
    selector = MessageSelector.byTag('girl')
    ml = MyMessageListenerConcurrently()
    cs.registerMessageListener(ml)
    cs.subscribe('test_topic', selector)
    cs.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cs.start()
    time.sleep(5)
    cs.shutdown()
