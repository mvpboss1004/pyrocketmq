PyRocketMQ
==========

1. Introduce
------------

1.1 Why another python RocketMQ API ?
*************************************

Currently the existing apis are too outdated. For example:
- `The official python API<https://github.com/apache/rocketmq-client-python>`_, which you can install via "pip install rocketmq-client-python", is for RocketMQ 2.0 and below, and does not support PullConsumer.
- `An unofficial python API written by messense<https://github.com/messense/rocketmq-python>`_, which you can install via "pip install rocketmq", wraps the same cpp dynamic libraries as the official API, does not support RocketMQ 4.0+ either.
- `The official cpp API<https://github.com/apache/rocketmq-client-cpp>`_ now has v5.0-rc1 version, but does not provide pre-compiled binaries, which means you have to compile the project your self. If you're not familiar bazel tool and cpp class export, it may be a little difficult.

1.2 What's the features of this API ?
*************************************

- It's developed and tested on RocketMQ 4.0+, so it supports many recent features. For example, custom queue selector when producing messages, custom message selector by tag/sql when consuming messages, etc.
- It exposes the java API as it is as possible, via `JPype<https://github.com/jpype-project/jpype>`_. You can directly reference the java doc in most cases.
- The get method of any java class are wrapped as property of the coresponding python class. For example, call "msg.topic" in python is equivalent to call "msg.getTopic()" in java.

2. Installation
---------------

- Download RocketMQ binary release, for example: https://archive.apache.org/dist/rocketmq/4.3.2/rocketmq-all-4.3.2-bin-release.zip.
- Extract it to somewhere as you like, for example: /path/to/rocketmq-all-4.3.2-bin-release
- Install pyrocketmq via pip: "pip install pyrocketmq"
- Make sure the jvm is started **BEFORE** you import and use pyrocketmq, for example:

..  code-block::
    import jpype
    import jpype.imports
    jpype.startJVM(classpath=['/path/to/rocketmq-all-4.3.2-bin-release/lib/*',])
    from pyrocketmq import *
    # do something
    jpype.shutdownJVM()


3. QuickStart
-------------


:Code: `GitHub
 <https://github.com/mvpboss1004/pyrocketmq>`_
:License: `Apache 2 License`_

SPDX-License-Identifier: Apache-2.0