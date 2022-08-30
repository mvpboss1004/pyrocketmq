from setuptools import setup, find_packages

setup(
    name='pyrocketmq',
    version='0.1.0',
    description=(
        'A Python RocketMQ 4.0+ API based on JPype'
    ),
    long_description=open('README.rst').read(),
    author='mvpboss1004',
    author_email='mvpboss1004@126.com',
    maintainer='mvpboss1004',
    maintainer_email='mvpboss1004@126.com',
    license='Apache License 2.0',
    packages=find_packages(),
    platforms=['all'],
    url='https://github.com/mvpboss1004/pyrocketmq',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Topic :: Software Development :: Libraries'
    ],
    install_requires=[
        'JPype1>=1',
    ],
)