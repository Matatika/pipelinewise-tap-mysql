#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-mysql',
      version='1.5.6',
      description='Singer.io tap for extracting data from MySQL & MariaDB - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise-tap-mysql',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_mysql'],
      install_requires=[
          'pendulum==3.2.0',
          'pipelinewise-singer-python==1.*',
          'mysql-replication==0.43',
          'PyMySQL==1.2.*',
          'plpygis==0.6.1',
          'cryptography',
          'orjson',
          'tzlocal==5.3.1',
          'backports-datetime-fromisoformat>=2.0.1; python_version<"3.11"',
      ],
      extras_require={
          'test': [
              'pytest',
              'pytest-cov',
              'ruff',
              'python-dotenv',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-mysql=tap_mysql:main
      ''',
      packages=['tap_mysql', 'tap_mysql.sync_strategies'],
      python_requires=">=3.10"
      )
