#!/usr/bin/env python

from setuptools import setup

setup(name='tap-toggl',
      version='0.0.4',
      description='Singer.io tap for extracting data from the Toggl API',
      author='Stitch',
      url='http://github.com/singer-io/tap-toggl',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_toggl'],
      install_requires=[
          'singer-python==5.1.5',
          'requests==2.20.0',
          'backoff==1.3.2'
      ],
      entry_points='''
          [console_scripts]
          tap-toggl=tap_toggl:main
      ''',
      packages=['tap_toggl'],
      package_data = {
          "schemas": ["tap_toggl/schemas/*.json"]
      },
      include_package_data=True,
)
