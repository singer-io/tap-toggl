#!/usr/bin/env python

from setuptools import setup

setup(name='tap-toggl',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Toggl API',
      author='lambtron',
      author_email="andyjiang@gmail.com",
      url='https://andyjiang.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_toggl'],
      install_requires=[
          'singer-python==5.1.5',
          'requests==2.20.0'
      ],
      entry_points='''
          [console_scripts]
          tap-toggl=tap_toggl:main
      ''',
      packages=['tap_toggl'],
      include_package_data=True,
)
