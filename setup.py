#!/usr/bin/env python

from setuptools import setup

setup(name='tap-toggl',
      version='2.0.0',
      description='Singer.io tap for extracting data from the Toggl API',
      author='Stitch',
      url='http://github.com/singer-io/tap-toggl',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_toggl'],
      install_requires=[
          'singer-python==6.1.1',
          'requests==2.32.3',
          'backoff==2.2.1'
      ],
      extras_require={
        "dev": [
            "pylint",
            "ipdb",
        ]
    },
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
