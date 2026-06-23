#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-toggl',
      version='2.2.0',
      description='Singer.io tap for extracting data from the Toggl API',
      author='Stitch',
      url='http://github.com/singer-io/tap-toggl',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_toggl'],
      install_requires=[
          'singer-python==6.8.0',
          'requests==2.34.2',
          'backoff==2.2.1'
      ],
      extras_require={
        "dev": [
            "pylint",
            "ipdb",
            "pytest",
            "coverage"
        ]
    },
      entry_points='''
          [console_scripts]
          tap-toggl=tap_toggl:main
      ''',
      packages=find_packages(),
      package_data = {
          "schemas": ["tap_toggl/schemas/*.json"]
      },
      include_package_data=True,
)
