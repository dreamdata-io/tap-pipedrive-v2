#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-pipedrive-v2",
    version="0.0.1",
    description="tap to fetch pipedrive API",
    py_modules=["tap_pipedrive_v2"],
    install_requires=[
        "backoff==1.8.0",
        "requests==2.22.0",
        "singer-python==5.8.1",
        "python-dateutil==2.8.0",
    ],
    entry_points="""
          [console_scripts]
          tap-pipedrive-v2=tap_pipedrive:main
      """,
    packages=["tap_pipedrive"],
)
