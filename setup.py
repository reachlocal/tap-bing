#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-bing",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["bing"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
        "bingads==13.0.7",
        "futures3==1.0.0",
        "retrying==1.3.3"
    ],
    entry_points="""
    [console_scripts]
    tap-bing=tap_bing:main
    """,
    packages=["tap_bing"],
    package_data = {
        "schemas": ["tap_bing/schemas/*.json"]
    },
    include_package_data=True,
)
