#!/usr/bin/env python

import os

try:
  from setuptools import setup
except ImportError as e:
  from distutils.core import setup

from meerkathi_misc import version
requirements = []

with open('{0:s}/requirements.txt'.format(os.path.dirname(__file__))) as rstd:
    for line in rstd.readlines():
        requirements.append(line.strip())

setup(name = "meerkathi",
    version = version.version,
    description = "MeerKAT end-to-end data reduction pipeline for spectral line data",
    author = "MeerKATHI peeps",
    author_email = "sphemakh@gmail.com",
    url = "https://github.com/sphemakh/meerkathi",
    packages = ["meerkathi","meerkathi_misc", "meerkathi/workers"],
    package_data = { "meerkathi" : ['default-config.yml']},
    install_requires = requirements,
    scripts = ["bin/" + i for i in os.listdir("bin")],
    classifiers = [],
     )
