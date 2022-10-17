#!/usr/bin/env python3

import os

try:
    from setuptools import setup
except ImportError as e:
    from distutils.core import setup


PACKAGE_NAME = 'caracal'
__version__ = '1.1.0'
build_root = os.path.dirname(__file__)


def readme():
    """Get readme content for package long description"""
    with open(os.path.join(build_root, 'README.md')) as f:
        return f.read()


def requirements():
    """Get package requirements"""
    with open(os.path.join(build_root, 'requirements.txt')) as f:
        return [pname.strip() for pname in f.readlines()]


setup(name=PACKAGE_NAME,
      version=__version__,
      author="The Caracal Team",
      author_email="caracal-info@googlegroups.com",
      description="A pipeline for radio interferometry data reduction",
      long_description=readme(),
      long_description_content_type='text/markdown',
      url="https://github.com/caracal-pipeline/caracal",
      packages=[PACKAGE_NAME],
      python_requires='>=3.6',
      install_requires=requirements(),
      extras_require=dict(astroquery=["astroquery"]),
      include_package_data=True,
      # package_data - any binary or meta data files should go into MANIFEST.in
      scripts=["bin/" + j for j in os.listdir("bin")],
      license="GNU GPL v2",
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "Intended Audience :: Science/Research",
          "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
          "Operating System :: POSIX :: Linux",
          "Programming Language :: Python",
          "Topic :: Scientific/Engineering :: Astronomy"
      ]
      )
