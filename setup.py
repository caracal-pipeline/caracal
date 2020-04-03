#!/usr/bin/env python

import os

try:
    from setuptools import setup
except ImportError as e:
    from distutils.core import setup

requirements = [
    'ruamel.yaml',
    'decorator',
    'numpy',
    'stimela==1.2.5',
    'scipy',
    'pysolr',
    'progressbar2',
    'pykwalify',
    'yamlordereddictloader',
    'astroquery',
    'npyscreen',
    'ipdb',
    'astropy',
    'matplotlib',
    'aplpy',
    'pandas',
    'nbconvert',
    'regions',
]

PACKAGE_NAME = 'meerkathi'
__version__ = '0.3.0'

setup(name=PACKAGE_NAME,
      version=__version__,
      description="End-to-end data reduction pipeline for radio interferometry data",
      author="MeerKATHI peeps",
      author_email="sphemakh@gmail.com",
      url="https://github.com/ska-sa/meerkathi",
      packages=[PACKAGE_NAME],
      install_requires=requirements,
      include_package_data=True,
      # package_data - any binary or meta data files should go into MANIFEST.in
      scripts=["bin/" + j for j in os.listdir("bin")],
      license=["GNU GPL v2"],
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Intended Audience :: Science/Research",
          "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
          "Operating System :: POSIX :: Linux",
          "Programming Language :: Python",
          "Topic :: Scientific/Engineering :: Astronomy"
      ]
      )
