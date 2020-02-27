#!/usr/bin/env python

import os

try:
    from setuptools import setup
except ImportError as e:
    from distutils.core import setup

requirements = [
    'ruamel.yaml>=0.16.6',
    'decorator>=4.4.1',
    'numpy>=1.18.1',
    'stimela @ git+https://github.com/SpheMakh/Stimela',
    'scipy>=1.4.1',
    'pysolr>=3.8.1',
    'progressbar2>=3.47.0',
    'pykwalify>=1.7.0',
    'yamlordereddictloader>=0.4.0',
    'astroquery>=0.4',
    'npyscreen>=4.10.5',
    'ipdb>=0.12.3',
]

# these are only there for diagnostics, make them optional
beta = [
    'python-casacore>=3.2.0',
    'astropy>=3.2.3',
    'matplotlib>=3.0.3',
    'tornado>=4.0.0',
    'jupyter>=1.0.0',
    'aplpy>=2.0.3',
    'pandas>=0.24.2',
    'nbconvert>=5.6.1',
]

PACKAGE_NAME = 'meerkathi'
__version__ = '0.2.0'

setup(name=PACKAGE_NAME,
      version=__version__,
      description="MeerKAT end-to-end data reduction pipeline",
      author="MeerKATHI peeps",
      author_email="sphemakh@gmail.com",
      url="https://github.com/ska-sa/meerkathi",
      packages=[PACKAGE_NAME],
      install_requires=requirements,
      extras_require={
          'beta': beta,
          'testing': 'pytest'
      },
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
