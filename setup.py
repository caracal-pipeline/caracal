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
    'stimela>=1.4.3',
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
    'bokeh',
    'aplpy',
    'pandas',
    'nbconvert',
    'regions',
    'radiopadre-client',
    'jinja2',
]

PACKAGE_NAME = 'caracal'
__version__ = '1.0.0'

setup(name=PACKAGE_NAME,
      version=__version__,
      description="End-to-end data reduction pipeline for radio interferometry data",
      author="Caracal peeps",
      author_email="sphemakh@gmail.com",
      url="https://github.com/ska-sa/caracal",
      packages=[PACKAGE_NAME],
      python_requires='>=3.5',
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
