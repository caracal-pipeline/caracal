#!/usr/bin/env python

import os

try:
    from setuptools import setup
except ImportError as e:
    from distutils.core import setup

requirements = [
    'ruamel.yaml',
    'decorator',
    'numpy>=1.14',
    'stimela>=1.6.5',
    'python-casacore',
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
    'regions',
    'pandas',
    'nbconvert',
    'radiopadre-client>=1.1',
    'jinja2',
    'psutil',
]

PACKAGE_NAME = 'caracal'
__version__ = '1.0.4'

setup(name=PACKAGE_NAME,
      version=__version__,
      description="Development Status :: 5 - Production/Stable",
      author="The Caracal Team",
      author_email="caracal-info@googlegroups.com",
      url="https://github.com/caracal-pipeline/caracal",
      packages=[PACKAGE_NAME],
      python_requires='>=3.6',
      install_requires=requirements,
      include_package_data=True,
      # package_data - any binary or meta data files should go into MANIFEST.in
      scripts=["bin/" + j for j in os.listdir("bin")],
      license=["GNU GPL v2"],
      classifiers=[
          "Development Status :: 5 - Production/Stable",
          "Intended Audience :: Science/Research",
          "License :: OSI Approved :: GNU General Public License v2 (GPLv2)",
          "Operating System :: POSIX :: Linux",
          "Programming Language :: Python",
          "Topic :: Scientific/Engineering :: Astronomy"
      ]
      )
