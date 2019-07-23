#!/usr/bin/env python

import os

try:
  from setuptools import setup
except ImportError as e:
  from distutils.core import setup

requirements = [
'ruamel.yaml>=0.15.22',
'numpy>=1.15.4',
'stimela>=1.0.1',
'scipy>=0.19.1',
'pysolr>=3.4.0',
'progressbar2>=3.11.0',
'pykwalify>=1.6.0',
'yamlordereddictloader',
'astroquery>0.3.8',
'npyscreen>=4.10.5',
'ipdb>=0.11',
]

# these are only there for diagnostics, make them optional
extra_diagnostics = [
    'python-casacore>=2.2.0',
    'astropy<3.1.2',
    'matplotlib>=2.1.0',
    'tornado>=4.0.0,<5.0.0',
    'jupyter>=1.0.0',
    'aplpy>=1.1.1',
    'pandas>=0.24.0',
    'nbconvert>=5.3.1',
]

PACKAGE_NAME = 'meerkathi'
__version__ = '0.2.0'

setup(name = PACKAGE_NAME,
    version = __version__,
    description = "MeerKAT end-to-end data reduction pipeline",
    author = "MeerKATHI peeps",
    author_email = "sphemakh@gmail.com",
    url = "https://github.com/ska-sa/meerkathi",
    packages=[PACKAGE_NAME], 
    install_requires = requirements,
    extras_require = {
        'extra_diagnostics': extra_diagnostics
    },
    include_package_data = True,
    ##package_data - any binary or meta data files should go into MANIFEST.in
    scripts = ["bin/" + j for j in os.listdir("bin")],
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
