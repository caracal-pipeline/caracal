# Full documentation
https://caracal.readthedocs.io, which includes the Download & Install instructions copied below, and much more.

# Usage and publication policy

When using CARACal please be aware of and adhere to the [CARACal publication policy](https://docs.google.com/document/d/12LjHM_e1G4kWRfCLcz0GgM8rlXOny23vVdcriiA8ayU).

# Download & Install

## Requirements
- [Python](https://www.python.org) 3.5 or higher.
- Container technology of your choice. It can be one of the following:
  - [Docker](https://www.docker.com);
  - [Singularity](https://github.com/sylabs/singularity) > 2.6.0-dist;
  - [Podman](https://podman.io) **(currently not fully supported)**.

## Installation

### Manual installation

We strongly recommend and describe an installation using a Python3 virtual environment (see [Installing a Python 3 virtualenv](#Installing-a-Python-3-virtualenv) for more information on this). Try outside a virtual environment only if you know what you are doing.

Choose the name `${caracal-venv}` of the virtual environment. Then:

```
python3 -m venv ${caracal-venv} 
source ${caracal-venv}/bin/activate
pip install -U pip setuptools wheel
pip install -U git+https://github.com/ska-sa/caracal.git#egg=caracal
# pip install -U caracal # Available soon, once Caracal's first release is out
```

If using [Docker](https://www.docker.com):
```
stimela build
```

If using [Singularity](https://github.com/sylabs/singularity), choose a pull folder `${singularity_pull_folder}`, where the [Singularity](https://github.com/sylabs/singularity) images are stored:

```  
stimela pull --singularity --pull-folder ${singularity_pull_folder}
```

If using [Podman](https://podman.io) (currently not fully supported):
```
$ stimela pull -p
``` 

### Installation with the caratekit.sh script

Download the installation script https://github.com/ska-sa/caracal/blob/master/caratekit.sh . Choose the parent directory `${workspace}` and the name of the Caracal directory `${caracal_dir}`.

If using [Docker](https://www.docker.com):

```
 caratekit.sh -ws ${workspace} -cr -di -ct ${caracal_dir} -rp install -f
```

If using [Singularity](https://github.com/sylabs/singularity):

```
$ caratekit.sh -ws ${workspace} -cr -si -ct ${caracal_testdir} -rp install -f
```

## Tips and Troubleshooting

### Installing a Python 3 virtualenv
To use CARACal it is recommended to install a virtualenv with Python >= 3.5.
On Ubuntu, do:
```
$ sudo apt-get update
$ sudo apt-get install python3-pip
```
For Mac OSX, find installation instructions e.g. [here](https://vgkits.org/blog/pip3-macos-howto/).

Create a virtual environment called ``${caracal-venv}``:
```  
$ python3 -m venv ${caracal-venv}  
```
E.g.:
```  
$ python3 -m venv /home/myagi/vs/caracal_venv  
```
If this does not work on your machine, try:
```
$ virtualenv -p python3 ${cvenv}
```
Activate and upgrade the ``${cvenv}``:
```
$ source ${cvenv}/bin/activate
```
if you are using bash or sh and
```
> source ${cvenv}/bin/activate.csh
```
if you are using csh or tcsh . 
Check if a Python > 3.5 is installed inside the virtualenv:
```
(caracal-venv)$ python --version
```
From now on all python installations using pip will be contained inside ${caracal-venv} and your global [Python](https://www.python.org/) is not affected. Install and/or upgrade the installation tools inside your virtualenv:
```
$ pip install -U pip setuptools wheel
```

### Stimela cache file
When re-building/pullng/updating stimela (any stimela call above), sometimes problems will arise with the cache file of stimela, whose standard location is
```
~/.stimela
```
If you run into unexplicable errors when installing a stimela version, including a failed update (possibly resulting in a repeating error when running CARACal), do:
```
$ rm ~/.stimela/*
$ stimela ...
```

before re-building. If that does not work, re-building the dependencies might help.
```
> pip install --upgrade --force-reinstall caracal
> rm ~/.stimela/*
> stimela ...
```
### Singularity-specific issues
If you get a "Too many open files" error when running WSClean increase the system-wide max number of open files with "ulimit -n <max_number>". You can also add this command to the venv/bin/activate script so you don't have to do    this manually every time.

