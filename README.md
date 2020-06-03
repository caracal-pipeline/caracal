# Full documentation
https://caracal.readthedocs.io, which includes the Install & Run instructions copied below, and much more.

# Installation & Run

## Usage and publication policy

When using CARACal please be aware of and adhere to the [CARACal publication policy](https://docs.google.com/document/d/12LjHM_e1G4kWRfCLcz0GgM8rlXOny23vVdcriiA8ayU).

## Requirements
- [Python](https://www.python.org) 3.5 or higher.
- Container technology of your choice. It can be one of the following:
  - [Docker](https://www.docker.com);
  - [Singularity](https://github.com/sylabs/singularity) > 3.5 (nearly all functionality available for > 2.6.0-dist, see [here](https://github.com/caracal-pipeline/caracal/issues/1154) for further information)
  - [Podman](https://podman.io) **(currently not fully supported)**.

## Manual installation

We strongly recommend and describe an installation using a Python3 virtual environment. Only try outside a virtual environment if you know what you are doing. Any name as ``${name}`` occurring in the description below can be chosen arbitrarily. If it symbolises directories or files, those directories or files should exist and the user should have write acccess.

Choose the name of the virtual environment `${caracal-venv}`. Then:

```
python3 -m venv ${caracal-venv}
# OR, if the command above does not work
#virtualenv -p python3 ${caracal-venv}

source ${caracal-venv}/bin/activate
pip install -U pip setuptools wheel

# CARACal stable release
pip install -U caracal
# OR CARACal developer version
#pip install -U git+https://github.com/ska-sa/caracal.git#egg=caracal
```
*(Ignore any error messages concerning pyregion.)*

In case you are *not* carrying out a fresh installation, remove earlier Stimela images with:

```
stimela clean -ac
```

<!-- Then, if using [Docker](https://www.docker.com):

```
stimela pull
```
-->

If using [Singularity](https://github.com/sylabs/singularity), choose a pull folder `${singularity_pull_folder}`, where the [Singularity](https://github.com/sylabs/singularity) images are stored and define an environment variable by adding this in the rc file of your shell (e.g. .bashrc):

```
export SINGULARITY_PULLFOLDER=${WORKSPACE_ROOT}/singularity_images
```
and run:

``` 
stimela pull -s
```

If using [Podman](https://podman.io) (currently not fully supported):

```
stimela pull -p
``` 

## Installation with the caratekit.sh script

Download the installation script [caratekit.sh](https://github.com/caracal-pipeline/caracal/blob/master/caratekit.sh) . Choose the parent directory `${workspace}` and the name of the CARACal directory `${caracal_dir}`. Any name as ``${name}`` occurring in the description below can be chosen arbitrarily. If it symbolises directories or files, those directories or files should exist and the user should have write acccess.

If using [Docker](https://www.docker.com):

```
caratekit.sh -ws ${workspace} -cr -di -ct ${caracal_dir} -rp install -f -kh
```

If using [Singularity](https://github.com/sylabs/singularity):

```
caratekit.sh -ws ${workspace} -cr -si -ct ${caracal_testdir} -rp install -f -kh
```

## Run

If you installed CARACal manually, activate the virtual environment with:
```
source ${caracal-venv}/bin/activate
```

If you installed CARACal with the caratekit.sh script, activate the virtual environment with:
```
source ${workspace}/${caracal_dir}/caracal_venv/bin/activate
```

If using [Docker](https://www.docker.com) run CARACal with:

```
caracal -c ${your-configuration-file}
```

If using [Singularity](https://github.com/sylabs/singularity) run CARACal with:

```
caracal -ct singularity -c ${your-configuration-file}
```

For more detailed installation instructions, trouble-shooting tips and a full user manual please see [caracal.readthedocs.io](https://caracal.readthedocs.io).


## Known and new issues

We encourage users who experience problems installing or running CARACal to check for known issues or open a new issue at
our [GitHub issues page](https://github.com/caracal-pipeline/caracal/issues). When opening a new issue, please include your installation type (e.g., Docker, Singularity), software version (both CARACal and Stimela), CARACal configuration file, and CARACal log files.
