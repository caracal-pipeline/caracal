The **Containerized Automated Radio Astronomy Calibration (CARACal)** pipeline is a Python-based script to reduce radiointerferometric data in the cm- to dm-wavelength range in an automatized fashion, reducing user-intervention to a minimum (ideally requiring only a configuration), and at the same time relying on state-of-the-arts data reduction software and  data reduciton techniques. It makes use of [Stimela](https://github.com/ratt-ru/Stimela), a platform-independent radio astronomy scripting environment, providing pre-fabricated containerized software packages. [Stimela](https://github.com/ratt-ru/Stimela) itself relies on [Kern](https://kernsuite.info/), a suite of [Ubuntu](https://ubuntu.com/) radio astronomy packages.
# Full documentation
https://caracal.readthedocs.io

# Download & Install

## Usage and publication policy

When using CARACal please be aware of and adhere to the [CARACal publication policy](https://docs.google.com/document/d/12LjHM_e1G4kWRfCLcz0GgM8rlXOny23vVdcriiA8ayU).

## Software requirements and supported platforms
Most dependencies are taken care of by using [pip](https://pypi.org/project/pip/) and [PiPy](https://pypi.org) to control Python dependencies and [Stimela](https://github.com/ratt-ru/Stimela/tree/master/stimela) as a platform-independent scripting framework. This leaves a small number of known dependencies (currently only one) that need to be installed prior to using CARACal:
- [Python](https://www.python.org/) 3.6 or higher
- [Singularity](https://github.com/sylabs/singularity) > 2.6.0-dist is required only if [Singularity](https://github.com/sylabs/singularity) is chosen as containerization technology to run [Stimela](https://github.com/ratt-ru/Stimela/tree/master/stimela) with (no known [Docker](https://www.docker.com/) dependencies).

CARACal fully supports [Docker](https://www.docker.com/) and [Singularity](https://github.com/sylabs/singularity). It is also possible to use it in combination with [Podman](https://podman.io/).

## Installing and running CARACal

Shell-style is used to indicate names and paths of directories and files which can be chosen by the user: ``${name}``.

### Installing and running manually
We recommend and describe an installation using a virtual environment created with [Virtualenv](https://virtualenv.pypa.io/en/latest/). This is not a requirement, but strongly recommended.
The user chooses:
- the name (including path) ${caracal-venv} of the virtualenv
- the location ``${singularity_pull_folder}`` of a [Singularity](https://github.com/sylabs/singularity) pull-folder, where [Singularity](https://github.com/sylabs/singularity) images are stored (not required when using only [Docker](https://www.docker.com/) or [Podman](https://podman.io/), the latter currently not being fully supported).
#### Installation
The latest CARACal release can be obtained by typing:
```
$ python3 -m venv ${caracal-venv}  
$ source ${caracal-venv}/bin/activate
$ pip install -U pip setuptools wheel
$ pip install -U caracal
```
Using [Docker](https://www.docker.com/):
```
$ stimela build
```
Using [Singularity](https://github.com/sylabs/singularity) (choose a [Singularity](https://github.com/sylabs/singularity) pull-folder ``${singularity_pull_folder}``):  

```  
$ stimela pull --singularity --pull-folder ${singularity_pull_folder}
```

Using [Podman](https://podman.io/) (currently not fully supported):
```
$ stimela pull -p
```
Please see [Installing a Python 3 virtualenv](#Installing-a-Python-3-virtualenv) for more information on virtualenv.

##### Current development branch
*Warning: the current development branch obviously contains the most recent developments but it might contain bugs.*
It is also possible not to install the release version but instead the current development version of CARACal. 

*Warning: the current development branch obviously contains the most recent developments but it might contain bugs. We take no responsibility for development versions of CARACal.*

Do do so, replace above pip installation of CARACal 
```
pip install -U --force-reinstall caracal
```
with:
```
$ pip install -U --force-reinstall git+https://github.com/ska-sa/caracal.git#egg=caracal
```

#### Running CARACal
The user needs to know the name ``${caracal-venv}`` of the virtualenv used during the installation and the name ``${singularity_pull_folder}`` of the [Singularity](https://github.com/sylabs/singularity) pull folder used when installing CARACal. For the most basic usage, the user generally chooses:
- the name ${my_caracal_run_dir} (and hence the location) of a folder in which the data reduction takes place
- a template configuration file ``${template_config}`` to use for the data reduction, as can be downloaded (and re-named to your choice) from the [sample_configurations](https://github.com/ska-sa/caracal/tree/master/caracal/sample_configurations) folder in the CARACal repository. To start, we recommend to use [``minimal_config.yml``](https://github.com/ska-sa/caracal/blob/master/caracal/sample_configurations/minimalConfig.yml).
- a name for the final configuration file ``${my_config_file}`` to use.

An example run then looks like:
```
$ mkdir ${my_caracal_run_dir}
$ cd ${my_caracal_run_dir}
```
Copy ``${template_config}`` into ``${my_caracal_run_dir}``, then re-name (this is strictly not necessary):
```
$ mv ${template_config} ${my_config_file}
```
Create ``msdir`` inside ``${my_caracal_run_dir}`` 
```
$ mkdir ${my_caracal_run_dir}/msdir
```
and move/copy your raw measurement set files into that directory. Edit your ``${my_config_file}`` to link to those files. If the names of your measurement sets are ``a.ms b.ms c.ms``, then edit the line
```
dataid=[] -> dataid=['a','b','c']
```
in ${my_config_file}. Do analogously for different names of your measurement sets.

Finally, run CARACal:
Using [Docker](https://www.docker.com/):
```
$ caracal -c ${my_config_file}
```
Using [Singularity](https://github.com/sylabs/singularity) (using the [Singularity](https://github.com/sylabs/singularity) pull-folder ``${singularity_pull_folder}`` created during the installation):  

```  
$ caracal -c ${my_config_file} --container-tech singularity -sid ${singularity_pull_folder}
```
Using podman (currently not fully supported):
```
$ caracal -c ${my_config_file} --container-tech podman
```

# Tips
## Installing a Python 3 virtualenv
To use CARACal it is essential to install a virtualenv with Python >= 3.5.
On Ubuntu, do:
```
$ sudo apt-get update
$ sudo apt-get install python3-pip
```
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
##### CARACal
###### Current development branch
*Warning: the current development branch obviously contains the most recent developments but it might contain bugs.*

Select a target directory ``${motherdir}`` (e.g. ``/home/caine/software`` or ``/home/larusso/datareductions``) and clone CARACal into that directory, and install via pip:
```
$ cd ${motherdir}
$ git clone https://github.com/ska-sa/caracal
$ pip install -U --force-reinstall ./caracal
```
###### Via pip
```
$ pip install -U --force-reinstall caracal
```


## Installation using caratekit.sh

To newly install CARACal using caratekit, download [caratekit.sh](https://github.com/ska-sa/caracal/raw/master/caracal/utils/caratekit.sh) and make it executable (alternatively clone caracal and find ``caratekit.sh`` ):
```
$ chmod u+x caratekit.sh 
```
Then select a parent directory ``$parent`` (e.g. ``/home/pete``) to the installation and a name $installation_name (e.g. ``caracal``) for the installation. Then do:
```
caratekit.sh -ws $parent -ct  $installation_name -kh -di -op -ur
```
for a docker installation and:
```
caratekit.sh -ws $parent -ct  $installation_name -kh -si -op -ur -sr
```
for a singularity installation.

Each time you want to run caracal using the docker installation do:

``$ source $parent/$installation_name/caracal_venv/bin/activate`` (bash)
or
``> source $parent/$installation_name/caracal_venv/bin/activate.csh`` (csh or tcsh)
then create a data reduction directory $datared and put the configuration file $config.yml into that directory. Also create a directory msdir in $datared and put your raw measurement set data sets therein. Then

``$ cd $datared``

``$ caracal -c $config.yml`` (Docker)

``$ caracal -c $config.yml --container-tech singularity -sid $parent/$installation_name/stimela_singularity`` (Singularity)

For details on caratekit.sh type ``$ caratekit.sh -h`` or ``$ caratekit.sh -v``.

### On Linux

0. Clone this repository
Use https and your github credentials, then go to the pipeline folder 'caracal'.
```
$ git clone https://github.com/ska-sa/caracal.git
$ cd caracal
```
1. Start and activate virtual environment outside the caracal directory
```
$ cd ..
$ virtualenv -p python3 caracal-venv 
$ source caracal-venv/bin/activate
```
2. If working from master branch it may be necessary to install bleeding edge fixes from upstream dependencies. Please install the requirements.txt requirements:
```
$ pip install -U -r <absolute path to caracal folder>/requirements.txt
```
3. Install caracal
```
$ pip install <absolute path to caracal folder>"[extra_diagnostics]"
```
If the requirements cannot be installed on your system you may omit [extra_diagnostics]. This will disable report rendering.

4. Pull and/or build stimela images

  - **Podman**[Recommended]
    ```
    $ stimela pull -p
    ```
    
  - **Singularity**[Recommended]
    Requires versions >= 2.6.0-dist
    ```
    $ stimela pull --singularity --pull-folder <folder to store stimela singularity images>
    ```

  - **uDocker**[Recommended]<note: no python3 support>
    ```
    $ stimela pull
    ```
    
  - **Docker**
    ```
    $ stimela pull -d
    $ stimela build -nc
    ```

5. run caracal

  - **Podman**[Recommended]
    ``` $ caracal -c path_to_configuration_file --container-tech podman```

  - **Singularity**[Recommended]
    ```$ caracal -c path_to_configuration_file --container-tech singularity -sid <folder where singularity images are stored>```

  - **uDocker**[no python3 support]
    ``` $ caracal -c path_to_configuration_file --container-tech udocker```

  - **Docker**
    ```$ caracal -c< path to configuration file>```

### Troubleshooting

- **Stimela cache file**
When re-building/pullng/updating stimela (any stimela call above), sometimes problems will arise with the cache file of stimela, whose standard location is
```
~/.stimela
```
If you run into unexplicable errors when installing a stimela version, including a failed update (possibly resulting in a repeating error when running CARACal), do:
```
> rm ~/.stimela/*
> stimela ...
```

before re-building. If that does not work, re-building the dependencies might help.
```
> pip install --upgrade --force-reinstall -r <absolute path to caracal folder>/requirements.txt
> rm ~/.stimela/*
> stimela ...
```
### On Mac

0. create a python environment

`$ conda create env --name meer_venv`

1. activate environment

`$ source activate meer_venv`

2. clone `caracal`
```
$ git clone https://github.com/ska-sa/caracal.git
$ cd caracal
```
3. Start and activate virtual environment
```
$ virtualenv caracal-venv
$ source caracal-venv/bin/activate
$ pip install pip wheel setuptools -U
```
4. If working from master branch it may be necessary to install bleeding edge fixes from upstream dependencies. Please install the requirements.txt requirements:
```
$ pip install -U -r <absolute path to caracal folder>/requirements.txt
```
5. Install caracal
```
$ pip install <absolute path to caracal folder>
$ export PYTHONPATH='' # Ensure that you use venv Python
```

6. Pull and/or build stimela images
  - **uDocker**[Recommended]
    ```
    $ stimela pull
    ```
    
  - **Singularity**[Recommended]
    ```
    $ stimela pull --singularity --pull-folder <folder to store stimela singularity images>
    ```

  - **Docker**
    ```
    $ stimela pull
    $ stimela build
    ```

7. run caracal
  - **uDocker**[Recommended]
    ```$ caracal -c path_to_configuration_file --container-tech udocker```

  - **Singularity**[Recommended]
    ```$ caracal -c path_to_configuration_file --container-tech singularity -sid <folder where singularity images are stored>```
      
  - **Docker**
    ```$ caracal -c< path to configuration file>```
    
    
## Running the pipeline
1. Activate the CARACal virtual environment
2. Make sure that the data all the required data is present
3. Check if your installation is set up properly:
```
$ caracal --help
```
4. GUI config editor can be started with (you need an X-session and Mozilla Firefox >= 56.0):
```
$ caracal -ce
```
5. An existing configuration file can be edited as from the gui (ie. don't specify -c):
```
$ caracal
```
6. (Alternatively) dump a copy of the default configuration file to the current directory:
```
$ caracal -gd <configuration file>
```
7. Run the pipeline
```
$ caracal -c <configuration file>
```
8. View generated reports (produced even in event of partial pipeline failure)
```
$ caracal -c <configuration file> -rv
```
## Help and descriptions
For Stimela see https://github.com/SpheMakh/Stimela and wiki therein.
For this pipeline and MeerKAT specific notes see [wiki](https://github.com/ska-sa/caracal/wiki) of this repository.

## Singularity specific issues
If you get a "Too many open files" error when running WSClean increase the system-wide max number of open files with "ulimit -n <max_number>". You can also add this command to the venv/bin/activate script so you don't have to do    this manually every time.
