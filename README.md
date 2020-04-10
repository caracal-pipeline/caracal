# Documentation
https://caracal.readthedocs.io

# Download & Install

## Usage and publication policy

When using CARACal please be aware of and adhere to the [CARACal publication policy](https://docs.google.com/document/d/12LjHM_e1G4kWRfCLcz0GgM8rlXOny23vVdcriiA8ayU).

## Software requirements
Most dependencies are taken care of by using [pip](https://pypi.org/project/pip/) and [PiPy](https://pypi.org) to control Python dependencies and [Stimela](https://github.com/ratt-ru/Stimela/tree/master/stimela) as a platform-independent scripting framework. This leaves a small number of known dependencies (currently only one) that need to be installed prior to using CARACal:
- [Python](https://www.python.org/) 3.6 or higher
- [Singularity](https://github.com/sylabs/singularity) > 2.6.0-dist is required only if [Singularity](https://github.com/sylabs/singularity) is chosen as containerization technology to run [Stimela](https://github.com/ratt-ru/Stimela/tree/master/stimela) with (no known [Docker](https://www.docker.com/) dependencies).

## Manual installation
(Placeholders for names of directories, names, and files, which can be chosen by the user are highligted in shell-style: ``${name}`` )

We recommend and describe an installation using a virtual environment created with [Virtualenv](https://virtualenv.pypa.io/en/latest/). This is not a requirement, but strongly recommended.
### Virtualenv
Make sure that virtualenv is installed and updated on your computer. E.g. on Ubuntu, do:
```
$ sudo apt-get update
$ sudo apt-get install python3-pip
```
Create a virtual environment called ``${cvenv}``:
```  
$ python3 -m venv ${cvenv}  
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
if you are using csh or tcsh . From now on all python installations using pip will be contained inside ${cvenv} and your global [Python](https://www.python.org/) is not affected. Install and/or upgrade the installation tools inside your virtualenv:
```
$ pip install -U pip setuptools wheel
```
### CARACal
#### Current development branch
*Warning: the current development branch obviously contains the most recent developments but it might contain bugs.*

Select a target directory ``${motherdir}`` (e.g. ``/home/caine/software`` or ``/home/larusso/datareductions``) and clone CARACal into that directory, and install via pip:
```
$ cd ${motherdir}
$ git clone https://github.com/ska-sa/caracal
$ pip install -U --force-reinstall ./caracal
```
#### Via pip
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
