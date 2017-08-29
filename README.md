## Install
0. Clone this repository
Use https and your github credentials, then go to the pipeline folder 'meerkathi'.
```
git clone https://github.com/ska-sa/meerkathi.git
cd meerkathi
```
1. Start and activate virtual environment
```
$ virtualenv meerkathi-venv
$ source meerkathi-venv/bin/activate
$ pip install <absolute path to meerkathi folder>
$ export PYTHONPATH='' # Ensure that you use venv Python
```

2. Install stimela in venv
```
pip install git+https://github.com/SpheMakh/Stimela
```

3. Build Stimela (don't do this on com4, I've alredy built it)
```
$ stimela build
```
## Running the pipeline
1. Activate the MeerKATHI virtual environment
2. Make sure that the data all the required data is present
3. Check if you pipeline is setup properly 
```
$ meerkathi -c <configuration file> --help
```
4. Run the pipeline
```
$ meerkathi -c <configuration>
```

## Help and descriptions
For Stimela see https://github.com/SpheMakh/Stimela and wiki therein.
For this pipeline and MeerKAT specific notes see [wiki](https://github.com/ska-sa/meerkathi/wiki) of this repository.
