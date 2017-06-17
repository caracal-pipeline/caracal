## Set up (do this once)
0. Clone this repository
Use https and your github credentials, then go to the pipeline folder 'meerkathi'.
```
git clone https://github.com/SpheMakh/meerkathi.git
cd meerkathi
```
1. Start and activate virtual environment
```
$ virtualenv meerkathi-venv
$ source meerkathi-venv/bin/activate
$ [install anything you want]
$ export PYTHONPATH='' # Ensure that you use venv Python
```

2. Install stimela in venv
```
pip install git+https://github.com/SpheMakh/Stimela
```

3. Build Stimela (don't do this on com4, I've alredy built it)
```
stimela build
```
## Running the pipeline (Assuming you've done steps above)

1. Navigate to MeerKATHI virtual environment, and activate it
```
$ cd <MeerKATHI virtual environment>
$ source bin/activate
```
or
```
$ source <MeerKATHI virtual environment>/bin/activate
```
2. Move into pipeline folder and execute pipeline
Assume that we are in the meerkathi local clone of the repository.
```
$ cd pipeline
$ stimela run meerkathi-pipeline.py
```

## Help and descriptions
For Stimela see https://github.com/SpheMakh/Stimela and wiki therein.
For this pipeline and MeerKAT specific notes see wiki of this repository.
