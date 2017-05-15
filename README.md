## Set up (do this once)

1. Start and activate virtual environment
```
$ virtualenv meerkathi-venv
$ source meerkathi-venv/bin/activate
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
2. Move into pipeline folder and execute pipeline
```
$ cd pipeline
$ stimela run meerkathi-pipeline.py
```
