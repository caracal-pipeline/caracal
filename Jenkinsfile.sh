WORKSPACE_ROOT="$WORKSPACE/$BUILD_NUMBER"
TEST_OUTPUT_DIR="$WORKSPACE_ROOT/test-output"
TEST_DATA_DIR="$WORKSPACE/../../../test-data"
mkdir $TEST_OUTPUT_DIR

#Custom home for this run's temporary stuff
HOME=$WORKSPACE_ROOT
export HOME

# Install Stimela into a virtual env
virtualenv ${WORKSPACE_ROOT}/projects/pyenv
. ${WORKSPACE_ROOT}/projects/pyenv/bin/activate
pip install pip setuptools -U
PATH=${WORKSPACE}/projects/pyenv/bin:$PATH
LD_LIBRARY_PATH=${WORKSPACE}/projects/pyenv/lib:$LD_LIBRARY_PATH
pip install ${WORKSPACE_ROOT}/projects/Stimela/

# Copy a clean dataset over
mkdir $TEST_OUTPUT_DIR/msdir
tar -xzvf $TEST_DATA_DIR/kat-7-small.ms.tar.gz -C $TEST_OUTPUT_DIR/msdir
mkdir $TEST_OUTPUT_DIR/input
cp -r $TEST_DATA_DIR/beams $TEST_OUTPUT_DIR/input/beams

which stimela
stimela --version
export SINGULARITY_PULLFOLDER=${WORKSPACE_ROOT}/singularity_images
mkdir $SINGULARITY_PULLFOLDER
stimela pull
stimela pull -s
# fresh build
stimela build -nc

#Run forest run!
cd $TEST_OUTPUT_DIR
nosetests --with-xunit --xunit-file $WORKSPACE_ROOT/nosetests.xml "${WORKSPACE_ROOT}/projects/Stimela/stimela/tests"

##wholesale garage sale --- send SIGKILL to whoever remains - nobody else should be running jobs on the testing machine
#pgrep -f bash | xargs kill -9
#docker kill $(docker ps -qa)
#docker rm $(docker ps -a -q)
