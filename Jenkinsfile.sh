set -e
set -u

WORKSPACE_ROOT="$WORKSPACE/$BUILD_NUMBER"
TEST_OUTPUT_DIR="$WORKSPACE_ROOT/test-output"
TEST_DATA_DIR="$WORKSPACE/../../../test-data"
PULLFOLDER=projects/meerkathi
mkdir $TEST_OUTPUT_DIR

#Custom home for this run's temporary stuff
HOME=$WORKSPACE_ROOT
export HOME
cd $TEST_OUTPUT_DIR
cp $TEST_DATA_DIR/pull_request_data.tar .
tar -xvf pull_request_data.tar
cd ..

###
# Meerkathi test environment variables
###
caracal_tests=$TEST_OUTPUT_DIR
export caracal_tests
caracal_version=$(cd $PULLFOLDER; git rev-parse HEAD) 
export caracal_version
pull_request_data=$TEST_OUTPUT_DIR
export pull_request_data
pull_request_name=$TEST_OUTPUT_DIR/$(cd $PULLFOLDER; git rev-parse HEAD)
export pull_request_name
mkdir -p $pull_requst_name
source $PULLFOLDER/pull_request.sh
