set -e

WKS_ROOT="$WORKSPACE/$BUILD_NUMBER"
TEST_OUTPUT_DIR="$WKS_ROOT/test-output"
TEST_DATA_DIR="$WORKSPACE/../../../test-data"
PULLFOLDER=projects/caracal
mkdir $TEST_OUTPUT_DIR

#Custom home for this run's temporary stuff
HOME=$WKS_ROOT
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
caracal_version=$WORKSPACE/$BUILD_NUMBER/$PULLFOLDER/
export caracal_version
pull_request_data=$TEST_OUTPUT_DIR/pull_request_data/
export pull_request_data
pull_request_name=$(cd $PULLFOLDER; git rev-parse HEAD | sed 's/\(^.\{1,7\}\).*/\1/')
export pull_request_name
mkdir -p ${pull_request_name}
source $WORKSPACE/$BUILD_NUMBER/$PULLFOLDER/caratekit.sh -ws ${caracal_tests} \
                                                         -td ${pull_request_data} \
                                                         -lc ${caracal_version} \
                                                         -ct ${pull_request_name} \
							 -sm \
							 -dm \
                                                         -or \
                                                         -f \
                                                         -op \
							 -ro \
							 -spf $WORKSPACE/singularity_pullfolder \
							 -hf $WORKSPACE/home \
                                                         -hn
