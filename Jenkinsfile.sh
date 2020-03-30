# The following Jenkins script is based on reverse-engineering the old one.
# Future versions should be built in a forward manner.
# Notice that in the old script the following variables appear to be external
# (environment) variables and existing directories, before test starts:
#
# ${WORKSPACE}                                     Directory for all tests
# ${BUILD_NUMBER}                                  An identifyer for a Jenkins
#                                                  build, which is not the same
#                                                  as the github build number
# ${WORKSPACE}/${BUILD_NUMBER}/projects            A top-level directory with
#                                                  all projects generated
#                                                  under the build number, it
#                                                  seems to be writeable
# TEST_DATA_DIR="$WORKSPACE/../../../test-data"    The directory with the test
#                                                  data
# ${WORKSPACE_ROOT}/projects/meerkathi             Local CARACal copy, available
#                                                  before start of
#                                                  Jenkinsfile.sh

# It follows that the following should work as a Jenkins test:
${WORKSPACE_ROOT}/projects/meerkathi/meerkathi/utils/carate.sh \
    -ws "${WORKSPACE}/projects" \
    -td "${WORKSPACE}/../../../test-data" \
    -ct "CARACal_test" \
    -ls "${WORKSPACE_ROOT}/projects/meerkathi" \
    -dm \
    -da \
    -ur \
    -f \
    -or \

# Notice that this only tests docker, as the singularity installation currently
# does not work. Future switches should be
# -sm \
# -se \

#########
#########
#########
# old Jenkinsfile initial lines, kept for reference, start
#########
#########
#########
# echo "----------------------------------------------"
# echo "$JOB_NAME build $BUILD_NUMBER"
# WORKSPACE_ROOT="$WORKSPACE/$BUILD_NUMBER"
# echo "Setting up build in $WORKSPACE_ROOT"
# TEST_OUTPUT_DIR_REL=testcase_output
# TEST_OUTPUT_DIR="$WORKSPACE_ROOT/$TEST_OUTPUT_DIR_REL"
# TEST_DATA_DIR="$WORKSPACE/../../../test-data"
# PROJECTS_DIR_REL="projects"
# PROJECTS_DIR=$WORKSPACE_ROOT/$PROJECTS_DIR_REL
# mkdir $TEST_OUTPUT_DIR
# echo "----------------------------------------------"
# echo "\nEnvironment:"
# df -h .
# echo "----------------------------------------------"
# cat /proc/meminfo
# echo "----------------------------------------------"

#Custom home for this run's temporary stuff
# HOME=$WORKSPACE_ROOT
# export HOME

# Install Meerkathi into a virtual env
# virtualenv ${WORKSPACE_ROOT}/projects/pyenv -p python3.6
# . ${WORKSPACE_ROOT}/projects/pyenv/bin/activate
# pip install pip setuptools wheel -U
# PATH=${WORKSPACE}/projects/pyenv/bin:$PATH
# LD_LIBRARY_PATH=${WORKSPACE}/projects/pyenv/lib:$LD_LIBRARY_PATH
# pip install ${WORKSPACE_ROOT}/projects/meerkathi\[extra_diagnostics\]
#put the necessary bits in input
# cd $TEST_OUTPUT_DIR
# mkdir input

# Aim, fire!
# stimela pull -d
# stimela build

# New selfcal test once it goes in
# meerkathi --get_data_dataid 1477074305 \
#          --general_data_path $TEST_DATA_DIR \
#          --get_data_mvftoms_enable true \
#          --get_data_mvftoms_channel_range 2525,2776 \
#          --self_cal_img_npix 4096 \
#          --self_cal_cal_niter 3 \
#          --self_cal_image_enable yes \
#          --self_cal_image_auto_mask 40 30 10 \
#          --self_cal_image_auto_thresh 0.5 0.5 0.5 \
#          --self_cal_calibrate_enable yes \
#          --image_HI_wsclean_image_npix 256 256 \
#          --image_HI_flagging_summary_enable no \
#          --polcal_enable true \
#          --no-interactive

#########
#########
#########
# old Jenkinsfile initial lines, kept for reference, end
#########
#########
#########
