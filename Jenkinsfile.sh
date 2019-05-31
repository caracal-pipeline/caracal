echo "----------------------------------------------"
echo "$JOB_NAME build $BUILD_NUMBER"
WORKSPACE_ROOT="$WORKSPACE/$BUILD_NUMBER"
echo "Setting up build in $WORKSPACE_ROOT"
TEST_OUTPUT_DIR_REL=testcase_output
TEST_OUTPUT_DIR="$WORKSPACE_ROOT/$TEST_OUTPUT_DIR_REL"
TEST_DATA_DIR="$WORKSPACE/../../../test-data"
PROJECTS_DIR_REL="projects"
PROJECTS_DIR=$WORKSPACE_ROOT/$PROJECTS_DIR_REL
mkdir $TEST_OUTPUT_DIR
echo "----------------------------------------------"
echo "\nEnvironment:"
df -h .
echo "----------------------------------------------"
cat /proc/meminfo
echo "----------------------------------------------"

#Custom home for this run's temporary stuff
HOME=$WORKSPACE_ROOT
export HOME

# Install Meerkathi into a virtual env
virtualenv ${WORKSPACE_ROOT}/projects/pyenv
. ${WORKSPACE_ROOT}/projects/pyenv/bin/activate
pip install pip setuptools wheel -U
PATH=${WORKSPACE}/projects/pyenv/bin:$PATH
LD_LIBRARY_PATH=${WORKSPACE}/projects/pyenv/lib:$LD_LIBRARY_PATH
pip install ${WORKSPACE_ROOT}/projects/meerkathi\[extra_diagnostics\]
#put the necessary bits in input
cd $TEST_OUTPUT_DIR
mkdir input

# Aim, fire!
stimela pull
stimela build

# New selfcal test once it goes in
meerkathi --get_data_dataid 1477074305 \
          --general_data_path $TEST_DATA_DIR \
          --get_data_mvftoms_enable true \
          --get_data_mvftoms_channel_range 2525,2776 \
          --self_cal_img_npix 4096 \
          --self_cal_cal_niter 3 \
          --self_cal_image_enable yes \
          --self_cal_image_auto_mask 40 30 10 \
          --self_cal_image_auto_thresh 0.5 0.5 0.5 \
          --self_cal_calibrate_enable yes \
          --image_HI_wsclean_image_npix 256 256 \
          --image_HI_flagging_summary_enable no \
          --polcal_enable true \
          --no-interactive

