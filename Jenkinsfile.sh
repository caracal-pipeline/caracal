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
pip install --force-reinstall ${WORKSPACE_ROOT}/projects/meerkathi
pip install -U -r ${WORKSPACE_ROOT}/projects/meerkathi/requirements.txt
#put the necessary bits in input
cd $TEST_OUTPUT_DIR
mkdir input
cp ${WORKSPACE_ROOT}/projects/meerkathi/meerkathi_misc/aoflagger_strategies/labelled_rfimask.pickle.npy input/
cp ${WORKSPACE_ROOT}/projects/meerkathi/meerkathi_misc/aoflagger_strategies/1Apr2017_firstpass_strategy.rfis input/
cp ${WORKSPACE_ROOT}/projects/meerkathi/meerkathi_misc/aoflagger_strategies/30Mar2017_secondpass_strategy.rfis input/
cp ${WORKSPACE_ROOT}/projects/meerkathi/meerkathi_misc/aoflagger_strategies/firstpass_HI_strat2.rfis input/

# Aim, fire!
stimela pull
stimela build

#meerkathi --get_data_meerkat_query_available_poll_mode override \
#          --get_data_dataid 1477074305 \
#          --general_data_path $TEST_DATA_DIR \
#          --get_data_meerkat_query_available_enable no \
#          --get_data_download_enable no \
#          --get_data_h5toms_channel_range '2525,2781' \
#          --self_cal_img_npix 4096 \
#          --self_cal_image_2_auto_mask 10 \
#          --self_cal_image_3_enable yes \
#          --self_cal_image_4_enable no \
#          --self_cal_image_5_enable no \
#          --self_cal_calibrate_3_enable yes \
#          --self_cal_calibrate_4_enable no \
#          --self_cal_extract_sources_3_enable yes \
#          --self_cal_restore_model_model 2 \
#          --image_HI_wsclean_image_npix 256 256 \
#          --image_HI_flagging_summary_enable no \
#          --self_cal_restore_model_clean_model 3          

# New selfcal test once it goes in
meerkathi --get_data_meerkat_query_available_poll_mode override \
          --get_data_dataid 1477074305 \
          --general_data_path $TEST_DATA_DIR \
          --get_data_meerkat_query_available_enable no \
          --get_data_download_enable no \
          --get_data_h5toms_channel_range 2525,2781 \
          --self_cal_img_npix 4096 \
          --self_cal_cal_niter 3 \
          --self_cal_image_enable yes \
          --self_cal_image_auto_mask 40 30 10 \
          --self_cal_image_auto_thresh 0.5 0.5 0.5 \
          --self_cal_extract_sources_enable yes \
          --self_cal_extract_sources_thresh_pix 10 10 10 \
          --self_cal_extract_sources_thresh_isl 8 8 5 \
          --self_cal_calibrate_enable yes \
          --self_cal_restore_model_clean_model 2 \
          --image_HI_wsclean_image_npix 256 256 \
          --image_HI_flagging_summary_enable no \
          --self_cal_restore_model_clean_model 3 

