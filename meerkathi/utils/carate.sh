#!/usr/bin/env bash
# The user has first to set the global variables

# Preamble
printf "\n"
printf "##################################################\n"
printf "# Testing CARACal at home (only builder's group) #\n"
printf "##################################################\n"
printf "Temporary Jenkins replacement\n\n"

# Control if help should be switched on
#(( $# > 0 )) || NOINPUT=1

for arg in "$@"
do
    if [[ "$arg" == "--help" ]] || [[ "$arg" == "-h" ]]
    then
        HE=1
    fi
    if [[ "$arg" == "--verbose" ]] || [[ "$arg" == "-v" ]]
    then
        VE=1
    fi
    if [[ "$arg" == "--docker_minimal" ]] || [[ "$arg" == "-dm" ]]
    then
        DM=1
    fi
    if [[ "$arg" == "--docker_extended" ]] || [[ "$arg" == "-de" ]]
    then
        DE=1
    fi
    if [[ "$arg" == "--docker_installation" ]] || [[ "$arg" == "-di" ]]
    then
        DI=1
    fi
    if [[ "$arg" == "--singularity_minimal" ]] || [[ "$arg" == "-sm" ]]
    then
        SM=1
    fi
    if [[ "$arg" == "--singularity_extended" ]] || [[ "$arg" == "-se" ]]
    then
        SE=1
    fi
    if [[ "$arg" == "--singularity_installation" ]] || [[ "$arg" == "-si" ]]
    then
        SI=1
    fi
    if [[ "$arg" == "--use-requirements" ]] || [[ "$arg" == "-ur" ]]
    then
        UR=1
    fi
    if [[ "$arg" == "--omit-stimela-reinstall" ]] || [[ "$arg" == "-os" ]]
    then
        ORSR=1
    fi
    if [[ "$arg" == "--delete-singularity-cache" ]] || [[ "$arg" == "-ds" ]]
    then
        DS=1
    fi
    if [[ "$arg" == "--force" ]] || [[ "$arg" == "-f" ]]
    then
	FORCE=1
    else
	FORCE=0
    fi
    if [[ "$arg" == "--override" ]] || [[ "$arg" == "-or" ]]
    then
        OR=1
    fi
done

if [[ -n "$HE" ]] || [[ -n "$VE" ]]
then
    echo "testingathome.sh"
    echo "Testing CARACal"
    echo
    echo "Input via setting environment variables prior to call and switches"
    echo ""
    echo "Environment variables:"
    echo
    echo "  Setting an environment variable using csh or tcsh:"
    echo "    > setenv variable_name \"variable_value\""
    echo
    echo "  Setting an environment variable"
    echo "  using bash or sh (likely your setup):"
    echo "    \$ export variable_name=\"variable_value\""
    echo
    echo "Environmental variables recognized by this script:"
    echo
    echo "  CARATEST_WORKSPACE:            Directory in which all tests are made"
    echo ""
    echo "  CARATEST_TEST_DATA_DIR:        Directory containing test data (ms"
    echo "                                 format)"
    echo ""

    echo "  CARATEST_CARACAL_BUILD_NUMBER: Build number to test. If not set, master"
    echo "                                 will be tested"
    echo ""
    
    echo "  CARATEST_CARACAL_TEST_NUMBER:  Only specify if CARATEST_CARACAL_BUILD_NUMBER"
    echo "                                 is undefined. All data and installations for"
    echo "                                 a specific test will be saved in the directory"
    echo "                                 \$CARATEST_WORKSPACE/\$CARATEST_CARACAL_TEST_NUMBER." 
    echo "                                 CARATEST_CARACAL_TEST_NUMBER is changed to"
    echo "                                 CARATEST_CARACAL_BUILD_NUMBER"
    echo "                                 if CARATEST_CARACAL_BUILD_NUMBER is defined."
    echo ""
    echo "  CARATEST_LOCAL_SOURCE:         Local CARACal/MeerKATHI copy to use. If not"
    echo "                                 set, CARACal/MeerKATHI will be downloaded"
    echo "                                 from https://github.com/ska-sa/meerkathi"
    echo ""
    echo "  CARATEST_CONFIG_SOURCE:        Local configuration file copy to use for an" 
    echo "                                 additional test"
    echo
    echo "Switches:"
    echo ""
    echo "  --help -h                      Show help"
    echo ""
    echo "  --verbose -v                   Show verbose help"
    echo ""
    echo "  --docker_minimal -dm           Test Docker installation and test run with"
    echo "                                 minimal configuration"
    echo ""
    echo "  --docker_extended -de          Test Docker installation and test run with"
    echo "                                 extended configuration"
    echo ""
    echo "  --docker_installation -di      Test Docker installation"                                         
    echo ""
    echo "  --singularity_minimal -sm      Test Singularity installation and test run"
    echo "                                 with minimal configuration"
    echo ""
    echo "  --singularity_extended -se     Test Singularity installation and test run"
    echo "                                 with extended configuration"
    echo ""
    echo "  --singularity_installation -si Test Singularity installation"                                         
    echo ""
    echo "  --use-requirements -ur         Use requirements.txt when installing"
    echo "                                 MeerKATHI"
    echo ""
    echo "  --omit-stimela-reinstall -os   Do not re-install stimela images"
    echo ""
    echo "  --delete-singularity-cache -ds Delete singularity cache prior to test"
    echo ""
    echo "  --force -f                     Force replacement and re-installation of" 
    echo "                                 all components (you will probably want that)"
    echo "  --override -or                 Override security question (showing root \n";\
    echo "                                 directory and asking whether to proceed.)"
    echo ""
fi

if [[ -n "$VE" ]]
then
    echo ""
    echo " The script creates a root directory"
    echo " \$CARATEST_WORKSPACE/\$CARATEST_CARACAL_TEST_NUMBER, where"
    echo " CARATEST_WORKSPACE is an environment variable containing the path of a"
    echo " root directory to all tests done with this script. The variable"
    echo " CARATEST_CARACAL_TEST_NUMBER is identical to the environment variable"
    echo " CARATEST_CARACAL_BUILD_NUMBER if that is set by the user, and has to"
    echo " be supplied independently (i.e. to be defined prior to the script"
    echo " call) as an environment variable if CARATEST_CARACAL_BUILD_NUMBER is"
    echo " not defined. The rationale behind that is that the test directory is"
    echo " always linked to a git(hub) build number if that exists. Otherwise, if"
    echo " CARATEST_CARACAL_BUILD_NUMBER is not defined, the user can supply an"
    echo " alternative name \$CARATEST_CARACAL_TEST_NUMBER. In the test root"
    echo " directory \$CARATEST_WORKSPACE/\$CARATEST_CARACAL_TEST_NUMBER, a home"
    echo " directory called home, a virtual environment called"
    echo " caracal_virtualenv, a CARACal copy meerkathi, and up to four test"
    echo " directories are created, within which the tests are conducted. If the"
    echo " --force or -f switch is set, all existing directories and"
    echo " installations, apart from a potentially existing singularity cache directory"
    echo " are deleted and replaced, if not, only those directories"
    echo " and files are created, which do not exist yet. Exceptions from that rule"
    echo " are set with the --omit-stimela-reinstall or -os switch, which would"
    echo " prevent a re-installation of stimela even if -f is set. If the switch"
    echo " --delete-singularity-cache or -ds is set, the singularity cache is deleted"
    echo " prior to installation."
    echo ""
    echo "  In detail (all installations in the root directory"
    echo "  \$CARATEST_WORKSPACE/\$CARATEST_CARACAL_TEST_NUMBER):"
    echo ""
    echo "  - a directory called home is created (if not existing or if -f is set)"
    echo "    and the HOME environment variable set to that home directory"
    echo ""
    echo "  - a python 3 virtual environment name caracal_venv is created (if not"
    echo "    existing or if -f is set) and activated"
    echo ""
    echo "  - caracal is either downloaded to the root directory (if not existing"
    echo "    or if -f is set) from https://github.com/ska-sa/meerkathi or, if the"
    echo "    CARATEST_LOCAL_SOURCE environment variable is set,"
    echo "    \$CARATEST_LOCAL_SOURCE is copied to the root directory (if not"
    echo "    existing or if -f is set, notice that the directory tree should be a"
    echo "    valid meerkathi tree, ready for installation)"
    echo ""
    echo "  - the caracal version CARATEST_CARACAL_BUILD_NUMBER is checked out"
    echo "    using git if CARATEST_CARACAL_BUILD_NUMBER is defined"
    echo ""
    echo "  - caracal[beta] is installed via pip"
    echo ""
    echo "  - if --use-requirements or -ur switch is set, the"
    echo "    meerkathi/requirements.txt is installed via pip"
    echo ""
    echo "  - when switches --docker_minimal, -dm, --docker_extended, -de,"
    echo "    --docker_installation, -di are set, home/.stimela is removed, docker"
    echo "    system prune is invoked, and docker stimela is installed (stimela"
    echo "    build)"
    echo ""
    echo "  - when switches --singularity_minimal, -sm, --singularity_extended,"
    echo "    -se, --singularity_installation, -si are set, home/.stimela is"
    echo "    removed, and singularity stimela is installed in the directory (if"
    echo "    not existing or if -f is set, stimela pull --singularity"
    echo "    --pull-folder rootfolder/stimela_singularity)"
    echo ""
    echo "  - when switch --singularity_minimal or -sm is set, a directory"
    echo "    test_minimal_singularity is created (if not existing or if -f is"
    echo "    set), the configuration file"
    echo "    meerkathi/meerkathi/sample_configurations/minimalConfig.yml is"
    echo "    copied to that directory, all .ms files from \$CARATEST_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_minimal_singularity"
    echo "    directory and minimalConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then meerkathi is run with"
    echo "    minimalConfig.yml and declared successful if certain expected files"
    echo "    are created."
    echo ""
    echo "  - when switch --singularity_extended or -se is set, a directory"
    echo "    test_extended_singularity is created (if not existing or if -f is"
    echo "    set), the configuration file"
    echo "    meerkathi/meerkathi/sample_configurations/extendedConfig.yml is"
    echo "    copied to that directory, all .ms files from \$CARATEST_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_extended_singularity"
    echo "    directory and extendedConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then meerkathi is run with"
    echo "    extendedConfig.yml and declared successful if certain expected files"
    echo "    are created."
    echo ""
    echo "  - when switch --docker_minimal or -dm is set, a directory"
    echo "    \- test_minimal_docker is created (if not existing or if -f is set),"
    echo "    the configuration file"
    echo "    meerkathi/meerkathi/sample_configurations/minimalConfig.yml is"
    echo "    copied to that directory, all .ms files from \$CARATEST_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_minimal_docker"
    echo "    directory and minimalConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then meerkathi is run with"
    echo "    minimalConfig.yml and declared successful if certain expected files"
    echo "    are created."
    echo ""
    echo "  - when switch --docker_extended or -de is set, a directory"
    echo "    test_extended_docker is created (if not existing or if -f is set),"
    echo "    the configuration file"
    echo "    meerkathi/meerkathi/sample_configurations/extendedConfig.yml is"
    echo "    copied to that directory, all .ms files from \$CARATEST_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_extended_docker"
    echo "    directory and extendedConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then meerkathi is run with"
    echo "    extendedConfig.yml and declared successful if certain expected files"
    echo "    are created."
    echo ""
    echo "  - when environment variable CARATEST_CONFIG_SOURCE is set in combination with"
    echo "    switches --singularity_installation or -si set, then that yaml configuration"
    echo "    source is used for a further singularity test in the directory"
    echo "    test_prefix_singularity, where prefix is the prefix of the yaml file. The line"
    echo "    dataid: [''] in that file is replaced by the appropriate line to process the "    
    echo "    test data sets in \$CARATEST_TEST_DATA_DIR"
    echo ""
    echo "  - when environment variable CARATEST_CONFIG_SOURCE is set in combination with"
    echo "    switches --docker_installation or -di set, then that yaml configuration"
    echo "    source is used for a further singularity test in the directory"
    echo "    test_prefix_singularity, where prefix is the prefix of the yaml file. The line"
    echo "    dataid: [''] in that file is replaced by the appropriate line to process the "    
    echo "    test data sets in \$CARATEST_TEST_DATA_DIR"
    echo ""
    echo " The test returns 0 if no error has turned up"
    echo ""
    echo " Note that in particular Stimela has components that are external to"
    echo " the root directory and will be touched by this test.  "
    echo
fi

if [[ -n "$HE" ]] || [[ -n "$VE" ]]
then
    echo "Stopping. Do not set switches --help --verbose -h -v to continue."
    kill "$PPID"; exit 0;
fi

printf "############\n"
printf "# Starting #\n"
printf "############\n"
printf "\n"

# Environment variables
# [ -n "$CARATEST_JOB_NAME" ] || { printf "You have to define a global CARATEST_JOB_NAME variable,
# like (if you're\nusing bash):\n$ export CARATEST_JOB_NAME="CARCal test"\n\n"; kill "$PPID"; exit 1; }
[[ -n "$CARATEST_WORKSPACE" ]] || { \
    printf "You have to define a global CARATEST_WORKSPACE variable, like (if you're\nusing bash):\n";\
    printf "$ export CARATEST_WORKSPACE=\"/home/username/meerkathi_tests\"\n";\
    printf "It is the top level directory of all tests.\n\n";\
    kill "$PPID"; exit 1;\
}

[[ -n "$CARATEST_TEST_DATA_DIR" ]] || { \
    printf "You have to define a global CARATEST_TEST_DATA_DIR variable, like (if\n";\
    printf "you're using bash):\n";\
    printf "$ export CARATEST_TEST_DATA_DIR=\"/home/username/meerkathi_tests/rawdata\"\n";\
    printf "And put test rawdata therein: a.ms  b.ms c.ms ...\n";\
    printf "These test data will be copied across for the test.\n\n";\
    kill "$PPID"; exit 1;\
}

[[ -n "$CARATEST_CARACAL_BUILD_NUMBER" ]] || { \
    printf "You can define a global variable CARATEST_CARACAL_BUILD_NUMBER, like (if you're\n";\
    printf "using bash):\n";\
    printf "$ export CARATEST_CARACAL_BUILD_NUMBER=\"b027661de6ff93a183ff240b96af86583932fc1e\"\n";
    printf "You can find the build number when running e.g.\n";\
    printf "$ git log        in your MeerKATHI folder.\n";\
    printf "Or you can look up the build number in github.\n";\
    printf "If defined this test will check out that build number.\n";\
    printf "If not defined, either the local installation or the current remote master\n";\
    printf "will be checked out.\n\n";\
}

# Force test number to be identical with build number, if it is defined
[[ -z "$CARATEST_CARACAL_BUILD_NUMBER" ]] || { \
    export CARATEST_CARACAL_TEST_NUMBER=$CARATEST_CARACAL_BUILD_NUMBER; \
}

[[ -n "$CARATEST_CARACAL_TEST_NUMBER" ]] || { \
    printf "Without build number you have to define a global CARATEST_CARACAL_TEST_NUMBER\n";\
    printf "variable, giving your test directory a name, like (if you're using bash):\n";\
    printf "$ export CARATEST_CARACAL_TEST_NUMBER=\"b027661de6ff93a183ff240b96af86583932fc1e\"\n";\
    printf "Otherwise choose any unique identifyer\n\n";\
    kill "$PPID"; exit 1; \
}

[[ -n "$CARATEST_LOCAL_SOURCE" ]] || { \
    printf "The global variable CARATEST_LOCAL_SOURCE is not set, meaning that MeerKATHI\n";\
    printf "will be downloaded from https://github.com/ska-sa/meerkathi\n\n";\
}

[[ -n "$CARATEST_CONFIG_SOURCE" ]] || { \
    printf "The global variable CARATEST_CONFIG_SOURCE is not set, meaning that no CARACal\n";\
    printf "test will be made on own supplied configuration.\n\n";\
}

# Start test
echo "##########################################"
echo "CARACal test $CARATEST_CARACAL_TEST_NUMBER"
echo "##########################################"
echo

[[ -e $CARATEST_WORKSPACE ]] || {echo "The workspace direcotyr $CARATEST_WORKSPACE" does not yet exist.}

# Create workspace
WORKSPACE_ROOT="$CARATEST_WORKSPACE/$CARATEST_CARACAL_TEST_NUMBER"

# Check if all's well, force reply by user
echo "The directory"
echo "$CARATEST_WORKSPACE/$CARATEST_CARACAL_TEST_NUMBER"
echo "and its content will be created/changed."
echo "The directory $CARATEST_WORKSPACE/.singularity might be created/changed"
if [[ -z $OR ]]
then
    echo "Is that ok (Yes/No)?"
    read proceed
    [[ $proceed == "Yes" ]] || { echo "Cowardly quitting"; kill "$PPID"; exit 1; }
fi


# Check if workspace_root exists if we do not use force
if ( ( $FORCE==0 ) )
then
    if [[ -d $WORKSPACE_ROOT ]]
    then
	echo "Be aware that no existing file will be replaced, use -f to override"
	echo
    fi
fi

echo "##########################################"
echo "Setting up build in $WORKSPACE_ROOT"
echo "##########################################"
echo

#(( FORCE==0 )) || { rm -rf $WORKSPACE_ROOT; }
mkdir -p $WORKSPACE_ROOT

# Save home for later 
if [[ -n $HOME ]]
then
    OLD_HOME=$HOME
fi

# This ensures that when stopping, the $HOME environment variable is restored
function cleanup {
  export HOME=$OLD_HOME
}
trap cleanup EXIT

if [[ -n "$CARATEST_CONFIG_SOURCE" ]]
then
    if [[ -z $DI ]] && [[ -z $SI ]]
    then
	echo "No Stimela installation made in context with specifying an additional config"
	echo "file. Ommitting testing that file"
    else
	# Get the config file name
	configfilename=`echo $CARATEST_CONFIG_SOURCE | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}'`
	echo $configfilename 
    fi
fi
exit
# Search for test data and set variable accordingly
if [[ -n $DM ]] || [[ -n $DE ]] || [[ -n $SM ]] || [[ -n $SE ]] 
then
    if [[ -e $CARATEST_TEST_DATA_DIR ]]
    then
        # Check if there are any ms files
	mss=`find $CARATEST_TEST_DATA_DIR -name *.ms`
	[[ ! -z "$mss" ]] || { printf "Test data required in $CARATEST_TEST_DATA_DIR \n"; kill "$PPID"; exit 1; }
	
	# This generates the dataid string
	dataidstr=`ls -d $CARATEST_TEST_DATA_DIR/*.ms | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}' | sed '{s/ /\x27,\x27/g; s/$/\x27\]/; s/^/dataid: \[\x27/}'`
    else
	printf "Create directory $CARATEST_TEST_DATA_DIR and put test rawdata\n";\
	printf "therein: a.ms b.ms c.ms ...\n"
	kill "$PPID"; exit 1;
    fi
fi


# The following would only work in an encapsulated environment
export HOME=$WORKSPACE_ROOT/testhome
(( FORCE==0 )) || { rm -rf $HOME; }
mkdir -p $HOME

# Create virtualenv and start
echo "##########################################"
echo "Building virtualenv in $WORKSPACE_ROOT"
echo "##########################################"
echo
(( FORCE==0 )) || { rm -rf ${WORKSPACE_ROOT}/caracal_venv; }
[[ -d ${WORKSPACE_ROOT}/caracal_venv ]] || { virtualenv -p python3 ${WORKSPACE_ROOT}/caracal_venv; }
echo "Entering virtualenv in $WORKSPACE_ROOT"
. ${WORKSPACE_ROOT}/caracal_venv/bin/activate
pip install pip setuptools wheel -U

# Install software
echo
echo "##########################################"
echo "Fetching CARACal"
echo "##########################################"
echo
(( FORCE==0 )) || { rm -rf ${WORKSPACE_ROOT}/meerkathi; }
if [[ -n "$CARATEST_LOCAL_SOURCE" ]]
then
    if [[ -e ${WORKSPACE_ROOT}/meerkathi ]]
    then
	echo "Not re-fetching MeerKATHI, use -f if you want that."
    else
	cp -r ${CARATEST_LOCAL_SOURCE} ${WORKSPACE_ROOT}/
    fi
else
    cd ${WORKSPACE_ROOT}
    if [[ -e ${WORKSPACE_ROOT}/meerkathi ]]
    then
    	if (( FORCE==0 ))
        then
	    echo "Not re-fetching MeerKATHI, use -f if you want that."
        else
	    rm -rf ${WORKSPACE_ROOT}/meerkathi
	    git clone https://github.com/ska-sa/meerkathi.git
	fi
    else
	git clone https://github.com/ska-sa/meerkathi.git
    fi
fi
if [[ -n "$CARATEST_CARACAL_BUILD_NUMBER" ]]
then
    cd meerkathi
    [[ -z $CARATEST_LOCAL_SOURCE ]] || { \
	echo "If an error occurs here, it likely means that the local installation";\
	echo "of CARACal does not contain the build number. You may want to use the";\
	echo "master branch and unset the environmrnt variable CARATEST_CARACAL_BUILD_NUMBER:";\
	echo "In bash: $ unset CARATEST_CARACAL_BUILD_NUMBER";\
    }
    git checkout ${CARATEST_CARACAL_BUILD_NUMBER}
fi
								  
echo
echo "##########################################"
echo "Installing CARACal"
echo "##########################################"
echo

#PATH=${WORKSPACE}/projects/pyenv/bin:$PATH
#LD_LIBRARY_PATH=${WORKSPACE}/projects/pyenv/lib:$LD_LIBRARY_PATH
pip install -U -I ${WORKSPACE_ROOT}/meerkathi\[beta\]
if [[ -n $UR ]]
then
    echo "Intstalling requirements.txt"
    pip install -U -I -r ${WORKSPACE_ROOT}/meerkathi/requirements.txt
fi

if [[ -z $DM ]] && [[ -z $DE ]] && [[ -z $DI ]] && [[ -z $SM ]] && [[ -z $SE ]] && [[ -z $SI ]]
then
    echo "You have not defined a test:"
    echo "--docker_minimal or -dm"
    echo "--docker_extended or -de"
    echo "--docker_installation or -di"
    echo "--singularity_minimal or -sm"
    echo "--singularity_extended or -se"
    echo "--singularity_installation or -si"
    echo "Use -h flag for more information"
    kill "$PPID"; exit 0
fi

if [[ -n $DM ]] || [[ -n $DE ]] || [[ -n $DI ]]
then
    if [[ -n $ORSR ]]
    then
	printf "|${ORSR}|"
	echo "Omitting re-installation of Stimela Docker images"
    else
	echo
	echo "#####################################"
	echo "# Installing Stimela Docker images" #
	echo "#####################################"
	echo
	echo "Installing Stimela (Docker)"
	# Not sure if stimela listens to $HOME or if another variable has to be set.
	# This $HOME is not the usual $HOME, see above
	rm -f $HOME/.stimela/*
	docker system prune
	stimela build
    fi
fi

if [[ -n $DE ]]
then
    echo
    echo "##########################"
    echo "# Docker: extendedConfig #"
    echo "##########################"
    echo

    if [[ -e ${WORKSPACE_ROOT}/test_extendedConfig_docker ]] && (( FORCE==0 ))
    then
	echo "Will not re-create existing directory ${WORKSPACE_ROOT}/test_extendedConfig_docker"
	echo "and use old results. Use -f to override."
    else
	rm -rf ${WORKSPACE_ROOT}/test_extendedConfig_docker
	
	echo "Preparing extended Docker test (using extendedConfig.yml) in"
	echo "${WORKSPACE_ROOT}/test_extendedConfig_docker"
	mkdir -p ${WORKSPACE_ROOT}/test_extendedConfig_docker/msdir
	sed "s/dataid: \[\x27\x27\]/$dataidstr/" ${WORKSPACE_ROOT}/meerkathi/sample_configurations/extendedConfig.yml > ${WORKSPACE_ROOT}/test_extendedConfig_docker/extendedConfig.yml
	cp -r $CARATEST_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/test_extendedConfig_docker/msdir/

	echo "Running extended Docker test (using extendedConfig.yml)"
	cd ${WORKSPACE_ROOT}/test_extendedConfig_docker

	# Notice that currently all output will be false, such that || true is required to ignore this
	meerkathi -c extendedConfig.yml || true
    fi

    echo "Checking output of extendedConfig Docker test"
    if [[ ! -f "${WORKSPACE_ROOT}/test_extendedConfig_docker/output/mosaic/XXXX" ]];
    then
	echo "${WORKSPACE_ROOT}/test_extendedConfig_docker/output/mosaic/XXXX does not exist, aborting"
	kill "$PPID"; exit 1
    fi
    if [[ ! -f "${WORKSPACE_ROOT}/test_extendedConfig_docker/output/mosaic/XXXX" ]];
    then
	echo "${WORKSPACE_ROOT}/test_extendedConfig_docker/output/mosaic/XXXX does not exist, aborting"
	kill "$PPID"; exit 1
    fi
fi

if [[ -n $DE ]]
then
    echo
    echo "##########################"
    echo "# Docker: minimalConfig #"
    echo "##########################"
    echo

    if [[ -e ${WORKSPACE_ROOT}/test_minimalConfig_docker ]] && (( FORCE==0 ))
    then
	echo "Will not re-create existing directory ${WORKSPACE_ROOT}/test_minimalConfig_docker"
	echo "and use old results. Use -f to override."
    else
	rm -rf ${WORKSPACE_ROOT}/test_minimalConfig_docker
	
	echo "Preparing extended Docker test (using minimalConfig.yml) in"
	echo "${WORKSPACE_ROOT}/test_minimalConfig_docker"
	mkdir -p ${WORKSPACE_ROOT}/test_minimalConfig_docker/msdir
	sed "s/dataid: \[\x27\x27\]/$dataidstr/" ${WORKSPACE_ROOT}/meerkathi/sample_configurations/minimalConfig.yml > ${WORKSPACE_ROOT}/test_minimalConfig_docker/minimalConfig.yml
	cp -r $CARATEST_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/test_minimalConfig_docker/msdir/

	echo "Running extended Docker test (using minimalConfig.yml)"
	cd ${WORKSPACE_ROOT}/test_minimalConfig_docker

	# Notice that currently all output will be false, such that || true is required to ignore this
	meerkathi -c minimalConfig.yml || true
    fi

    echo "Checking output of minimalConfig Docker test"
    if [[ ! -f "${WORKSPACE_ROOT}/test_minimalConfig_docker/output/mosaic/XXXX" ]];
    then
	echo "${WORKSPACE_ROOT}/test_minimalConfig_docker/output/mosaic/XXXX does not exist, aborting"
	kill "$PPID"; exit 1
    fi
    if [[ ! -f "${WORKSPACE_ROOT}/test_minimalConfig_docker/output/mosaic/XXXX" ]];
    then
	echo "${WORKSPACE_ROOT}/test_minimalConfig_docker/output/mosaic/XXXX does not exist, aborting"
	kill "$PPID"; exit 1
    fi
fi


if [[ -n $SM ]] || [[ -n $SE ]] || [[ -n $SI ]]
then
    # This sets the singularity image folder to the test environment
    export SINGULARITY_CACHEDIR=$CARATEST_WORKSPACE/.singularity
    if (( FORCE==0 )) || [[ -n $ORSR ]]
    then
	if [[ -e ${WORKSPACE_ROOT}/stimela_singularity ]]
	then
	    echo "Will not re-create existing stimela_singularity and use old installation."
	    echo "Use -f to override and unset -or or --omit-stimela-reinstall flags."
	fi
    else
	rm -rf ${WORKSPACE_ROOT}/stimela_singularity
	[[ -z $DS ]] || { rm -rf $CARATEST_WORKSPACE/.singularity; }
    fi
    if [[ ! -e ${WORKSPACE_ROOT}/stimela_singularity ]]
    then
	echo
	echo "###########################################"
	echo "# Installing Stimela images (Singularity) #"
	echo "###########################################"
	echo
	rm -f $HOME/.stimela/*
	rm -rf ${WORKSPACE_ROOT}/stimela_singularity
	mkdir ${WORKSPACE_ROOT}/stimela_singularity
	stimela pull --singularity --pull-folder ${WORKSPACE_ROOT}/stimela_singularity
    fi
fi

if [[ -n $DE ]]
then
    echo
    echo "###############################"
    echo "# Singularity: extendedConfig #"
    echo "###############################"
    echo

    if [[ -e ${WORKSPACE_ROOT}/test_extendedConfig_singularity ]] && (( FORCE==0 ))
    then
	echo "Will not re-create existing directory ${WORKSPACE_ROOT}/test_extendedConfig_singularity"
	echo "and use old results. Use -f to override."
    else
	rm -rf ${WORKSPACE_ROOT}/test_extendedConfig_singularity
	
	echo "Preparing extended Singularity test (using extendedConfig.yml) in"
	echo "${WORKSPACE_ROOT}/test_extendedConfig_singularity"
	mkdir -p ${WORKSPACE_ROOT}/test_extendedConfig_singularity/msdir
	sed "s/dataid: \[\x27\x27\]/$dataidstr/" ${WORKSPACE_ROOT}/meerkathi/sample_configurations/extendedConfig.yml > ${WORKSPACE_ROOT}/test_extendedConfig_singularity/extendedConfig.yml
	cp -r $CARATEST_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/test_extendedConfig_singularity/msdir/

	echo "Running extended Singularity test (using extendedConfig.yml)"
	cd ${WORKSPACE_ROOT}/test_extendedConfig_singularity

	# Notice that currently all output will be false, such that || true is required to ignore this
	meerkathi -c extendedConfig.yml --container-tech singularity -sid ${WORKSPACE_ROOT}/stimela_singularity || true
    fi

    echo "Checking output of extendedConfig Singularity test"
    if [[ ! -f "${WORKSPACE_ROOT}/test_extendedConfig_singularity/output/mosaic/XXXX" ]];
    then
	echo "${WORKSPACE_ROOT}/test_extendedConfig_singularity/output/mosaic/XXXX does not exist, aborting"
	kill "$PPID"; exit 1
    fi
    if [[ ! -f "${WORKSPACE_ROOT}/test_extendedConfig_singularity/output/mosaic/XXXX" ]];
    then
	echo "${WORKSPACE_ROOT}/test_extendedConfig_singularity/output/mosaic/XXXX does not exist, aborting"
	kill "$PPID"; exit 1
    fi
fi

if [[ -n $DE ]]
then
    echo
    echo "##########################"
    echo "# Singularity: minimalConfig #"
    echo "##########################"
    echo

    if [[ -e ${WORKSPACE_ROOT}/test_minimalConfig_singularity ]] && (( FORCE==0 ))
    then
	echo "Will not re-create existing directory ${WORKSPACE_ROOT}/test_minimalConfig_singularity"
	echo "and use old results. Use -f to override."
    else
	rm -rf ${WORKSPACE_ROOT}/test_minimalConfig_singularity
	
	echo "Preparing extended Singularity test (using minimalConfig.yml) in"
	echo "${WORKSPACE_ROOT}/test_minimalConfig_singularity"
	mkdir -p ${WORKSPACE_ROOT}/test_minimalConfig_singularity/msdir
	sed "s/dataid: \[\x27\x27\]/$dataidstr/" ${WORKSPACE_ROOT}/meerkathi/sample_configurations/minimalConfig.yml > ${WORKSPACE_ROOT}/test_minimalConfig_singularity/minimalConfig.yml
	cp -r $CARATEST_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/test_minimalConfig_singularity/msdir/

	echo "Running extended Singularity test (using minimalConfig.yml)"
	cd ${WORKSPACE_ROOT}/test_minimalConfig_singularity

	# Notice that currently all output will be false, such that || true is required to ignore this
	meerkathi -c minimalConfig.yml --container-tech singularity -sid ${WORKSPACE_ROOT}/stimela_singularity || true
    fi

    echo "Checking output of minimalConfig Singularity test"
    if [[ ! -f "${WORKSPACE_ROOT}/test_minimalConfig_singularity/output/mosaic/XXXX" ]];
    then
	echo "${WORKSPACE_ROOT}/test_minimalConfig_singularity/output/mosaic/XXXX does not exist, aborting"
	kill "$PPID"; exit 1
    fi
    if [[ ! -f "${WORKSPACE_ROOT}/test_minimalConfig_singularity/output/mosaic/XXXX" ]];
    then
	echo "${WORKSPACE_ROOT}/test_minimalConfig_singularity/output/mosaic/XXXX does not exist, aborting"
	kill "$PPID"; exit 1
    fi
fi

if [[ -n $configfilename ]]
then
    if [[ -n $SI ]]
    then
	echo
	echo "###############################"
	echo " Singularity: $configfilename "
	echo "###############################"
	echo

	if [[ -e ${WORKSPACE_ROOT}/test_${configfilename}_singularity ]] && (( FORCE==0 ))
	then
	    echo "Will not re-create existing directory ${WORKSPACE_ROOT}/test_${configfilename}_singularity"
	    echo "and use old results. Use -f to override."
	else
	    rm -rf ${WORKSPACE_ROOT}/test_${configfilename}_singularity
	
	    echo "Preparing extended Singularity test (using ${configfilename}.yml) in"
	    echo "${WORKSPACE_ROOT}/test_${configfilename}_singularity"
	    mkdir -p ${WORKSPACE_ROOT}/test_${configfilename}_singularity/msdir
	    sed "s/dataid: \[\x27\x27\]/$dataidstr/" $CARATEST_CONFIG_SOURCE > ${WORKSPACE_ROOT}/test_${configfilename}_singularity/${configfilename}.yml
	    cp -r $CARATEST_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/test_${configfilename}_singularity/msdir/
	    
	    echo "Running extended Singularity test (using ${configfilename}.yml)"
	    cd ${WORKSPACE_ROOT}/test_${configfilename}_singularity

	    # Notice that currently all output will be false, such that || true is required to ignore this
	    meerkathi -c ${configfilename}.yml --container-tech singularity -sid ${WORKSPACE_ROOT}/stimela_singularity || true
	fi

	echo "Checking output of ${configfilename} Singularity test"
	if [[ ! -f "${WORKSPACE_ROOT}/test_${configfilename}_singularity/output/mosaic/XXXX" ]];
	then
	    echo "${WORKSPACE_ROOT}/test_${configfilename}_singularity/output/mosaic/XXXX does not exist, aborting"
	    kill "$PPID"; exit 1
	fi
	if [[ ! -f "${WORKSPACE_ROOT}/test_${configfilename}_singularity/output/mosaic/XXXX" ]];
	then
	    echo "${WORKSPACE_ROOT}/test_${configfilename}_singularity/output/mosaic/XXXX does not exist, aborting"
	    kill "$PPID"; exit 1
	fi	
    fi
    if [[ -n $DI ]]
    then
	echo
	echo "###############################"
	echo " Docker: $configfilename "
	echo "###############################"
	echo

	if [[ -e ${WORKSPACE_ROOT}/test_${configfilename}_docker ]] && (( FORCE==0 ))
	then
	    echo "Will not re-create existing directory ${WORKSPACE_ROOT}/test_${configfilename}_docker"
	    echo "and use old results. Use -f to override."
	else
	    rm -rf ${WORKSPACE_ROOT}/test_${configfilename}_docker
	
	    echo "Preparing extended Docker test (using ${configfilename}.yml) in"
	    echo "${WORKSPACE_ROOT}/test_${configfilename}_docker"
	    mkdir -p ${WORKSPACE_ROOT}/test_${configfilename}_docker/msdir
	    sed "s/dataid: \[\x27\x27\]/$dataidstr/" $CARATEST_CONFIG_SOURCE > ${WORKSPACE_ROOT}/test_${configfilename}_docker/${configfilename}.yml
	    cp -r $CARATEST_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/test_${configfilename}_docker/msdir/
	    
	    echo "Running extended Docker test (using ${configfilename}.yml)"
	    cd ${WORKSPACE_ROOT}/test_${configfilename}_docker

	    # Notice that currently all output will be false, such that || true is required to ignore this
	    meerkathi -c ${configfilename}.yml || true
	fi

	echo "Checking output of ${configfilename} Docker test"
	if [[ ! -f "${WORKSPACE_ROOT}/test_${configfilename}_docker/output/mosaic/XXXX" ]];
	then
	    echo "${WORKSPACE_ROOT}/test_${configfilename}_docker/output/mosaic/XXXX does not exist, aborting"
	    kill "$PPID"; exit 1
	fi
	if [[ ! -f "${WORKSPACE_ROOT}/test_${configfilename}_docker/output/mosaic/XXXX" ]];
	then
	    echo "${WORKSPACE_ROOT}/test_${configfilename}_docker/output/mosaic/XXXX does not exist, aborting"
	    kill "$PPID"; exit 1
	fi	
    fi
fi

echo "Wow, everything seems to be ok"
exit 0
