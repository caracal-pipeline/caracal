#!/usr/bin/env bash
# The following ensures the script to stop on errors
set -e

# Preamble
printf "\n"
printf "###########################\n"
printf "# Testing CARACal at home #\n"
printf "###########################\n"

# Control if help should be switched on
#(( $# > 0 )) || NOINPUT=1

# Default variables
FN=4 

argcount=0
for arg in "$@"
do
    (( argcount += 1 ))
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
    if [[ "$arg" == "--fail-number" ]] || [[ "$arg" == "-fn" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --fail-number or -fn switch, stopping."; kill "$PPID"; exit 0; }
        FN=${!nextcount}
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
    if [[ "$arg" == "--workspace" ]] || [[ "$arg" == "-ws" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --workspace or -ws switch, stopping."; kill "$PPID"; exit 0; }
        CARATE_WORKSPACE=${!nextcount}
    fi
    if [[ "$arg" == "--test-data-dir" ]] || [[ "$arg" == "-td" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --test-data-dir or -td switch, stopping."; kill "$PPID"; exit 0; }
    
 CARATE_TEST_DATA_DIR=${!nextcount}
    fi
    if [[ "$arg" == "--caracal-build-number" ]] || [[ "$arg" == "-cb" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --caracal-build-number or -cb switch, stopping."; kill "$PPID"; exit 0; }
    
 CARATE_CARACAL_BUILD_NUMBER=${!nextcount}
    fi
    if [[ "$arg" == "--caracal-test-number" ]] || [[ "$arg" == "-ct" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --caracal-test-number or -ct switch, stopping."; kill "$PPID"; exit 0; }
    
 CARATE_CARACAL_TEST_NUMBER=${!nextcount}
    fi
    if [[ "$arg" == "--local-source" ]] || [[ "$arg" == "-ls" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --local-source or -ls switch, stopping."; kill "$PPID"; exit 0; }
 CARATE_LOCAL_SOURCE=${!nextcount}
    fi
    if [[ "$arg" == "--config-source" ]] || [[ "$arg" == "-cs" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --config-source or -cs switch, stopping."; kill "$PPID"; exit 0; }
 CARATE_CONFIG_SOURCE=${!nextcount}
    fi
done

if [[ -n "$HE" ]] || [[ -n "$VE" ]]
then
    echo "testingathome.sh"
    echo "Testing CARACal"
    echo
    echo "Input via setting environment variables"
    echo "(Input by appropriate switches overrides environment variables)"
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
    echo "  CARATE_WORKSPACE:            Directory in which all tests are made"
    echo ""
    echo "  CARATE_TEST_DATA_DIR:        Directory containing test data (ms"
    echo "                                 format)"
    echo ""

    echo "  CARATE_CARACAL_BUILD_NUMBER: Build number to test. If not set, master"
    echo "                                 will be tested"
    echo ""
    
    echo "  CARATE_CARACAL_TEST_NUMBER:  Only specify if CARATE_CARACAL_BUILD_NUMBER"
    echo "                                 is undefined. All data and installations for"
    echo "                                 a specific test will be saved in the directory"
    echo "                                 \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_NUMBER." 
    echo "                                 CARATE_CARACAL_TEST_NUMBER is changed to"
    echo "                                 CARATE_CARACAL_BUILD_NUMBER"
    echo "                                 if CARATE_CARACAL_BUILD_NUMBER is defined."
    echo ""
    echo "  CARATE_LOCAL_SOURCE:         Local CARACal/MeerKATHI copy to use. If not"
    echo "                                 set, CARACal/MeerKATHI will be downloaded"
    echo "                                 from https://github.com/ska-sa/meerkathi"
    echo ""
    echo "  CARATE_CONFIG_SOURCE:        Local configuration file copy to use for an" 
    echo "                                 additional test"
    echo
    echo "Switches:"
    echo ""
    echo "  --help -h                         Show help"
    echo ""
    echo "  --verbose -v                      Show verbose help"
    echo ""
    echo "  --workspace ARG -ws ARG             Use ARG instead of environment variable"
    echo "                                      CARATE_WORKSPACE"
    echo ""
    echo "  --test-data-dir ARG -td ARG         Use ARG instead of environment variable"
    echo "                                      CARATE_TEST_DATA_DIR"
    echo ""
    echo "  --caracal-build-number ARG -cb ARG  Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_BUILD_NUMBER"
    echo ""
    echo "  --caracal-test-number ARG -ct ARG   Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_TEST_NUMBER"
    echo ""
    echo "  --local-source ARG -ls ARG          Use ARG instead of environment variable"
    echo "                                      CARATE_LOCAL_SOURCE"
    echo ""
    echo "  --config-source ARG -csARG          Use ARG instead of environment variable"
    echo "                                      CARATE_CONFIG_SOURCE"
    echo ""
    echo "  --docker_minimal -dm                Test Docker installation and test run with"
    echo "                                      minimal configuration"
    echo ""
    echo "  --docker_extended -de               Test Docker installation and test run with"
    echo "                                      extended configuration"
    echo ""
    echo "  --docker_installation -di           Test Docker installation"                                         
    echo ""
    echo "  --singularity_minimal -sm           Test Singularity installation and test run"
    echo "                                      with minimal configuration"
    echo ""
    echo "  --singularity_extended -se          Test Singularity installation and test run"
    echo "                                      with extended configuration"
    echo ""
    echo "  --singularity_installation -si      Test Singularity installation"                                         
    echo ""
    echo "  --fail-number -fn                   Allowed Number of logfiles without reported"
    echo "                                      success. Default: 4"                                         
    echo ""
    echo "  --use-requirements -ur              Use requirements.txt when installing"
    echo "                                      MeerKATHI"
    echo ""
    echo "  --omit-stimela-reinstall -os        Do not re-install stimela images"
    echo ""
    echo "  --delete-singularity-cache -ds      Delete singularity cache prior to test"
    echo ""
    echo "  --force -f                          Force replacement and re-installation of" 
    echo "                                      all components (you will probably want" 
    echo "                                      that)"
    echo "  --override -or                      Override security question (showing root\n"
    echo "                                      directory and asking whether to proceed.)"
    echo ""
fi


if [[ -n "$VE" ]]
then
    echo ""
    echo " The script creates a root directory"
    echo " (Notice that all environment variables can also be supplied via the command line)"
    echo " \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_NUMBER, where"
    echo " CARATE_WORKSPACE is an environment variable containing the path of a"
    echo " root directory to all tests done with this script. The variable"
    echo " CARATE_CARACAL_TEST_NUMBER is identical to the environment variable"
    echo " CARATE_CARACAL_BUILD_NUMBER if that is set by the user, and has to"
    echo " be supplied independently (i.e. to be defined prior to the script"
    echo " call) as an environment variable if CARATE_CARACAL_BUILD_NUMBER is"
    echo " not defined. The rationale behind that is that the test directory is"
    echo " always linked to a git(hub) build number if that exists. Otherwise, if"
    echo " CARATE_CARACAL_BUILD_NUMBER is not defined, the user can supply an"
    echo " alternative name \$CARATE_CARACAL_TEST_NUMBER. In the test root"
    echo " directory \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_NUMBER, a home"
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
    echo "  \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_NUMBER):"
    echo ""
    echo "  - a directory called home is created (if not existing or if -f is set)"
    echo "    and the HOME environment variable set to that home directory"
    echo ""
    echo "  - a python 3 virtual environment name caracal_venv is created (if not"
    echo "    existing or if -f is set) and activated"
    echo ""
    echo "  - caracal is either downloaded to the root directory (if not existing"
    echo "    or if -f is set) from https://github.com/ska-sa/meerkathi or, if the"
    echo "    CARATE_LOCAL_SOURCE environment variable is set,"
    echo "    \$CARATE_LOCAL_SOURCE is copied to the root directory (if not"
    echo "    existing or if -f is set, notice that the directory tree should be a"
    echo "    valid meerkathi tree, ready for installation)"
    echo ""
    echo "  - the caracal version CARATE_CARACAL_BUILD_NUMBER is checked out"
    echo "    using git if CARATE_CARACAL_BUILD_NUMBER is defined"
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
    echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
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
    echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
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
    echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
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
    echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_extended_docker"
    echo "    directory and extendedConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then meerkathi is run with"
    echo "    extendedConfig.yml and declared successful if certain expected files"
    echo "    are created."
    echo ""
    echo "  - when environment variable CARATE_CONFIG_SOURCE is set in combination with"
    echo "    switches --singularity_installation or -si set, then that yaml configuration"
    echo "    source is used for a further singularity test in the directory"
    echo "    test_prefix_singularity, where prefix is the prefix of the yaml file. The line"
    echo "    dataid: [''] in that file is replaced by the appropriate line to process the "    
    echo "    test data sets in \$CARATE_TEST_DATA_DIR"
    echo ""
    echo "  - when environment variable CARATE_CONFIG_SOURCE is set in combination with"
    echo "    switches --docker_installation or -di set, then that yaml configuration"
    echo "    source is used for a further singularity test in the directory"
    echo "    test_prefix_singularity, where prefix is the prefix of the yaml file. The line"
    echo "    dataid: [''] in that file is replaced by the appropriate line to process the "    
    echo "    test data sets in \$CARATE_TEST_DATA_DIR"
    echo ""
    echo " For each test run, all logfiles created by CARACal are parsed. If the last two" 
    echo " lines contain the word \"successfully\", the single logged process is counted as"
    echo " success, as a failure otherwise. The number of allowed fails can be set using the"
    echo " argument --fail-number or -fn followed by the number of allowed fails. carate will"
    echo " abort and return 1 if the number of detected fails is larger than the number of"
    echo " allowed fails. carate will also abort and return 1 if the logfile created/changed"
    echo " just before log-meerkathi.txt (which is always the last) is deemed i nvalid."
    echo ""
    echo " Note that in particular Stimela has components that are external to"
    echo " the root directory and will be touched by this test.  "
    echo ""
    echo "Example 1:"
    echo "Testing a pull request with commit/build number 6d562c... using Docker"
    echo "and installing using requirements.txt"
    echo "  $carate.sh -dm -ur -f"
    echo ""
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
# [ -n "$CARATE_JOB_NAME" ] || { printf "You have to define a global CARATE_JOB_NAME variable,
# like (if you're\nusing bash):\n$ export CARATE_JOB_NAME="CARCal test"\n\n"; kill "$PPID"; exit 1; }
[[ -n "$CARATE_WORKSPACE" ]] || { \
    printf "You have to define a global CARATE_WORKSPACE variable, like (if you're\nusing bash):\n";\
    printf "$ export CARATE_WORKSPACE=\"/home/username/meerkathi_tests\"\n";\
    printf "It is the top level directory of all tests.\n\n";\
    kill "$PPID"; exit 1;\
}

[[ -n "$CARATE_TEST_DATA_DIR" ]] || { \
    printf "You have to define a global CARATE_TEST_DATA_DIR variable, like (if\n";\
    printf "you're using bash):\n";\
    printf "$ export CARATE_TEST_DATA_DIR=\"/home/username/meerkathi_tests/rawdata\"\n";\
    printf "And put test rawdata therein: a.ms  b.ms c.ms ...\n";\
    printf "These test data will be copied across for the test.\n\n";\
    kill "$PPID"; exit 1;\
}

[[ -n "$CARATE_CARACAL_BUILD_NUMBER" ]] || { \
    printf "You can define a global variable CARATE_CARACAL_BUILD_NUMBER, like (if you're\n";\
    printf "using bash):\n";\
    printf "$ export CARATE_CARACAL_BUILD_NUMBER=\"b027661de6ff93a183ff240b96af86583932fc1e\"\n";
    printf "You can find the build number when running e.g.\n";\
    printf "$ git log        in your MeerKATHI folder.\n";\
    printf "Or you can look up the build number in github.\n";\
    printf "If defined this test will check out that build number.\n";\
    printf "If not defined, either the local installation or the current remote master\n";\
    printf "will be checked out.\n\n";\
}

# Force test number to be identical with build number, if it is defined
[[ -z "$CARATE_CARACAL_BUILD_NUMBER" ]] || { \
    export CARATE_CARACAL_TEST_NUMBER=$CARATE_CARACAL_BUILD_NUMBER; \
}

[[ -n "$CARATE_CARACAL_TEST_NUMBER" ]] || { \
    printf "Without build number you have to define a global CARATE_CARACAL_TEST_NUMBER\n";\
    printf "variable, giving your test directory a name, like (if you're using bash):\n";\
    printf "$ export CARATE_CARACAL_TEST_NUMBER=\"b027661de6ff93a183ff240b96af86583932fc1e\"\n";\
    printf "Otherwise choose any unique identifyer\n\n";\
    kill "$PPID"; exit 1; \
}

[[ -n "$CARATE_LOCAL_SOURCE" ]] || { \
    printf "The global variable CARATE_LOCAL_SOURCE is not set, meaning that MeerKATHI\n";\
    printf "will be downloaded from https://github.com/ska-sa/meerkathi\n\n";\
}

[[ -n "$CARATE_CONFIG_SOURCE" ]] || { \
    printf "The global variable CARATE_CONFIG_SOURCE is not set, meaning that no CARACal\n";\
    printf "test will be made on own supplied configuration.\n\n";\
}

# Start test
echo "##########################################"
echo "CARACal test $CARATE_CARACAL_TEST_NUMBER"
echo "##########################################"
echo

[[ -e $CARATE_WORKSPACE ]] || {echo "The workspace direcotyr $CARATE_WORKSPACE" does not yet exist.}

# Create workspace
WORKSPACE_ROOT="$CARATE_WORKSPACE/$CARATE_CARACAL_TEST_NUMBER"

# Check if all's well, force reply by user
echo "The directory"
echo "$CARATE_WORKSPACE/$CARATE_CARACAL_TEST_NUMBER"
echo "and its content will be created/changed."
echo "The directory $CARATE_WORKSPACE/.singularity might be created/changed"
if [[ -z $OR ]]
then
    echo "Is that ok (Yes/No)?"
    read proceed
    [[ $proceed == "Yes" ]] || { echo "Cowardly quitting"; kill "$PPID"; exit 1; }
fi


# Check if workspace_root exists if we do not use force
if (( $FORCE==0 ))
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

if [[ -n "$CARATE_CONFIG_SOURCE" ]]
then
    if [[ -z $DI ]] && [[ -z $SI ]]
    then
 echo "No Stimela installation made in context with specifying an additional config"
 echo "file. Ommitting testing that file"
    else
 # Get the config file name
 configfilename=`echo $CARATE_CONFIG_SOURCE | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}'`
 echo $configfilename 
    fi
fi

# Search for test data and set variable accordingly
if [[ -n $DM ]] || [[ -n $DE ]] || [[ -n $SM ]] || [[ -n $SE ]] 
then
    if [[ -e $CARATE_TEST_DATA_DIR ]]
    then
        # Check if there are any ms files
 mss=`find $CARATE_TEST_DATA_DIR -name *.ms`
 [[ ! -z "$mss" ]] || { printf "Test data required in $CARATE_TEST_DATA_DIR \n"; kill "$PPID"; exit 1; }
 
 # This generates the dataid string
 dataidstr=`ls -d $CARATE_TEST_DATA_DIR/*.ms | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}' | sed '{s/ /\x27,\x27/g; s/$/\x27\]/; s/^/dataid: \[\x27/}'`
    else
 printf "Create directory $CARATE_TEST_DATA_DIR and put test rawdata\n";\
 printf "therein: a.ms b.ms c.ms ...\n"
 kill "$PPID"; exit 1;
    fi
fi


# The following would only work in an encapsulated environment
export HOME=$WORKSPACE_ROOT/home
####(( FORCE==0 )) || { rm -rf $HOME; }
mkdir -p $HOME

# Create virtualenv and start
echo "##########################################"
echo "Building virtualenv in $WORKSPACE_ROOT"
echo "##########################################"
echo
####(( FORCE==0 )) || { rm -rf ${WORKSPACE_ROOT}/caracal_venv; }
####[[ -d ${WORKSPACE_ROOT}/caracal_venv ]] || { virtualenv -p python3 ${WORKSPACE_ROOT}/caracal_venv; }
echo "Entering virtualenv in $WORKSPACE_ROOT"
. ${WORKSPACE_ROOT}/caracal_venv/bin/activate
####pip install pip setuptools wheel -U

# Install software
echo
echo "##########################################"
echo "Fetching CARACal"
echo "##########################################"
echo
####(( FORCE==0 )) || { rm -rf ${WORKSPACE_ROOT}/meerkathi; }
if [[ -n "$CARATE_LOCAL_SOURCE" ]]
then
    if [[ -e ${WORKSPACE_ROOT}/meerkathi ]]
    then
 echo "Not re-fetching MeerKATHI, use -f if you want that."
    else
 #### cp -r ${CARATE_LOCAL_SOURCE} ${WORKSPACE_ROOT}/
 pass
    fi
else
    cd ${WORKSPACE_ROOT}
    if [[ -e ${WORKSPACE_ROOT}/meerkathi ]]
    then
     if (( FORCE==0 ))
        then
     echo "Not re-fetching MeerKATHI, use -f if you want that."
        else
     ####     rm -rf ${WORKSPACE_ROOT}/meerkathi
     ####     git clone https://github.com/ska-sa/meerkathi.git
     pass
 fi
    else
 #### git clone https://github.com/ska-sa/meerkathi.git
 pass
    fi
fi
if [[ -n "$CARATE_CARACAL_BUILD_NUMBER" ]]
then
    cd meerkathi
    [[ -z $CARATE_LOCAL_SOURCE ]] || { \
 echo "If an error occurs here, it likely means that the local installation";\
 echo "of CARACal does not contain the build number. You may want to use the";\
 echo "master branch and unset the environmrnt variable CARATE_CARACAL_BUILD_NUMBER:";\
 echo "In bash: $ unset CARATE_CARACAL_BUILD_NUMBER";\
    }
####    git checkout ${CARATE_CARACAL_BUILD_NUMBER}
fi
          
echo
echo "##########################################"
echo "Installing CARACal"
echo "##########################################"
echo

#PATH=${WORKSPACE}/projects/pyenv/bin:$PATH
#LD_LIBRARY_PATH=${WORKSPACE}/projects/pyenv/lib:$LD_LIBRARY_PATH
####pip install -U -I ${WORKSPACE_ROOT}/meerkathi\[beta\]
if [[ -n $UR ]]
then
    echo "Intstalling requirements.txt"
####    pip install -U -I -r ${WORKSPACE_ROOT}/meerkathi/requirements.txt
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
      #### rm -f $HOME/.stimela/*
       echo "Running $docker system prune"
      #### docker system prune
      #### stimela build
    fi
fi

if [[ -n $SM ]] || [[ -n $SE ]] || [[ -n $SI ]]
then
    # This sets the singularity image folder to the test environment
    export SINGULARITY_CACHEDIR=$CARATE_WORKSPACE/.singularity
    if (( FORCE==0 )) || [[ -n $ORSR ]]
    then
        if [[ -e ${WORKSPACE_ROOT}/stimela_singularity ]]
        then
            echo "Will not re-create existing stimela_singularity and use old installation."
            echo "Use -f to override and unset -or or --omit-stimela-reinstall flags."
        fi
    else
        rm -rf ${WORKSPACE_ROOT}/stimela_singularity
        [[ -z $DS ]] || { rm -rf $CARATE_WORKSPACE/.singularity; }
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

testingoutput () {
    # Function to test output after running a pipeline
    # Argument 1: $WORKSPACE_ROOT
    # Argument 2: Test directory, e.g. test_extendedConfig_docker
    # Argument 3: Number of logs which are allowed to fail
    # Argument 4: If set, check for success in the file changed before log-meerkathi.txt was last changed
    echo
    echo "#####################"
    echo "# Checking logfiles #"
    echo "#####################"
    echo
    echo "Checking logfiles in directory ${1}/${2}"
    echo "Will return error if $2 files do not report success in the last two lines"
    [[ -z $4 ]] || echo "Will also return error if no success is reported in the file"
    [[ -z $4 ]] || echo "changed before log-meerkathi.txt was last changed"
    allogs=`ls -t ${1}/${2}/output/logs/`
    total=$(( 0 ))
    failed=$(( 0 ))

    for log in $allogs
    do
        lastlines=`tail -2 ${1}/${2}/output/logs/$log`
        if ! grep -q successfully <<< $lastlines
        then
            echo "$log does not report success in the last two lines"
            (( failed+=1 ))
            [[ -z $hadmeerkathi ]] || failedbeforemeerkathi=1
        fi
        (( total+=1 ))

	unset hadmeerkathi
        if [[ $log == "log-meerkathi.txt" ]]
        then
            hadmeerkathi=1 
        fi
    done
    echo "$failed logfiles of $total logfiles did not report success in the last two lines"

    # Notice that 0 is true in bash
    (( $3 >= $failed)) || return 0
    [[ -z $4 ]] || [[ -z $failedbeforemeerkathi ]] || return 0
    return 1
}

runtest () {
    # Running a specific caracal test using a specific combination of configuration file, architecture, and containerization
    # Argument 1: Line appearing at the start of function
    # Argument 2: $WORKSPACE_ROOT
    # Argument 3: configuration file name without "yml"
    # Argument 4: containerisation architecture "docker" or "singularity"
    # Argument 5: delete existing files or not
    # Argument 6: Number of logs which are allowed to fail
    # Argument 7: Switches to pass to meerkathi
 
    local greetings_line=$1
    local WORKSPACE_ROOT=$2
    local configfilename=$3
    local contarch=$4
    local FORCE=$5
    local FN=$6
    local caracalswitches=$7
    
    echo
    echo "##########################"
    echo "$greetings_line "
    echo "##########################"
    echo

    if [[ -e ${WORKSPACE_ROOT}/test_${configfilename}_${contarch} ]] && (( FORCE==0 ))
    then
        echo "Will not re-create existing directory ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}"
        echo "and use old results. Use -f to override."
    else
 #### rm -rf ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}
 
    echo "Preparing ${contarch} test (using ${configfilename}.yml) in"
    echo "${WORKSPACE_ROOT}/test_${configfilename}_${contarch}"
 #### mkdir -p ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/msdir
    sed "s/dataid: \[\x27\x27\]/$dataidstr/" ${WORKSPACE_ROOT}/meerkathi/meerkathi/sample_configurations/${configfilename}.yml > ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/${configfilename}.yml
 #### cp -r $CARATE_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/msdir/

    echo "Running extended ${contarch} test (using ${configfilename}.yml)"
    cd ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}

 # Notice that currently all output will be false, such that || true is required to ignore this
 #### meerkathi -c ${configfilename}.yml ${caracalswitches} || true
    fi

    echo "Checking output of ${configfilename} ${contarch} test"
    echo testingoutput ${WORKSPACE_ROOT} test_${configfilename}_${contarch} $FN yes
    if testingoutput ${WORKSPACE_ROOT} test_${configfilename}_${contarch} $FN yes
    then
	echo
	echo "######################"
	echo "# carate test failed #"
	echo "######################"
	echo	
        kill "$PPID"
	exit 1
   fi
}

if [[ -n $DM ]]
then
    greetings_line="Docker: minimalConfig"
    confilename="minimalConfig"
    contarch="docker"
    caracalswitches=" "
    runtest $greetings_line ${WORKSPACE_ROOT} $confilename $contarch $FORCE $FN $caracalswitches
fi

if [[ -n $DE ]]
then
    greetings_line="Docker: extendedConfig"
    confilename="extendedConfig"
    contarch="docker"
    caracalswitches=" "
    runtest $greetings_line ${WORKSPACE_ROOT} $confilename $contarch $FORCE $FN $caracalswitches
fi

if [[ -n $SE ]]
then
    greetings_line="Singularity: extendedConfig"
    confilename="extendedConfig"
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${WORKSPACE_ROOT}/stimela_singularity"
    runtest $greetings_line ${WORKSPACE_ROOT} $confilename $contarch $FORCE $FN $caracalswitches
fi

if [[ -n $SM ]]
then
    greetings_line="Singularity: minimalConfig"
    confilename="minimalConfig"
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${WORKSPACE_ROOT}/stimela_singularity"
    runtest $greetings_line ${WORKSPACE_ROOT} $confilename $contarch $FORCE $FN $caracalswitches
fi


if [[ -n $configfilename ]]
then
    if [[ -n $SI ]]
    then
        greetings_line="Singularity: $configfilename"
        confilename=$configfilename
        contarch="singularity"
        caracalswitches="--container-tech singularity -sid ${WORKSPACE_ROOT}/stimela_singularity"
        runtest $greetings_line ${WORKSPACE_ROOT} $confilename $contarch $FORCE $FN $caracalswitches
    fi
    if [[ -n $DI ]]
    then
        greetings_line="Docker: $configfilename"
        confilename=$configfilename
        contarch="docker"
        caracalswitches=" "
        runtest $greetings_line ${WORKSPACE_ROOT} $confilename $contarch $FORCE $FN $caracalswitches
    fi
fi

echo "Wow, everything seems to be ok"
exit 0
