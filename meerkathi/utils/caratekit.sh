#!/usr/bin/env bash
# The following ensures the script to stop on errors
set -e

# The Carate Kid Mr. Myagi quotes, 16 fail quotes
kkfailquotes=( \
    "No such thing as bad student, only bad teacher. Teacher say, student do." \
	"Here are the 2 Rules of Miyagi-Ryu Carate. Rule Number 1: 'Carate for defense only.' Rule Number 2: 'First learn rule number 1.'" \
	"We make sacred pact. I promise teach carate to you, you promise learn. I say, you do, no questions." \
	"Never trust spiritual leader who cannot dance." \
	"Lesson not just carate only. Lesson for whole life. Whole life have a balance. Everything be better." \
	"Walk on road, hm? Walk left side, safe. Walk right side, safe. Walk middle, sooner or later get squish just like grape." \
	"Daniel-San, lie become truth only if person wanna believe it." \
	"Wax on, wax off. Wax on, wax off." \
	"If carate used defend honor, defend life, carate mean something. If carate used defend plastic metal trophy, carate no mean nothing." \
	"We make sacred pact. I promise teach carate to you, you promise learn. I say, you do, no questions." \
	"Miyagi: Oh, Daniel-san, you too much by self, not good." \
	"Better learn balance. Balance is key. Balance good, carate good. Everything good. Balance bad, better pack up, go home. Understand?" \
	"Never put passion in front of principle, even if you win, you’ll lose." \
	"Only root carate come from Miyagi. Just like bonsai choose own way grow because root strong you choose own way do carate same reason." \
	"Either you carate do 'yes' or carate do 'no.' You carate do 'guess so,' (get squished) just like grape." \
	"Just remember, license never replace eye, ear, and brain." \
)

# 2 sucess quotes
kksuccessquotes=( \
    "If come from inside you, always right one." \
	"Man who catch fly with chopstick, accomplish anything." \
)

success=0

# Preamble
printf "\n"
printf "#########################\n"
printf " Testing CARACal at home \n"
printf "#########################\n"
echo ""
echo "caratekit.sh $*"
echo ""
 sya="###########################" ; sya+=$'\n\n'
sya+=" Caratekit system information"  ; sya+=$'\n\n'
sya+="###########################" ; sya+=$'\n\n'
sya+="Start time: "; sya+=`date -u`; sya+=$'\n'
sya+="Call executing: caratekit.sh $*"; sya+=$'\n\n'
sya+="###########################" ; sya+=$'\n\n'
sya+="Host info: ";sya+=$'\n'; sya+=`hostnamectl | grep -Ev "Machine ID"'|'"Boot ID"`;sya+=$'\n'
#sya+=$'\n\n'

# Control if help should be switched on
#(( $# > 0 )) || NOINPUT=1

# Default variables
FORCE=0
SS="/dev/null"
IA=5

# current working directory
cwd=`pwd`

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
    if [[ "$arg" == "--docker-minimal" ]] || [[ "$arg" == "-dm" ]]
    then
        DM=1
    fi
    if [[ "$arg" == "--docker-alternative" ]] || [[ "$arg" == "-da" ]]
    then
        DA=1
    fi
    if [[ "$arg" == "--docker-installation" ]] || [[ "$arg" == "-di" ]]
    then
        DI=1
    fi
    if [[ "$arg" == "--singularity-minimal" ]] || [[ "$arg" == "-sm" ]]
    then
        SM=1
    fi
    if [[ "$arg" == "--singularity-alternative" ]] || [[ "$arg" == "-sa" ]]
    then
        SA=1
    fi
    if [[ "$arg" == "--singularity-installation" ]] || [[ "$arg" == "-si" ]]
    then
        SI=1
    fi
    if [[ "$arg" == "--use-stimela-stable" ]] || [[ "$arg" == "-us" ]]
    then
        US=1
    fi
    if [[ "$arg" == "--use-stimela-master" ]] || [[ "$arg" == "-um" ]]
    then

	[[ -z ${US} ]] ||  { echo "You can use only one of -us (--use-stimela-stable) or -um (--use-stimela-master), stopping."; kill "$PPID"; exit 1; }
        UM=1
    fi
    if [[ "$arg" == "--singularity-root" ]] || [[ "$arg" == "-sr" ]]
    then
        SR=1
    fi
    if [[ "$arg" == "--install-attempts" ]] || [[ "$arg" == "-ia" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --install-attempt or -ia switch, stopping."; kill "$PPID"; exit 1; }
        IA=${!nextcount}

    fi
    if [[ "$arg" == "--install-attempts" ]] || [[ "$arg" == "-ia" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --install-attempt or -ia switch, stopping."; kill "$PPID"; exit 1; }
        IA=${!nextcount}
    fi
    if [[ "$arg" == "--omit-stimela-reinstall" ]] || [[ "$arg" == "-os" ]]
    then
        ORSR=1
    fi
    if [[ "$arg" == "--omit-docker-prune" ]] || [[ "$arg" == "-op" ]]
    then
        OP=1
    fi
    if [[ "$arg" == "--force" ]] || [[ "$arg" == "-f" ]]
    then
        FORCE=1
    fi
    if [[ "$arg" == "--fastsim" ]] || [[ "$arg" == "-fs" ]]
    then
        FS=1
    fi
    if [[ "$arg" == "--override" ]] || [[ "$arg" == "-or" ]]
    then
        OR=1
    fi
    if [[ "$arg" == "--keep-home" ]] || [[ "$arg" == "-kh" ]]
    then
        KH=1
    fi
    if [[ "$arg" == "--pull-docker" ]] || [[ "$arg" == "-pd" ]]
    then
        PD=1
    fi
    if [[ "$arg" == "--workspace" ]] || [[ "$arg" == "-ws" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --workspace or -ws switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_WORKSPACE=${!nextcount}
	firstletter=`echo ${CARATE_WORKSPACE} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_WORKSPACE="${cwd}/${CARATE_WORKSPACE}" 
    fi
    if [[ "$arg" == "--test-data-dir" ]] || [[ "$arg" == "-td" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --test-data-dir or -td switch, stopping."; kill "$PPID"; exit 1; }
    
 CARATE_TEST_DATA_DIR=${!nextcount}
 	firstletter=`echo ${CARATE_TEST_DATA_DIR} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_TEST_DATA_DIR="${cwd}/${CARATE_TEST_DATA_DIR}" 
    fi
    if [[ "$arg" == "--caracal-build-number" ]] || [[ "$arg" == "-cb" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --caracal-build-number or -cb switch, stopping."; kill "$PPID"; exit 1; }    
 CARATE_CARACAL_BUILD_NUMBER=${!nextcount}
    fi
    if [[ "$arg" == "--caracal-test-number" ]] || [[ "$arg" == "-ct" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --caracal-test-number or -ct switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_CARACAL_TEST_ID=${!nextcount}
    fi
    if [[ "$arg" == "--local-source" ]] || [[ "$arg" == "-ls" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --local-source or -ls switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_LOCAL_SOURCE=${!nextcount}
 	firstletter=`echo ${CARATE_LOCAL_SOURCE} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_LOCAL_SOURCE="${cwd}/${CARATE_LOCAL_SOURCE}" 
    fi
    if [[ "$arg" == "--config-source" ]] || [[ "$arg" == "-cs" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --config-source or -cs switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_CONFIG_SOURCE=${!nextcount}
 	firstletter=`echo ${CARATE_CONFIG_SOURCE} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_CONFIG_SOURCE="${cwd}/${CARATE_CONFIG_SOURCE}" 
    fi
#    if [[ "$arg" == "--small-script" ]] || [[ "$arg" == "-ss" ]]
#    then
#        (( nextcount=argcount+1 ))
#        (( $nextcount <= $# )) || { echo "Argument expected for --small-script or -ss switch, stopping."; kill "$PPID"; exit 1; }
#	SS=${!nextcount}
#	firstletter=`echo ${SS} | head -c 1`
#	[[ ${firstletter} == "/" ]] || SS="${cwd}/${SS}" 
#    fi
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
    echo "                               format)"
    echo ""

    echo "  CARATE_CARACAL_BUILD_NUMBER: Build number to test. If not set, master"
    echo "                               will be tested"
    echo ""
    
    echo "  CARATE_CARACAL_TEST_ID:      Only specify if CARATE_CARACAL_BUILD_NUMBER"
    echo "                               is undefined. All data and installations for"
    echo "                               a specific test will be saved in the directory"
    echo "                               \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID." 
    echo "                               CARATE_CARACAL_TEST_ID is changed to"
    echo "                               CARATE_CARACAL_BUILD_NUMBER"
    echo "                               if CARATE_CARACAL_BUILD_NUMBER is defined."
    echo ""
    echo "  CARATE_LOCAL_SOURCE:         Local CARACal/MeerKATHI copy to use. If not"
    echo "                               set, CARACal/MeerKATHI will be downloaded"
    echo "                               from https://github.com/ska-sa/meerkathi"
    echo ""
    echo "  CARATE_CONFIG_SOURCE:        Local configuration file copy to use for an" 
    echo "                               additional test"
    echo ""
    echo "Switches:"
    echo ""
    echo "  --help -h                           Show help"
    echo ""
    echo "  --verbose -v                        Show verbose help"
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
    echo "                                      CARATE_CARACAL_TEST_ID"
    echo ""
    echo "  --local-source ARG -ls ARG          Use ARG instead of environment variable"
    echo "                                      CARATE_LOCAL_SOURCE"
    echo ""
    echo "  --config-source ARG -cs ARG         Use ARG instead of environment variable"
    echo "                                      CARATE_CONFIG_SOURCE"
    echo ""
    echo "  --keep-home -kh                     Do not change the HOME environment"
    echo "                                      variable during installation test"
    echo ""
    echo "  --docker-minimal -dm                Test Docker installation and test run with"
    echo "                                      minimal configuration"
    echo ""
    echo "  --docker-alternative -da            Test Docker installation and test run with"
    echo "                                      alternative configuration carateConfig.yml"
    echo ""
    echo "  --docker-installation -di           Test Docker installation"                                         
    echo ""
    echo "  --pull-docker -pd                   run stimela pull -d before stimela build"
    echo "                                      omit the step when switch is not set"
    echo ""
    echo "  --omit-docker-prune -op             Do not prune system during docker install"
    echo ""
    echo "  --singularity-minimal -sm           Test Singularity installation and test run"
    echo "                                      with minimal configuration"
    echo ""
    echo "  --singularity-alternative -sa       Test Singularity installation and test run"
    echo "                                      alternative configuration carateConfig.yml"
    echo ""
    echo "  --singularity-installation -si      Test Singularity installation"              
    echo ""
    echo "  --singularity-root -sr              Do not install Singularity images in"
    echo "                                      global \$CARATE_WORKSPACE but in the"
    echo "                                      specific root directory (can then not be"
    echo "                                      re-used)"
    echo ""
    echo "  --install-attempts -ia              Allowed number of attempts to pull images"
    echo "                                      or to run stimela build"
    echo ""
    echo "  --use-stimela-master -um            Use"
    echo "                                      pip install -U --force-reinstall -r (...)stimela_master.txt"
    echo "                                      when installing MeerKATHI"
    echo ""

    echo "  --use-stimela-stable -us            Use"
    echo "                                      pip install -U --force-reinstall -r (...)stimela_last_stable.txt"
    echo "                                      when installing MeerKATHI"
    echo ""
    echo "  --omit-stimela-reinstall -os        Do not re-install stimela"
    echo ""
    echo "  --force -f                          Force replacement and re-installation of" 
    echo "                                      all components (you will probably want" 
    echo "                                      that)"
    echo ""
    echo "  --fastsim -fs                       Omit all time-consuming steps" 
    echo ""
    echo "  --override -or                      Override security question (showing root"
    echo "                                      directory and asking whether to proceed.)"
#    echo "  --small-script ARG -ss ARG          Generate a small script ARG showing all"
#    echo "                                      steps taken by carate"
    echo ""
fi

if [[ -n "$VE" ]]
then
    echo ""
    echo " The script creates a root directory"
    echo " (Notice that all environment variables can also be supplied via the command"
    echo "  line) \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID, where"
    echo " CARATE_WORKSPACE is an environment variable containing the path of a"
    echo " parent directory to all tests done with this script. The variable"
    echo " CARATE_CARACAL_TEST_ID is identical to the environment variable"
    echo " CARATE_CARACAL_BUILD_NUMBER if that is set by the user, and has to"
    echo " be supplied independently (i.e. to be defined prior to the script"
    echo " call or supplied using switches --caracal-test-number or -ct) as an environment"
    echo " variable if CARATE_CARACAL_BUILD_NUMBER is"
    echo " not defined. The rationale behind that is that the test directory is"
    echo " always linked to a git(hub) build number if that exists. Otherwise, if"
    echo " CARATE_CARACAL_BUILD_NUMBER is not defined, the user can supply an"
    echo " alternative name \$CARATE_CARACAL_TEST_ID. In the test root"
    echo " directory \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID, a home"
    echo " directory called home, a virtual environment called"
    echo " caracal_virtualenv, a CARACal copy meerkathi, and up to six test"
    echo " directories are created, within which the tests are conducted. If the"
    echo " --force or -f switch is set, all existing directories and"
    echo " installations, apart from a potentially existing singularity cache directory"
    echo " are deleted and replaced, if not, only those directories"
    echo " and files are created, which do not exist yet. Exceptions from that rule"
    echo " are set with the --omit-stimela-reinstall or -os switch, which would"
    echo " prevent a re-installation of stimela even if -f is set. This includes the"
    echo " Re-installation of the virtual environment, the home directory, and"
    ehco " the file .stimela in the home directory."
    echo ""
    echo "  In detail (all installations in the root directory"
    echo "  \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID):"
    echo ""
    echo "  - a directory called home is created (if not existing or if -f is set)"
    echo "    and the HOME environment variable set to that home directory unless"
    echo "    the --keep-home or -kh switches are set"
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
    echo "  - caracal is installed via pip"
    echo ""
    echo "  - if --use-stimela-master or -um switch is set,"
    echo "    meerkathi/stimela_master.txt is installed via pip"
    echo ""
    echo "  - if --use-stimela-stable or -us switch is set,"
    echo "    meerkathi/stimela_last_stable.txt is installed via pip"
    echo ""
    echo "  - when switches --docker-minimal, -dm, --docker-alternative, -da,"
    echo "    --docker-installation, -di are set, home/.stimela is removed, docker"
    echo "    system prune is invoked, and docker stimela is installed (stimela"
    echo "    build)"
    echo ""
    echo "  - when switches --pull-docker, -pd are set, stimela pull -d is invoked"
    echo "    before running stimela build for Docker installation, omit step"
    echo "    otherwise"
    echo ""
    echo "  - when switches --singularity-minimal, -sm, --singularity-alternative,"
    echo "    -se, --singularity-installation, -si are set, home/.stimela is"
    echo "    removed, and singularity stimela is by default installed in the"
    echo "    directory (if not existing or if -f is set)"
    echo "    \$CARATE_WORKSPACE/stimela_singularity. If --stimela-root or"
    echo "    -sr are set, it is installed in rootfolder/stimela_singularity."
    echo "    The first variant allows to re-use the same stimela installation"
    echo "    In multiple tests"
    echo ""
    echo "  - when switch --singularity-minimal or -sm is set, a directory"
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
    echo "  - when switch --docker-minimal or -dm is set, a directory"
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
    echo "    switches --singularity-installation or -si set, then that yaml configuration"
    echo "    source is used for a further singularity test in the directory"
    echo "    test_prefix_singularity, where prefix is the prefix of the yaml file. The line"
    echo "    dataid: [''] in that file is replaced by the appropriate line to process the "    
    echo "    test data sets in \$CARATE_TEST_DATA_DIR"
    echo ""
    echo "  - when environment variable CARATE_CONFIG_SOURCE is set in combination with"
    echo "    switches --docker-installation or -di set, then that yaml configuration"
    echo "    source is used for a further singularity test in the directory"
    echo "    test_prefix_singularity, where prefix is the prefix of the yaml file. The line"
    echo "    dataid: [''] in that file is replaced by the appropriate line to process the "    
    echo "    test data sets in \$CARATE_TEST_DATA_DIR"
    echo ""
    echo " For each test run, log-meerkathi.txt is searched for keywords indicating the start"
    echo " and the end of a worker and those numbers are reported."
    echo " The test is declared failed and carate.sh returns 1 if:"
    echo "   - No logfiles are produced before CARACal finishes"
    echo "   - log-meerkathi.txt does not contain any keyword indicating that a worker has started"
    echo "   - The number of keywords in log-meerkathi.txt indicating the start of a worker differs"
    echo "     from the number of keywords in log-meerkathi.txt indicating the end of a worker."
    echo "   - If the exit status of CARACal is not 0 (success)"
    echo ""
    echo " caratekit will create a report directory \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID/report"
    echo " containing three files:"
    echo "   - a shell script \${CARATE_CARACAL_TEST_ID}.sh.txt reproducing all shell commands"
    echo "     initiated by carate.sh"
    echo "   - a file ${CARATE_CARACAL_TEST_ID}_sysinfo.txt with information about the computer and"
    echo "     the environment that was used for the test"
    echo "   - copies of the configuration files, one per run"
    echo "   - copies of the output log-meerkathi.txt, one per run"
    echo " Note that in particular Stimela has components that are external to"
    echo " the root directory and will be touched by this test.  "
    echo ""
    echo " Example 1:"
    echo " Testing a pull request with commit/build number 6d562c... using Docker and"
    echo " Singularity and installing using requirements.txt. The user is permanently"
    echo " testing CARACal and has therefore a standard directory with test files and"
    echo " a standard test location. Someone issues a pull request. The user looks up"
    echo " the build number of the corresponding branch commit in github:"
    echo " In the user's rc:"
    echo "   export CARATE_WORKSPACE=/home/tester/software/meerkathi_tests"
    echo "   export CARATE_TEST_DATA_DIR=/home/jozsa/software/meerkathi_tests/rawdata"
    echo "   export PATH=\$PATH:/home/user/software/meerkathi/meerkathi/utils/carate.sh"
    echo " or"
    echo "   setenv CARATE_WORKSPACE \"/home/tester/software/meerkathi_tests\""
    echo "   setenv CARATE_TEST_DATA_DIR=\"/home/jozsa/software/meerkathi_tests/rawdata\""
    echo "   set path = ( \$path /home/user/software/meerkathi/meerkathi/utils/carate.sh)"
    echo " Then start carate"
    echo "   \$carate.sh -dm -da -sm -sa -ur -f -cb 6d562c 2>&1 | tee carate_run.log"
    echo ""
    echo
    echo " Example 2:"
    echo " Testing a local installation using Docker only with an own configuration file"
    echo " and installing using requirements.txt. The user is permanently testing"
    echo " CARACal and has therefore a standard directory with test files and a "
    echo " standard test location.:"
    echo " In the user's rc:"
    echo "   export CARATE_WORKSPACE=/home/tester/software/meerkathi_tests"
    echo "   export CARATE_TEST_DATA_DIR=/home/jozsa/software/meerkathi_tests/rawdata"
    echo "   export PATH=\$PATH:/home/user/software/meerkathi/meerkathi/utils/carate.sh"
    echo " or"
    echo "   setenv CARATE_WORKSPACE \"/home/tester/software/meerkathi_tests\""
    echo "   setenv CARATE_TEST_DATA_DIR=\"/home/jozsa/software/meerkathi_tests/rawdata\""
    echo "   set path = ( \$path /home/user/software/meerkathi/meerkathi/utils/carate.sh)"
    echo " Then start carate"
    echo "   \$carate.sh -di -ur -f -cs $wherever/bla.yml -ct mynewthing -ss small_script.sh 2>&1 | tee carate_run.log"
    echo " Notice that bla.yml should contain the line dataid: [''], which will be"
    echo " replaced by a line containing the appropriate data sets from ../rawdata"
    echo ""
    echo ""
fi

if [[ -n "$HE" ]] || [[ -n "$VE" ]]
then
    echo "Stopping. Do not set switches --help --verbose -h -v to continue."
    kill "$PPID"; exit 1;
fi

printf "##########\n"
printf " Starting \n"
printf "##########\n"
printf "\n"

# Environment variables
#[ -n "$CARATE_JOB_NAME" ] || { printf "You have to define a global CARATE_JOB_NAME variable
# like (if you're\nusing bash):\n$ export CARATE_JOB_NAME="CARCal test"\nOr use the -\n"; kill "$PPID"; exit 1; }
[[ -n "$CARATE_WORKSPACE" ]] || { \
    printf "You have to define a global CARATE_WORKSPACE variable, like (if you're\nusing bash):\n";\
    printf "$ export CARATE_WORKSPACE=\"/home/username/meerkathi_tests\"\n";\
    printf "Or use the -ws switch. It is the top level directory of all tests.\n\n";\
    kill "$PPID"; exit 1;
}

# Create header to script
ss="workspace=${CARATE_WORKSPACE}"
ss+=$'\n'
tdfault=0
if [[ ! -n "$CARATE_TEST_DATA_DIR" ]]
then
    if [[ -n ${DM} ]] || [[ -n ${DA} ]] || [[ -n ${SM} ]] || [[ -n ${SA} ]]
    then
	tdfault=1
    else
	[[ ! -n ${CARATE_CONFIG_SOURCE} ]] || tdfault=1
    fi
else
    [[ -e ${CARATE_TEST_DATA_DIR} ]] || tdfault=1
fi
(( $tdfault == 0 )) || { \
    printf "You have to define a CARATE_TEST_DATA_DIR variable, like (if\n";\
    printf "you're using bash):\n";\
    printf "$ export CARATE_TEST_DATA_DIR=\"/home/username/meerkathi_tests/rawdata\"\n";\
    printf "Or use the -td switch.\n";\
    printf "You also have to create that directory $CARATE_TEST_DATA_DIR\n";\
    printf "and put test rawdata therein: a.ms  b.ms c.ms ...\n";\
    printf "These test data will be copied across for the test.\n\n";\
    kill "$PPID"; exit 1;
}
    
[[ ! -n "$CARATE_TEST_DATA_DIR" ]] || ss+="test_data_dir=${CARATE_TEST_DATA_DIR}"
[[ ! -n "$CARATE_TEST_DATA_DIR" ]] || ss+=$'\n'

# Force test number to be identical with build number, if it is defined
[[ -z "$CARATE_CARACAL_BUILD_NUMBER" ]] || { \
    CARATE_CARACAL_TEST_ID=$CARATE_CARACAL_BUILD_NUMBER; \
}

[[ -n "$CARATE_CARACAL_TEST_ID" ]] || { \
    printf "Without build number you have to define a global CARATE_CARACAL_TEST_ID\n";\
    printf "variable, giving your test directory a name, like (if you're using bash):\n";\
    printf "$export CARATE_CARACAL_TEST_ID=\"b027661de6ff93a183ff240b96af86583932fc1e\"\n";\
    printf "Otherwise choose any unique identifyer. You can also use the -ct switch.\n\n";\
    kill "$PPID"; exit 1;
}
ss+="caracal_test_id=${CARATE_CARACAL_TEST_ID}"
ss+=$'\n'

[[ -n "${DM}" ]] || [[ -n "${DA}" ]] || [[ -n "${DI}" ]] || [[ -n "${SM}" ]] || [[ -n "${SA}" ]] || [[ -n "${SI}" ]] || {\
    printf "Please use one of the switches -dm, -da, -di, -sm, -sa, or -si\n\n";\
    kill "$PPID"; exit 1;
}

if [[ -n "$CARATE_LOCAL_SOURCE" ]]
then
    ss+="local_source=${CARATE_LOCAL_SOURCE}"
    ss+=$'\n'
else
    printf "The variable CARATE_LOCAL_SOURCE is not set, meaning that MeerKATHI\n"
    printf "will be downloaded from https://github.com/ska-sa/meerkathi\n\n"
fi

if [[ -n "$CARATE_CONFIG_SOURCE" ]]
then
    ss+="config_source=${CARATE_CONFIG_SOURCE}"
    ss+=$'\n'
else
    printf "The variable CARATE_CONFIG_SOURCE is not set and switches"
    printf "config-source and -cs are not used meaning that no CARACal\n"
    printf "test will be made on own supplied configuration.\n\n"
fi

# Start test
echo "##########################################"
echo " CARACal test $CARATE_CARACAL_TEST_ID"
echo "##########################################"
echo
[[ -e $CARATE_WORKSPACE ]] || echo "The workspace directory $CARATE_WORKSPACE does not yet exist."

# Create workspace
ss+="workspace_root=\${workspace}/\${caracal_test_id}"
ss+=$'\n'
WORKSPACE_ROOT="$CARATE_WORKSPACE/$CARATE_CARACAL_TEST_ID"

# Save home for later 
if [[ -n $HOME ]]
then
    [[ -n ${KH} ]] || ss+="HOME_OLD=\${HOME}"
    [[ -n ${KH} ]] || ss+=$'\n'
    [[ -n ${KH} ]] || HOME_OLD=${HOME}
fi
if [[ -n ${PYTHONPATH} ]]
then
    ss+="PYTHONPATH_OLD=\${PYTHONPATH}"
    ss+=$'\n'
    PYTHONPATH_OLD=${PYTHONPATH}
fi

# This ensures that when stopping, the $HOME environment variable is restored
# Variable defininition ends here in script
ss+=""
ss+=$'\n'

# Check if all's well, force reply by user
echo "The directory"
echo "$CARATE_WORKSPACE/$CARATE_CARACAL_TEST_ID"
echo "and its content will be created/changed."
echo "The directory $CARATE_WORKSPACE/.singularity might be created/changed"
echo ""

if [[ -z ${OR} ]]
then
    echo "Is that ok (Yes/No)?"
    no_response=true
    while [ "$no_response" == true ]; do
	read proceed
	case "$proceed" in
	    [Yy][Ee][Ss]|[Yy]) # Yes or Y (case-insensitive).
	      no_response=false
              ;;
	    [Nn][Oo]|[Nn])  # No or N.
	      { echo "Cowardly quitting"; kill "$PPID"; exit 1; }
              ;;
	    *) # Anything else (including a blank) is invalid.
	      { echo "That is not a valid response."; }
              ;;
	esac
    done
#    [[ $proceed == "Yes" ]] || { echo "Cowardly quitting"; kill "$PPID"; exit 1; }
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
echo " Setting up build in $WORKSPACE_ROOT"
echo "##########################################"
echo

#(( $FORCE==0 )) || { rm -rf $WORKSPACE_ROOT; }
ss+="mkdir -p \${workspace_root}"
mkdir -p ${WORKSPACE_ROOT}

[[ ! -d ${WORKSPACE_ROOT}/report ]] || (( ${FORCE}==0 )) || rm -rf ${WORKSPACE_ROOT}/report
mkdir -p ${WORKSPACE_ROOT}/report

# Small script, use txt suffix to be able to upload to multiple platforms
SS=${WORKSPACE_ROOT}/report/${CARATE_CARACAL_TEST_ID}.sh.txt

# Empty ss into the small script
[[ ! -d ${SS} ]] || (( $FORCE==0 )) || rm -rf ${SS}
echo "$ss" >> ${SS}

# Sysinfo
SYA=${WORKSPACE_ROOT}/report/${CARATE_CARACAL_TEST_ID}_sysinfo.txt

# Empty into the sysinfo
[[ ! -d ${SYA} ]] || (( $FORCE==0 )) || rm -rf ${SYA}
echo "$sya" >> ${SYA}

if [[ -n "$CARATE_CONFIG_SOURCE" ]]
then
    if [[ -z $DI ]] && [[ -z $SI ]]
    then
        echo "No Stimela installation made in context with specifying an additional config"
        echo "file. Ommitting testing that file"
    else
 # Get the config file name
        configfilename=`echo $CARATE_CONFIG_SOURCE | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}'`
    fi
fi

# Search for test data and set variable accordingly
if [[ -e ${CARATE_TEST_DATA_DIR} ]]
then
    # Check if there are any ms files
    mss=`find ${CARATE_TEST_DATA_DIR} -name *.ms`
    [[ ! -z "$mss" ]] || { printf "Test data required in ${CARATE_TEST_DATA_DIR} \n"; kill "$PPID"; exit 1; }
    
    # This generates the dataid string
    dataidstr=`ls -d ${CARATE_TEST_DATA_DIR}/*.ms | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}' | sed '{s/ /\x27,\x27/g; s/$/\x27\]/; s/^/dataid: \[\x27/}'`
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    sya=`ls -d ${CARATE_TEST_DATA_DIR}/*.ms | sed '{s=.*/==}' | sed '{:q;N;s/\n/, /g;t q}'`
    echo "Test data: ${sya}" >> ${SYA}
    # Size of test data
    outsize=`du -ms ${CARATE_TEST_DATA_DIR} | awk '{print $1}'`
    echo "Total size of test data: ${outsize} MB" >> ${SYA}
    echo "" >> ${SYA}    
fi

function cleanup {
    if (( success==0 ))
    then
	echo "##########################################"
	echo ""
	echo ${kkfailquotes[$(( $RANDOM % 16 ))]}
	echo "Caratekit failed."
	echo ""
	echo "##########################################"
	echo ""
    fi
    
    [[ -n ${KH} ]] || echo "export HOME=\${OLD_HOME}" >> ${SS}
    [[ -n ${KH} ]] || export HOME=${OLD_HOME}
    if [[ -n ${PYTHONPATH_OLD} ]]
    then
	echo "export PYTHONPATH=\${PYTHONPATH_OLD}" >> ${SS}
	export PYTHONPATH=${PYTHONPATH_OLD}
    fi
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    sya=" End time: "; sya+=`date -u`;
    echo "${sya}" >> ${SYA} 
    echo "" >> ${SYA}
    echo "##########################################" >> ${SYA}
}
trap cleanup EXIT

# The following would only work in an encapsulated environment
[[ -n ${KH} ]] || echo "export HOME=\${workspace_root}/home" >> ${SS}
[[ -n ${KH} ]] || export HOME=$WORKSPACE_ROOT/home
if (( $FORCE != 0 ))
then
    [[ -n ${KH} ]] || [[ -n ${ORSR} ]] || echo "rm -rf \${WORKSPACE_ROOT}/home" >> ${SS}
    # We could write rm -rf ${HOME} but we are not crazy, some young hacker makes one mistake...
    [[ -n ${KH} ]] || [[ -n ${ORSR} ]] || [[ -n ${FS} ]] || rm -rf ${WORKSPACE_ROOT}/home
fi

[[ -n ${KH} ]] || echo "mkdir -p ${WORKSPACE_ROOT}/home" >> ${SS}
# Same here, don't do crazy stuff
[[ -n ${KH} ]] || mkdir -p ${WORKSPACE_ROOT}/home

# For some reason we have to be somewhere
echo "cd \${HOME}" >> ${SS}
cd $HOME

# Create virtualenv and start
echo "##########################################"
echo " Building virtualenv in $WORKSPACE_ROOT"
echo "##########################################"
echo
if (( $FORCE != 0 ))
then
    [[ -n ${ORSR} ]] || echo "rm -rf \${workspace_root}/caracal_venv" >> ${SS}
    [[ -n ${ORSR} ]] || [[ -n ${FS} ]] || rm -rf ${WORKSPACE_ROOT}/caracal_venv
fi
if [[ ! -d ${WORKSPACE_ROOT}/caracal_venv ]]
then
    [[ -n ${FS} ]] && echo "python3 -m venv \${workspace_root}/caracal_venv" >> ${SS}
    [[ -n ${FS} ]] || { python3 -m venv ${WORKSPACE_ROOT}/caracal_venv && echo "python3 -m venv \${workspace_root}/caracal_venv" >> ${SS}; } || { echo 'Using "python3 -m venv" failed when instaling virtualenv.'; echo 'Trying "virtualenv -p python3"'; virtualenv -p python3 ${WORKSPACE_ROOT}/caracal_venv && echo "virtualenv -p python3 \${workspace_root}/caracal_venv" >> ${SS}; } 
fi

# Report on virtualenv
if [[ -f ${WORKSPACE_ROOT}/caracal_venv/pyvenv.cfg ]]
then
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    echo "Virtualenv info (from pyvenv.cfg):" >> ${SYA}
    cat ${WORKSPACE_ROOT}/caracal_venv/pyvenv.cfg >> ${SYA}
    echo "" >> ${SYA}
fi

echo "Entering virtualenv in $WORKSPACE_ROOT"
echo ". \${workspace_root}/caracal_venv/bin/activate" >> ${SS}
[[ -n ${FS} ]] || . ${WORKSPACE_ROOT}/caracal_venv/bin/activate

echo "export PYTHONPATH=''" >> ${SS}
export PYTHONPATH=''
echo "pip install pip setuptools wheel -U"  >> ${SS}
[[ -n ${FS} ]] || pip install pip setuptools wheel -U

# Report on Python version
echo "##########################################" >> ${SYA}
echo "" >> ${SYA}
python --version >> ${SYA}

# Report on pip version
pip --version | awk '{print $1, $2}' >> ${SYA}
echo "" >> ${SYA}

# Install software
echo
echo "##################"
echo " Fetching CARACal "
echo "##################"
echo
if (( $FORCE==1 ))
then
    echo "rm -rf \${workspace_root}/meerkathi" >> ${SS}
    [[ -n ${FS} ]] || rm -rf ${WORKSPACE_ROOT}/meerkathi
fi

echo "cd \${workspace_root}" >> ${SS}
cd ${WORKSPACE_ROOT}

if [[ -n "$CARATE_LOCAL_SOURCE" ]]
then
    if [[ -e ${WORKSPACE_ROOT}/meerkathi ]]
    then
        echo "Not re-fetching MeerKATHI, use -f if you want that or"
        echo "omit -fs if you have set it."
	echo ""
    else
	echo "Fetching CARACal from local source ${local_source}"
	echo
        echo "cp -r \${local_source} \${workspace_root}/"  >> ${SS}
	[[ -n ${FS} ]] || cp -r ${CARATE_LOCAL_SOURCE} ${WORKSPACE_ROOT}/
    fi
else
    if [[ -e ${WORKSPACE_ROOT}/meerkathi ]]
    then
        if (( $FORCE==0 ))
        then	    
            echo "Not re-fetching MeerKATHI, use -f if you want that."
        else
	    echo "Fetching MeerKATHI from https://github.com/ska-sa/meerkathi.git"
	    echo "rm -rf \${workspace_root}/meerkathi" >> ${SS}
	    [[ -n ${FS} ]] || rm -rf ${WORKSPACE_ROOT}/meerkathi

            echo "git clone https://github.com/ska-sa/meerkathi.git" >> ${SS}
            [[ -n ${FS} ]] || git clone https://github.com/ska-sa/meerkathi.git
        fi
    else
	echo "Fetching MeerKATHI from https://github.com/ska-sa/meerkathi.git"
	echo "git clone https://github.com/ska-sa/meerkathi.git" >> ${SS}
	[[ -n ${FS} ]] || git clone https://github.com/ska-sa/meerkathi.git
    fi
fi

if [[ -n "$CARATE_CARACAL_BUILD_NUMBER" ]]
then
    echo "cd \${workspace_root}/meerkathi" >> ${SS}
    cd ${WORKSPACE_ROOT}/meerkathi
    [[ -z $CARATE_LOCAL_SOURCE ]] || { \
	echo "If an error occurs here, it likely means that the local installation";\
	echo "of CARACal does not contain the build number. You may want to use the";\
	echo "master branch and unset the environmrnt variable CARATE_CARACAL_BUILD_NUMBER:";\
	echo "In bash: $ unset CARATE_CARACAL_BUILD_NUMBER";\
    }
    echo "git checkout ${CARATE_CARACAL_BUILD_NUMBER}" >> ${SS}
    git checkout ${CARATE_CARACAL_BUILD_NUMBER}
fi


if [[ -d ${WORKSPACE_ROOT}/meerkathi ]]
then
    # Report on CARACal build
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    cd ${WORKSPACE_ROOT}/meerkathi
    if [[ -n "$CARATE_LOCAL_SOURCE" ]]
    then
	sya="Caracal build: local"; sya+=$'\n';
    else
        sya="Caracal build: "; sya+=`git log -1 --format=%H`; sya+=$'\n';
	sya+="from: https://github.com/ska-sa/meerkathi"; sya+=$'\n';
    fi
    echo "${sya}" >> ${SYA}
#    echo ""  >> ${SYA}

    # Get Stimela tag. This can be simplified...
    if [[ -n $US ]]
    then
	# continue here

        stimelaline=`grep "https://github.com/ratt-ru/Stimela" stimela_last_stable.txt | sed -e 's/.*Stimela@\(.*\)#egg.*/\1/'`
        if [[ -z ${stimelaline} ]]
        then
	    # Stimela tag depends on whether the repository is in or not
            stimelaline=`grep "stimela==" setup.py | sed -e 's/.*==\(.*\)\x27.*/\1/'`
            [[ -z ${stimelaline} ]] || echo "Stimela release: $stimelaline" >> ${SYA}
            stimelaline=`grep https://github.com/ratt-ru/Stimela setup.py`
            [[ -z ${stimelaline} ]] || stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'`
            [[ -z ${stimelaline} ]] || echo "Stimela build: ${stimelabuild}" >> ${SYA}
        else
            echo "Stimela build: ${stimelaline}" >> ${SYA}
        fi
    elif [[ -n $UM ]]
    then
        # Stimela tag depends on whether the repository is in or not
        stimelaline=`grep https://github.com/ratt-ru/Stimela stimela_master.txt`
        [[ -z ${stimelaline} ]] || stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'`
        [[ -z ${stimelaline} ]] || echo "Stimela build: ${stimelabuild}" >> ${SYA}
    else
        # Stimela tag depends on whether the repository is in or not
        stimelaline=`grep "stimela==" setup.py | sed -e 's/.*==\(.*\)\x27.*/\1/'`
        [[ -z ${stimelaline} ]] || echo "Stimela release: $stimelaline" >> ${SYA}
	
        [[ -n ${stimelaline} ]] || stimelaline=`grep 'https://github.com/ratt-ru/Stimela' setup.py`
        [[ -z ${stimelaline} ]] || [[ -n ${stimelabuild} ]] || stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'`
        [[ -z ${stimelaline} ]] || echo "Stimela build: ${stimelabuild}" >> ${SYA}
    fi

    echo "from: https://github.com/ratt-ru/Stimela" >> ${SYA}
    [[ -z ${ORSR} ]] || echo "Stimela has not been re-build, so this is a guess" >> ${SYA}
    echo "" >> ${SYA}
fi

echo "####################"
echo " Installing CARACal "
echo "####################"
echo

#PATH=${WORKSPACE}/projects/pyenv/bin:$PATH
#LD_LIBRARY_PATH=${WORKSPACE}/projects/pyenv/lib:$LD_LIBRARY_PATH
echo "Installing CARACal using pip install"
echo "pip install -U --force-reinstall \${workspace_root}/meerkathi" >> ${SS}
[[ -n ${FS} ]] || pip install -U --force-reinstall ${WORKSPACE_ROOT}/meerkathi
if [[ -n ${UM} ]]
then
    echo "Intstalling stimela_master.txt"
    echo "pip install -U --force-reinstall -r \${workspace_root}/meerkathi/stimela_master.txt" >> ${SS}
    [[ -n ${FS} ]] || pip install -U --force-reinstall -r ${WORKSPACE_ROOT}/meerkathi/stimela_master.txt
fi
if [[ -n ${US} ]]
then
    echo "Intstalling stimela_last_stable.txt"
    echo "pip install -U --force-reinstall -r \${workspace_root}/meerkathi/stimela_last_stable.txt" >> ${SS}
    [[ -n ${FS} ]] || pip install -U --force-reinstall -r ${WORKSPACE_ROOT}/meerkathi/stimela_last_stable.txt
fi

if [[ -z $DM ]] && [[ -z $DA ]] && [[ -z $DI ]] && [[ -z $SM ]] && [[ -z $SA ]] && [[ -z $SI ]]
then
    echo "You have not defined a test:"
    echo "--docker-minimal or -dm"
    echo "--docker-alternative or -da"
    echo "--docker-installation or -di"
    echo "--singularity-minimal or -sm"
    echo "--singularity-alternative or -sa"
    echo "--singularity-installation or -si"
    echo "Use -h flag for more information"
    kill "$PPID"; exit 0
fi

if [[ -n $DM ]] || [[ -n $DA ]] || [[ -n $DI ]]
then
    if [[ -n $ORSR ]]
    then
        echo "Omitting re-installation of Stimela Docker images"
        echo "##########################################" >> ${SYA}
	echo "Omitting re-installation of Stimela Docker images" >> ${SYA}
        echo "##########################################" >> ${SYA}
        echo "" >> ${SYA}
    else
        echo
        echo "##################################"
        echo " Installing Stimela Docker images "
        echo "##################################"
        echo
        echo "Installing Stimela (Docker)"

        echo "##########################################" >> ${SYA}
        echo "" >> ${SYA}
	docker --version >> ${SYA}
        echo "" >> ${SYA}

        # Not sure if stimela listens to $HOME or if another variable has to be set.
        # This $HOME is not the usual $HOME, see above
	echo "Removing \${HOME}/.stimela/*"
        echo "rm -f \${HOME}/.stimela/*" >> ${SS}
        [[ -n ${FS} ]] || rm -f ${HOME}/.stimela/*
        [[ -n ${OP} ]] || echo "Running docker system prune"
        [[ -n ${OP} ]] || echo "docker system prune" >> ${SS}
        [[ -n ${OP} ]] || [[ -n ${FS} ]] || docker system prune
        if [[ -n $PD ]]
        then
	    ii = 1
	    until (( ${ii} > ${IA} ))
	    do
	        echo "Running stimela pull -d"
	        echo "stimela pull -d" >> ${SS}
                if [[ -z ${FS} ]]
	        then
		    stimela pull -d && break || {
			echo "stimela pull -d failed"
			(( ii++ ))
			}
		else
		    break
	        fi
	    done
        fi
	ii=1
        until (( ${ii} > ${IA} ))
        do
            echo "Running stimela build"
            echo "stimela build" >> ${SS}
            if [[ -z ${FS} ]]
	    then
	        stimela build && break || {
			echo "stimela build failed"
			(( ii++ ))
		    }
	    else
		break
	    fi
        done
    fi
    echo ""
fi

testingoutput () {
    
    # Function to test output after running a pipeline
    # Argument 1: $WORKSPACE_ROOT
    # Argument 2: Test directory, e.g. test_extendedConfig_docker

    echo
    echo "###################"
    echo " Counting logfiles "
    echo "###################"
    echo 
    echo "Counting logfiles in directory ${1}/${2}"
    allogs=""
    allogs=`ls -t ${1}/${2}/output/logs/` || true
    total=0

    for log in ${allogs}
    do
        (( total+=1 ))
	[[ -z $hadmeerkathi ]] || { echo "$log is the last log before log-meerkathi.txt"; }
	[[ -z $hadmeerkathi ]] || { echo "$log is the last log before log-meerkathi.txt" >> ${SYA}; }
        unset hadmeerkathi
        if [[ $log == "log-meerkathi.txt" ]]
        then
            hadmeerkathi=1 
        fi
    done
    echo "Total number of logfiles: $total"
    echo "Total number of logfiles: $total" >> ${SYA}

    # Count number of runs of workers and the number of finishes
    worker_runs=`grep "Running worker" ${1}/${2}/output/logs/log-meerkathi.txt | wc | sed 's/^ *//; s/ .*//'`
    worker_fins=`grep "Finished worker" ${1}/${2}/output/logs/log-meerkathi.txt | wc | sed 's/^ *//; s/ .*//'`

    (( $worker_runs == $worker_fins )) || { echo "Workers starting (${worker_runs}) and ending (${worker_fins}) are unequal in log-meerkathi.txt" >> ${SYA}; }
    (( $worker_runs == $worker_fins )) || { echo "Workers starting (${worker_runs}) and ending (${worker_fins}) are unequal in log-meerkathi.txt"; echo "Returning error"; return 1; }

    (( $worker_runs > 0 )) || { echo "No workers have started according to log-meerkathi.txt" >> ${SYA}; echo "Returning error"; }
    (( $worker_runs > 0 )) || { echo "No workers have started according to log-meerkathi.txt"; echo "Returning error"; return 1; }

    # Notice that 0 is true in bash
    (( $total > 0 )) || { echo "No logfiles produced. Returning error." >> ${SYA}; }
    (( $total > 0 )) || { echo "No logfiles produced. Returning error."; return 1; }
    return 0
}

runtest () {
    # Running a specific caracal test using a specific combination of configuration file, architecture, and containerization
    # Argument 1: Line appearing at the start of function
    # Argument 2: $WORKSPACE_ROOT
    # Argument 3: configuration file name without "yml"
    # Argument 4: containerisation architecture "docker" or "singularity"
    # Argument 5: delete existing files or not
    # Argument 6: Location of the configfile
    # Argument 7: Location of the configfile, string to pass to the output
    # Argument 8: Switches to pass to meerkathi

    local greetings_line=$1
    local WORKSPACE_ROOT=$2
    local configfilename=$3
    local contarch=$4
    local FORCE=$5
    local configlocation=$6
    local configlocationstring=$7
    local caracalswitches=$8

    echo "##########################################"
    echo " $greetings_line "
    echo "##########################################"
    echo

    # echo 1 greetings_line       $1
    # echo 2 WORKSPACE_ROOT       $2
    # echo 3 configfilename       $3
    # echo 4 contarch             $4
    # echo 5 FORCE                $5
    # echo 6 configlocation       $6
    # echo 7 configlocationstring $7
    # echo 8 caracalswitches      $8

    failedrun=0

    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    sya=" Test ${greetings_line} preparation start time:";sya+=$'\n'; sya+=`date -u`;
    echo "${sya}" >> ${SYA} 
    
    if [[ -e ${WORKSPACE_ROOT}/test_${configfilename}_${contarch} ]] && (( $FORCE==0 ))
    then
        echo "Will not re-create existing directory ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}"
        echo "and use old results. Use -f to override."
    else
	echo "rm -rf \${workspace_root}/test_${configfilename}_${contarch}" >> ${SS}
        [[ -n ${FS} ]] || rm -rf ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}
        echo "Preparing ${contarch} test (using ${configfilename}.yml) in"
        echo "${WORKSPACE_ROOT}/test_${configfilename}_${contarch}"
	echo "mkdir -p \${workspace_root}/test_${configfilename}_${contarch}/msdir" >> ${SS}
        mkdir -p ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/msdir
        echo "sed \"s/dataid: \[\x27\x27\]/$dataidstr/\" ${configlocationstring} > \${workspace_root}/test_${configfilename}_${contarch}/${configfilename}.yml" >> ${SS}
	[[ -n ${FS} ]] || sed "s/dataid: \[\x27\x27\]/$dataidstr/" ${configlocation} > ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/${configfilename}.yml
	[[ ! -f ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/${configfilename}.yml ]] || cp ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/${configfilename}.yml ${WORKSPACE_ROOT}/report/${configfilename}_${contarch}.yml.txt
	echo "cp -r \${test_data_dir}/*.ms \${workspace_root}/test_${configfilename}_${contarch}/msdir/" >> ${SS}
	[[ -n ${FS} ]] || cp -r $CARATE_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/msdir/
        echo "Running ${contarch} test (using ${configfilename}.yml)"
	echo "cd \${workspace_root}/test_${configfilename}_${contarch}" >> ${SS}
        cd ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}
	
        # Notice that currently all output will be false, such that || true is required to ignore this
	failed=0
	echo 	meerkathi -c ${configfilename}.yml ${caracalswitches} || true

	# Report CARACal start time
        sya=" Test ${greetings_line} CARACal start time:"; sya+=$'\n'; sya+=`date -u`;
        echo "${sya}" >> ${SYA} 

	echo "meerkathi -c ${configfilename}.yml ${caracalswitches}" >> ${SS}
	[[ -n ${FS} ]] || meerkathi -c ${configfilename}.yml ${caracalswitches} || { true; echo "CARACal run returned an error."; failedrun=1; }
    fi
    [[ ! -f ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/output/logs/log-meerkathi.txt ]] || cp ${WORKSPACE_ROOT}/test_${configfilename}_${contarch}/output/logs/log-meerkathi.txt ${WORKSPACE_ROOT}/report/log-meerkathi_test_${configfilename}_${contarch}.txt
    echo "Checking output of ${configfilename} ${contarch} test"
    failedoutput=0
    testingoutput ${WORKSPACE_ROOT} test_${configfilename}_${contarch} || { true; failedoutput=1; }
    #    failedoutput=$?
    sya=" Test ${greetings_line} end time:"; sya+=$'\n'; sya+=`date -u`; sya+=$'\n';
    echo "${sya}" >> ${SYA} 

    
    if (( ${failedrun} == 1 || ${failedoutput} == 1 ))
    then
        echo
        echo "###############"
        echo " caratekit failed "
        echo "###############"
        echo 
        echo "###############" >> ${SYA} 
        echo >> ${SYA} 
        echo " caratekit failed " >> ${SYA} 
        echo >> ${SYA} 
        echo "###############" >> ${SYA} 
        echo  >> ${SYA} 
        kill "$PPID"
        exit 1
    fi
    
    # Size of test
    outsize=`du -ms ${WORKSPACE_ROOT}/test_${configfilename}_${contarch} | awk '{print $1}'`
    echo "Final test folder size (test_${configfilename}_${contarch}): ${outsize} MB" >> ${SYA}
    echo "" >> ${SYA}    
}

if [[ -n $DM ]]
then
    greetings_line="Docker: minimalConfig"
    confilename="minimalConfig"
    contarch="docker"
    caracalswitches=" "
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/meerkathi/meerkathi/sample_configurations/${confilename}.yml" "\{workspace_root}/meerkathi/meerkathi/sample_configurations/${confilename}.yml" "${caracalswitches}"
fi

if [[ -n $DA ]]
then
    greetings_line="Docker: (alternative) carateConfig"
    confilename="carateConfig"
    contarch="docker"
    caracalswitches=" "
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/meerkathi/meerkathi/sample_configurations/${confilename}.yml" "\${workspace_root}/meerkathi/meerkathi/sample_configurations/${confilename}.yml" "${caracalswitches}"
fi

if [[ -n $DI ]] && [[ -n $configfilename ]]
then
    greetings_line="Docker: $configfilename"
    confilename=$configfilename
    contarch="docker"
    caracalswitches=" "
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${CARATE_CONFIG_SOURCE}" "\${config_source}" "${caracalswitches}"
fi

if [[ -n $SM ]] || [[ -n $SA ]] || [[ -n $SI ]]
then
    # This sets the singularity image folder to the test environment, but it does not work correctly
    # Not only the cache is moved there but also the images and it gets all convolved.
    ###### export SINGULARITY_CACHEDIR=$CARATE_WORKSPACE/.singularity
    if [[ -n "$SR" ]]
    then
	singularity_loc=${CARATE_WORKSPACE}/stimela_singularity
	singularity_locstring="\${workspace}/stimela_singularity"
    else
	singularity_loc=${WORKSPACE_ROOT}/stimela_singularity
	singularity_locstring="\${workspace_root}/stimela_singularity"
    fi
    if (( $FORCE==0 )) || [[ -n $ORSR ]]
    then
        if [[ -e ${singularity_loc} ]]
        then
            echo "Will not re-create existing stimela_singularity and use old installation."
            echo "Use -f to override and unset -or or --omit-stimela-reinstall flags."
            echo "##########################################" >> ${SYA}
            echo "Will not re-create existing stimela_singularity and use old installation." >> ${SYA}
            echo "Use -f to override and unset -or or --omit-stimela-reinstall flags." >> ${SYA}
            echo "##########################################" >> ${SYA}
            echo "" >> ${SYA}	    
        fi
    else
	echo "rm -rf ${singularity_locstring}" >> ${SS}
        [[ -n ${FS} ]] || rm -rf ${singularity_loc}
        ######rm -rf $CARATE_WORKSPACE/.singularity
    fi
    if [[ ! -e "${singularity_loc}" ]]
    then
        echo
        echo "#########################################"
        echo " Installing Stimela images (Singularity) "
        echo "#########################################"
        echo
        echo "##########################################" >> ${SYA}
        echo "" >> ${SYA}
	singvers=`singularity --version`
	echo "Singularity version: ${singvers}" >> ${SYA}
        echo "" >> ${SYA}

        if [[ -z $KRSR ]]
        then
	    echo "Removing \${HOME}/.stimela/*"
            echo "rm -f \${HOME}/.stimela/*" >> ${SS}
            [[ -n ${FS} ]] || rm -f ${HOME}/.stimela/*
        fi
	echo "Installing Stimela images in ${singularity_locstring}"
	echo "mkdir -p ${singularity_locstring}"
	mkdir -p ${singularity_loc}
	ii=1
	until (( ${ii} > ${IA} ))
	do
	    echo stimela pull --singularity -f --pull-folder ${singularity_loc}
	    echo "stimela pull --singularity -f --pull-folder ${singularity_locstring}" >> ${SS}
	    if [[ -z ${FS} ]]
	    then
		stimela pull --singularity -f --pull-folder ${singularity_loc} && break || {
			echo "stimela pull --singularity -f --pull-folder ${singularity_loc} failed"
			(( ii++ ))
			}
	    else
		break
	    fi		    
	done
    fi

    # Size of images
    outsize=`du -ms ${singularity_loc} | awk '{print $1}'`
    echo "Singularity image folder size: ${outsize} MB" >> ${SYA}
    echo "" >> ${SYA}    
fi

if [[ -n $SM ]]
then
    greetings_line="Singularity: minimalConfig_singularity"
    confilename="minimalConfig"
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${singularity_loc}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/meerkathi/meerkathi/sample_configurations/${confilename}.yml" "\${workspace_root}/meerkathi/meerkathi/sample_configurations/${confilename}.yml" "${caracalswitches}"
fi

if [[ -n $SA ]]
then
    greetings_line="Singularity: (alternative) carateConfig"
    confilename="carateConfig"
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${singularity_loc}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/meerkathi/meerkathi/sample_configurations/${confilename}.yml" "\${workspace_root}/meerkathi/meerkathi/sample_configurations/${confilename}.yml" "${caracalswitches}"
fi

if [[ -n $SI ]] && [[ -n $configfilename ]]
then
    greetings_line="Singularity: $configfilename"
    confilename=$configfilename
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${singularity_loc}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${CARATE_CONFIG_SOURCE}" "\${config_source}" "${caracalswitches}"
fi

echo "###############" >> ${SYA} 
echo >> ${SYA} 
echo " caratekit succeeded." >> ${SYA} 
echo >> ${SYA} 
echo "###############" >> ${SYA} 

echo
echo "###########################################################"
echo ""
echo ${kksuccessquotes[$(( $RANDOM % 2 ))]}
echo "Caratekit succeeded."
echo ""
echo "###########################################################"
echo

success=1

exit 0
