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
echo ""
echo "#######################"
echo "Testing CARACal at home "
echo "#######################"
echo ""
echo "caratekit.sh $*"
echo ""
 sya="############################" ; sya+=$'\n\n'
sya+="Caratekit system information"  ; sya+=$'\n\n'
sya+="############################" ; sya+=$'\n\n'
sya+="Start time: "; sya+=`date -u`; sya+=$'\n'
sya+="Call executing: caratekit.sh $*"; sya+=$'\n\n'
sya+="############################" ; sya+=$'\n\n'
hostnamectl &>/dev/null && hostinfo=`hostnamectl | grep -Ev "Machine ID"'|'"Boot ID"` || hostinfo="Not accessible"
sya+="Host info: ";sya+=$'\n'; sya+=${hostinfo};sya+=$'\n'
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
    if [[ "$arg" == "--omit-copy-test-data" ]] || [[ "$arg" == "-od" ]]
    then
        OD=1
    fi
    if [[ "$arg" == "--input-dir" ]] || [[ "$arg" == "-id" ]]
    then
	(( nextcount=argcount+1 ))
	(( $nextcount <= $# )) || { echo "Argument expected for --input-dir or -id switch, stopping."; kill "$PPID"; exit 1; }

	CARATE_INPUT_DIR=${!nextcount}
 	firstletter=`echo ${CARATE_INPUT_DIR} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_INPUT_DIR="${cwd}/${CARATE_INPUT_DIR}"
    fi
    if [[ "$arg" == "--virtualenv" ]] || [[ "$arg" == "-ve" ]]
    then
	(( nextcount=argcount+1 ))
	(( $nextcount <= $# )) || { echo "Argument expected for --virtualenv or -ve switch, stopping."; kill "$PPID"; exit 1; }

	CARATE_VIRTUALENV=${!nextcount}
 	firstletter=`echo ${CARATE_VIRTUALENV} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_VIRTUALENV="${cwd}/${CARATE_VIRTUALENV}"
    fi
    if [[ "$arg" == "--omit-venv-reinstall" ]] || [[ "$arg" == "-ov" ]]
    then
      OV=1
    fi
    if [[ "$arg" == "--local_stimela" ]] || [[ "$arg" == "-lst" ]]
    then
	(( nextcount=argcount+1 ))
	(( $nextcount <= $# )) || { echo "Argument expected for --local-stimela or -lst switch, stopping."; kill "$PPID"; exit 1; }

	CARATE_LOCAL_STIMELA=${!nextcount}
 	firstletter=`echo ${CARATE_LOCAL_STIMELA} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_LOCAL_STIMELA="${cwd}/${CARATE_LOCAL_STIMELA}"
    fi
    if [[ "$arg" == "--caracal-build-number" ]] || [[ "$arg" == "-cb" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --caracal-build-number or -cb switch, stopping."; kill "$PPID"; exit 1; }
 CARATE_CARACAL_BUILD_NUMBER=${!nextcount}
    fi
    if [[ "$arg" == "--caracal-test-id" ]] || [[ "$arg" == "-ct" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --caracal-test-id or -ct switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_CARACAL_TEST_ID=${!nextcount}
    fi
    if [[ "$arg" == "--omit-caracal-reinstall" ]] || [[ "$arg" == "-oc" ]]
    then
        OC=1
    fi
    if [[ "$arg" == "--omit-caracal-fetch" ]] || [[ "$arg" == "-of" ]]
    then
        OF=1
    fi
    if [[ "$arg" == "--caracal-run-prefix" ]] || [[ "$arg" == "-rp" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --caracal-run-prefix or -rp switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_CARACAL_RUN_PREFIX=${!nextcount}
 	firstletter=`echo ${CARATE_CARACAL_RUN_PREFIX} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_CARACAL_RUN_PREFIX="${cwd}/${CARATE_CARACAL_RUN_PREFIX}"
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
    if [[ "$arg" == "--keep-config-source" ]] || [[ "$arg" == "-kc" ]]
    then
        KC=1
    fi
    if [[ "$arg" == "--docker-sample-configs" ]] || [[ "$arg" == "-dsc" ]]
    then
      DSC=1
    fi
    if [[ "$arg" == "--singularity-sample-configs" ]] || [[ "$arg" == "-ssc" ]]
    then
      SSC=1
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

    echo "  CARATE_INPUT_DIR:            Directory containing input data"
    echo "                               (optional)"
    echo ""

    echo "  CARATE_VIRTUALENV:           Location of the virtualenv directory"
    echo "                               (optional)"
    echo ""

    echo "  CARATE_LOCAL_STIMELA:        Location of a local stimela"
    echo "                               (optional)"
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
    echo "  CARATE_LOCAL_SOURCE:         Local CARACal copy to use. If not"
    echo "                               set, CARACal will be downloaded"
    echo "                               from https://github.com/ska-sa/caracal"
    echo ""
    echo "  CARATE_CONFIG_SOURCE:        Local configuration file copy to use for an"
    echo "                               additional test"
    echo ""
    echo "  CARATE_CARACAL_RUN_PREFIX:   The name prefix of an individual carate run. Will"
    echo "                               be automatically generated if not supplied."
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
    echo "  --input-dir ARG -id ARG             Use ARG instead of environment variable"
    echo "                                      CARATE_INPUT_DIR"
    echo ""
    echo "  --virtualenv ARG -ve ARG            Use ARG instead of internal virtualenv"
    echo "                                      variable CARATE_VIRTUALENV"
    echo ""
    echo "  --omit-venv-reinstall -ov           Do not re-install the virtual environment"
    echo ""
    echo "  --caracal-build-number ARG -cb ARG  Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_BUILD_NUMBER"
    echo ""
    echo "  --caracal-test-id ARG -ct ARG       Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_TEST_ID"
    echo ""
    echo "  --omit-copy-test-data -od           Do not re-copy test data"
    echo ""
    echo "  --caracal-run-prefix ARG -rp ARG    Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_RUN_PREFIX"
    echo ""
    echo "  --local-source ARG -ls ARG          Use ARG instead of environment variable"
    echo "                                      CARATE_LOCAL_SOURCE"
    echo ""
    echo "  --omit-caracal-reinstall -oc        Do not pip install caracal"
    echo ""
    echo "  --omit-caracal-fetch -of            Do not fetch or copy caracal directory"
    echo "                                      if present"
    echo ""
    echo "  --config-source ARG -cs ARG         Use ARG instead of environment variable"
    echo "                                      CARATE_CONFIG_SOURCE"
    echo ""
    echo "  --keep-config-source -kc            Do not autodetect measurement sets but"
    echo "                                      keep the confic source as it is. Only"
    echo "                                      if --config-source is supplied."
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
    echo "                                      when installing CARACal"
    echo ""
    echo "  --use-stimela-stable -us            Use"
    echo "                                      pip install -U --force-reinstall -r (...)stimela_last_stable.txt"
    echo "                                      when installing CARACal"
    echo ""
    echo "  --local-stimela ARG -lst ARG        Use pip install -U --force-reinstall ARG"
    echo "                                      to install a local stimela (or whatever is in ARG)"
    echo "                                      when installing CARACal"
    echo ""
    echo "  --omit-stimela-reinstall -os        Do not re-install stimela"
    echo ""
    echo "  --force -f                          Force replacement and re-installation of"
    echo "                                      all components if possible (see below)"
    echo "                                      "
    echo ""
    echo "  --fastsim -fs                       Omit all time-consuming steps"
    echo ""
    echo "  --override -or                      Override security question (showing root"
    echo "                                      directory and asking whether to proceed.)"
    echo ""
    echo "  --docker-sample-configs -dsc         Check all sample configurations to pass"
    echo "                                       observation config with docker."
    echo ""
    echo "  --singularity-sample-configs -ssc    Check all sample configurations to pass"
    echo "                                       observation config with singiularity."
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
    echo " call or supplied using switches --caracal-test-id or -ct) as an environment"
    echo " variable if CARATE_CARACAL_BUILD_NUMBER is"
    echo " not defined. The rationale behind that is that the test directory is"
    echo " always linked to a git(hub) build number if that exists. Otherwise, if"
    echo " CARATE_CARACAL_BUILD_NUMBER is not defined, the user can supply an"
    echo " alternative name \$CARATE_CARACAL_TEST_ID. In the test root"
    echo " directory \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID, a home"
    echo " directory called home, a virtual environment called"
    echo " caracal_virtualenv, a CARACal copy caracal, and up to six test"
    echo " directories are created, within which the tests are conducted. If the"
    echo " --force or -f switch is set, existing directories and installations"
    echo " are deleted and replaced, if not, only those directories"
    echo " and files are created, which do not exist yet. Exceptions from that rule"
    echo " exist. If the --omit-stimela-reinstall or -os switch is set, "
    echo " a re-installation of stimela is preventedeven if -f is set. This includes the"
    echo " Re-installation of the virtual environment, the home directory, and"
    echo " the file .stimela in the home directory. If the option --keep-home or -kh is"
    echo " set, the directory \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID/home is pre-"
    echo " served. Switches --omit-virtualenv-reinstall and -ov prevent the deletion of"
    echo " the virtualenv, --omit-caracal-reinstall will prevent the deletion (and re-"
    echo " installation of CARACal. If option --fast-simulation (-fs) is set, only the directory "
    echo " \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID/report is deleted if option -f is"
    echo " set. No directories or files supplied to caratekit.sh (including their"
    echo " parent directories) are deleted."
    echo ""
    echo "  In detail (all installations in the root directory"
    echo "  \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID):"
    echo ""
    echo "  - a directory called home is created (if not existing or if -f is set)"
    echo "    and the HOME environment variable set to that home directory unless"
    echo "    the --keep-home or -kh switches are set"
    echo ""
    echo "  - a python 3 virtual environment name caracal_venv is created (if not"
    echo "    existing or if -f is set) and activated. It is not re-created if"
    echo "    switches --omit-stimela-reinstall (-os) or --omit-virtualenv-reinstall (-ov)"
    echo "    are set."
    echo ""
    echo "  - caracal is either downloaded to the root directory (if not existing"
    echo "    or if -f is set) from https://github.com/ska-sa/caracal or, if the"
    echo "    CARATE_LOCAL_SOURCE environment variable is set,"
    echo "    \$CARATE_LOCAL_SOURCE is copied to the root directory if not"
    echo "    existing or if -f is set (notice that the directory tree should be a"
    echo "    valid caracal tree, ready for installation). If switches"
    echo "    --omit-caracal-fetch or -of are set, the sources are also not copied"
    echo "    across."
    echo ""
    echo "  - the caracal version CARATE_CARACAL_BUILD_NUMBER is checked out"
    echo "    using git if CARATE_CARACAL_BUILD_NUMBER is defined. If switches"
    echo "    --omit-caracal-reinstall or -oc are set, this is not done"
    echo ""
    echo "  - caracal is installed via pip. If switches --omit-caracal-reinstall"
    echo "    or -oc are set, this is not done"
    echo ""
    echo "  - if --use-stimela-master or -um switch is set,"
    echo "    caracal/stimela_master.txt is installed via pip. If switches"
    echo "    --omit-stimela-reinstall or -os are set, this is not done"
    echo ""
    echo "  - if --use-stimela-stable or -us switch is set,"
    echo "    caracal/stimela_last_stable.txt is installed via pip. If switches"
    echo "    --omit-stimela-reinstall or -os are set, this is not done"
    echo ""
    echo "  - when switches --docker-minimal, -dm, --docker-alternative, -da,"
    echo "    --docker-installation, -di are set, home/.stimela is removed, docker"
    echo "    system prune is invoked, and docker stimela is installed (stimela"
    echo "    build). If switches"
    echo "    --omit-stimela-reinstall or -os are set, this is not done"
    echo ""
    echo "  - when switches --pull-docker, -pd are set, stimela pull -d is invoked"
    echo "    before running stimela build for Docker installation, omit step"
    echo "    otherwise. If switches"
    echo "    --omit-stimela-reinstall or -os are set, this is not done"
    echo ""
    echo "  - when switches --singularity-minimal, -sm, --singularity-alternative,"
    echo "    -se, --singularity-installation, -si are set, home/.stimela is"
    echo "    removed, and singularity stimela is by default installed in the"
    echo "    directory (if not existing or if -f is set)"
    echo "    \$CARATE_WORKSPACE/stimela_singularity. If --stimela-root or"
    echo "    -sr are set, it is installed in rootfolder/stimela_singularity."
    echo "    The first variant allows to re-use the same stimela installation"
    echo "    In multiple tests. If switches"
    echo "    --omit-stimela-reinstall or -os are set, this is not done"
    echo ""
    echo "  - when switch --singularity-minimal or -sm is set, a directory"
    echo "    test_minimal_singularity is created (if not existing or if -f is"
    echo "    set), the configuration file"
    echo "    caracal/caracal/sample_configurations/minimalConfig.yml is"
    echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_minimal_singularity"
    echo "    directory and minimalConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then caracal is run with"
    echo "    minimalConfig.yml and declared successful if certain expected files"
    echo "    are created (see exceptions below)."
    echo ""
    echo "  - when switch --singularity_extended or -se is set, a directory"
    echo "    test_extended_singularity is created (if not existing or if -f is"
    echo "    set), the configuration file"
    echo "    caracal/caracal/sample_configurations/extendedConfig.yml is"
    echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_extended_singularity"
    echo "    directory and extendedConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then caracal is run with"
    echo "    extendedConfig.yml and declared successful if certain expected files"
    echo "    are created (see exceptions below)."
    echo ""
    echo "  - when switch --docker-minimal or -dm is set, a directory"
    echo "    \- test_minimal_docker is created (if not existing or if -f is set),"
    echo "    the configuration file"
    echo "    caracal/caracal/sample_configurations/minimalConfig.yml is"
    echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_minimal_docker"
    echo "    directory and minimalConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then caracal is run with"
    echo "    minimalConfig.yml and declared successful if certain expected files"
    echo "    are created (see exceptions below)."
    echo ""
    echo "  - when switch --docker_extended or -da is set, a directory"
    echo "    test_extended_docker is created (if not existing or if -f is set),"
    echo "    the configuration file"
    echo "    caracal/caracal/sample_configurations/extendedConfig.yml is"
    echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
    echo "    are copied into the msdir directory in the test_extended_docker"
    echo "    directory and extendedConfig.yml is edited to point to those .ms"
    echo "    files in the variable dataid, then caracal is run with"
    echo "    extendedConfig.yml and declared successful if certain expected files"
    echo "    are created (see exceptions below)."
    echo ""
    echo "  - when environment variable CARATE_CONFIG_SOURCE is set in combination with"
    echo "    switches --singularity-installation or -si set, then that yaml configuration"
    echo "    source is used for a further singularity test in the directory"
    echo "    test_prefix_singularity, where prefix is the prefix of the yaml file. The line"
    echo "    dataid: ['...','...'] in that file is replaced by the appropriate line to"
    echo "    process the test data sets in \$CARATE_TEST_DATA_DIR unless switch -kc is set."
    echo "    See exceptions below."
    echo ""
    echo "  - when environment variable CARATE_CONFIG_SOURCE is set in combination with"
    echo "    switches --docker-installation or -di set, then that yaml configuration"
    echo "    source is used for a further singularity test in the directory"
    echo "    test_prefix_singularity, where prefix is the prefix of the yaml file. The line"
    echo "    dataid: ['...','...'] in that file is replaced by the appropriate line to"
    echo "    process the test data sets in \$CARATE_TEST_DATA_DIR unless switch -kc is set."
    echo "    See exceptions below."
    echo ""
    echo "  - when environment variable CARATE_INPUT_DIR is set the contents of that"
    echo "    direcory will be copied into the input directory of CARACal prior to"
    echo "    starting the test"
    echo ""
    echo "  - when environment variable CARATE_VIRTUALENV is set the that"
    echo "    direcory is used as the location of the virtual environment"
    echo ""
    echo "  - when switch omit-venv-reinstall or -ov is set, the virtual environment will"
    echo "    not be re-installed if already present."
    echo ""
    echo "  - when environment variable CARATE_LOCAL_STIMELA is set the that"
    echo "    direcory is used as the Stimela location."
    echo ""
    echo "  - when switches --omit-copy-test-data or -od are set, the test data will not"
    echo "    be copied if for a test run the directory with the test data does already"
    echo "    exist"
    echo ""
    echo "  - when environment variable CARATE_CARACALRUN_PREFIX is set (or defined via"
    echo "    --caracal-run-prefix or -rp switches) tests will not be named using that"
    echo "    variable as a prefix and a suffix indicating the number of the test."
    echo ""
    echo " For each test run, log-caracal.txt is searched for keywords indicating the start"
    echo " and the end of a worker and those numbers are reported."
    echo " The test is declared failed and carate.sh returns 1 if:"
    echo "   - No logfiles are produced before CARACal finishes"
    echo "   - log-caracal.txt does not contain any keyword indicating that a worker has started"
    echo "   - The number of keywords in log-caracal.txt indicating the start of a worker differs"
    echo "     from the number of keywords in log-caracal.txt indicating the end of a worker."
    echo "   - If the exit status of CARACal is not 0 (success)"
    echo ""
    echo " caratekit will create a report directory \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID/report"
    echo " containing three files:"
    echo "   - a shell script \${CARATE_CARACAL_TEST_ID}.sh.txt reproducing all shell commands"
    echo "     initiated by carate.sh"
    echo "   - a file ${CARATE_CARACAL_TEST_ID}_sysinfo.txt with information about the computer and"
    echo "     the environment that was used for the test"
    echo "   - copies of the configuration files, one per run"
    echo "   - copies of the output log-caracal.txt, one per run"
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
    echo "   export CARATE_WORKSPACE=/home/tester/software/caracal_tests"
    echo "   export CARATE_TEST_DATA_DIR=/home/jozsa/software/caracal_tests/rawdata"
    echo "   export PATH=\$PATH:/home/user/software/caracal/caracal/utils/carate.sh"
    echo " or"
    echo "   setenv CARATE_WORKSPACE \"/home/tester/software/caracal_tests\""
    echo "   setenv CARATE_TEST_DATA_DIR=\"/home/jozsa/software/caracal_tests/rawdata\""
    echo "   set path = ( \$path /home/user/software/caracal/caracal/utils/carate.sh)"
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
    echo "   export CARATE_WORKSPACE=/home/tester/software/caracal_tests"
    echo "   export CARATE_TEST_DATA_DIR=/home/jozsa/software/caracal_tests/rawdata"
    echo "   export PATH=\$PATH:/home/user/software/caracal/caracal/utils/carate.sh"
    echo " or"
    echo "   setenv CARATE_WORKSPACE \"/home/tester/software/caracal_tests\""
    echo "   setenv CARATE_TEST_DATA_DIR=\"/home/jozsa/software/caracal_tests/rawdata\""
    echo "   set path = ( \$path /home/user/software/caracal/caracal/utils/carate.sh)"
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

echo "##########"
echo " Starting "
echo "##########"
echo ""

# Environment variables
[[ -n "$CARATE_WORKSPACE" ]] || { \
    echo "You have to define a global CARATE_WORKSPACE variable, like (if you're\nusing bash):";\
    echo "$ export CARATE_WORKSPACE=\"/home/username/caracal_tests\"";\
    echo "Or use the -ws switch. It is the top level directory of all tests.";\
    echo "";\
    kill "$PPID"; exit 1;
}

# Create header to script
ss="workspace=${CARATE_WORKSPACE}"
ss+=$'\n'
tdfault=0
if [[ ! -n "${CARATE_TEST_DATA_DIR}" ]]
then
    CARATE_TEST_DATA_DIR_OLD=${CARATE_TEST_DATA_DIR}
    if [[ -n ${DM} ]] || [[ -n ${DA} ]] || [[ -n ${SM} ]] || [[ -n ${SA} ]] || [[ -n ${DSC} ]] || [[ -n ${SSC} ]]
    then
	tdfault=1
    else
	[[ ! -n ${CARATE_CONFIG_SOURCE} ]] || tdfault=1
    fi
    CARATE_TEST_DATA_DIR_OLD=""
else
    [[ -e ${CARATE_TEST_DATA_DIR} ]] || tdfault=1
    CARATE_TEST_DATA_DIR_OLD=${CARATE_TEST_DATA_DIR}
fi
(( $tdfault == 0 )) || { \
    echo "You likely have to define a CARATE_TEST_DATA_DIR variable, like (if";\
    echo "you're using bash):";\
    echo "$ export CARATE_TEST_DATA_DIR=\"/home/username/caracal_tests/rawdata\"";\
    echo "Or use the -td switch.";\
    echo "You also have to create that directory $CARATE_TEST_DATA_DIR";\
    echo "and put test rawdata therein: a.ms  b.ms c.ms ...";\
    echo "These test data will be copied across for the test.";\
    echo "This is not true only if you are using the --omit-copy-test-data or -od switches"
    echo "and you have run this test before."
}

[[ ! -n "$CARATE_TEST_DATA_DIR" ]] || ss+="test_data_dir=${CARATE_TEST_DATA_DIR}"
[[ ! -n "$CARATE_TEST_DATA_DIR" ]] || ss+=$'\n'

# Force test number to be identical with build number, if it is defined
[[ -z "$CARATE_CARACAL_BUILD_NUMBER" ]] || { \
    CARATE_CARACAL_TEST_ID=$CARATE_CARACAL_BUILD_NUMBER; \
}

[[ -n "$CARATE_CARACAL_TEST_ID" ]] || { \
    echo "Without build number you have to define a global CARATE_CARACAL_TEST_ID";\
    echo "variable, giving your test directory a name, like (if you're using bash):";\
    echo "$export CARATE_CARACAL_TEST_ID=\"b027661de6ff93a183ff240b96af86583932fc1e\"";\
    echo "Otherwise choose any unique identifyer. You can also use the -ct switch.";\
    echo "";\
    kill "$PPID"; exit 1;
}
ss+="caracal_test_id=${CARATE_CARACAL_TEST_ID}"
ss+=$'\n'

[[ -n "${DM}" ]] || [[ -n "${DA}" ]] || [[ -n "${DI}" ]] || [[ -n "${SM}" ]] || [[ -n "${SA}" ]] || [[ -n "${SI}" ]] || [[ -n "${DSC}" ]] || [[ -n "${SSC}" ]] ||{\
    echo "Please use one of the switches -dm, -da, -di, -sm, -sa, -si, -dsc, -ssc";\
    echo "";\
    kill "$PPID"; exit 1;
}

if [[ -n "$CARATE_LOCAL_SOURCE" ]]
then
    [[ -d "${CARATE_LOCAL_SOURCE}" ]] || { \
	echo "\$CARATE_LOCAL_SOURCE: ${CARATE_LOCAL_SOURCE}";
	echo "is specified (environment variable \${CARATE_LOCAL_SOURCE})";
	echo "is set or switches --local-source or -ls are used). But it";
	echo "does not exist. Stopping.";
	kill "$PPID"; exit 1;
    }
    ss+="local_source=${CARATE_LOCAL_SOURCE}"
    ss+=$'\n'
else
    echo "The variable CARATE_LOCAL_SOURCE is not set, meaning that CARACal"
    echo "will be downloaded from https://github.com/ska-sa/caracal"
    echo ""
fi

if [[ -n "$CARATE_CONFIG_SOURCE" ]]
then
    [[ -f "${CARATE_CONFIG_SOURCE}" ]] || { \
	echo "\$CARATE_CONFIG_SOURCE: ${CARATE_CONFIG_SOURCE}";
	echo "is specified (environment variable \${CARATE_CONFIG_SOURCE})";
	echo "is set or switches --config-source or -cs are used). But it";
	echo "does not exist. Stopping.";
	kill "$PPID"; exit 1;
    }
    ss+="config_source=${CARATE_CONFIG_SOURCE}"
    ss+=$'\n'
else
    echo "The variable CARATE_CONFIG_SOURCE is not set and switches"
    echo "config-source and -cs are not used meaning that no CARACal"
    echo "test will be made on own supplied configuration."
    echo ""
fi

# Determine number of stimelas and allow only one
counts=0
[[ -z ${US} ]] || (( counts+=1 ))
[[ -z ${UM} ]] || { (( counts+=1 )); }
[[ -z ${CARATE_LOCAL_STIMELA} ]] || (( counts+=1 ))
(( ${counts}<2 )) || { \
    echo "Use maximally one of switches and variables defined with -us -um -lst."; \
    kill "$PPID"; \
    exit 1; \
    }

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

if [[ -n "$CARATE_VIRTUALENV" ]]
then
    ss+="cvirtualenv=${CARATE_VIRTUALENV}"
    ss+=$'\n'
else
    ss+="cvirtualenv=\${workspace_root}/caracal_venv"
    ss+=$'\n'
    echo "The variable CARATE_VIRTUALENV is not set and switches"
    echo "--virtualenv and -ve are not used meaning that the virtualenv"
    echo "will be created or re-used inside the test installation."
    echo ""
fi

if [[ -n "$CARATE_LOCAL_STIMELA" ]]
then
    ss+="local_stimela=${CARATE_LOCAL_STIMELA}"
    ss+=$'\n'
else
    echo "The variable CARATE_LOCAL_STIMELA is not set and switches"
    echo "--local-stimela and -lst are not used meaning that the virtualenv"
    echo "will be created or re-used as specified in the CARACal installation."
    echo ""
fi

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

if [[ -n "$CARATE_CONFIG_SOURCE" ]]
then
    if [[ -z $DI ]] && [[ -z $SI ]]
    then
        echo "No container technology in context with specifying an additional config"
        echo "file. Stopping."
	kill "$PPID";
	exit 1;
    else
	# Get the config file name
        configfilename=`echo $CARATE_CONFIG_SOURCE | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}'`
    fi
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
if (( ${FORCE} == 0 ))
then
    if [[ -d $WORKSPACE_ROOT ]]
    then
        echo "Be aware that no existing file will be replaced, use -f to override"
        echo
    fi
fi

# Protect existing directories and files
checkex () {
    # Check if the input $1 is a file and if yes if it is one of the user-
    # supplied files ${CARATE_CONFIG_SOURCE}, return 0 (true) if yes
    # Check the input is a directory and if yes if it is a parent or one of
    # the user-supplied files and directories ${CARATE_INPUT_DIR}
    # ${CARATE_LOCAL_SOURCE} ${CARATE_WORKSPACE} ${CARATE_TEST_DATA_DIR},
    # return 0 (true) if yes

    # Input:
    # $1: input file to check

    local tocheck=$1
    local a=""
    local b=""
    local c=""
    local d=""
    local e=""
    local file=""
    local files=""
    local subfile=""

    if [[ -d ${tocheck} ]]
    then
	files=(${CARATE_VIRTUALENV} ${CARATE_LOCAL_STIMELA} ${CARATE_INPUT_DIR} ${CARATE_LOCAL_SOURCE} ${CARATE_WORKSPACE} ${CARATE_TEST_DATA_DIR} ${CARATE_CONFIG_SOURCE})
	for file in ${files[@]}
	do
	    e=`[[ -e ${file} ]] && stat -c %i ${file} || echo ""`
	    [[ ${e} == "" ]] && unset e
	    [[ -z ${e} ]] || { \
		a=`basename $file`; \
		b=`find $tocheck -name $a` || true; \
		c=($b); \
		}
	    for subfile in ${c[@]}
	    do
		d=`stat -c %i $subfile`
		[[ ${e} != ${d} ]] || { return 0; }
	    done
	done
    else
	[[ -f ${tocheck} ]] && e=`stat -c %i ${tocheck}`
	files=(${CARATE_CONFIG_SOURCE})
	for file in ${files[@]}
	do
	    d=`stat -c %i $file`
	    [[ ${e} != ${d} ]] || { echo why; }
	done
    fi
    return 1
}

echo "##########################################"
echo " Setting up build in $WORKSPACE_ROOT"
echo "##########################################"
echo

ss+="mkdir -p \${workspace_root}"
mkdir -p ${WORKSPACE_ROOT}

[[ ! -d ${WORKSPACE_ROOT}/report ]] || (( ${FORCE} == 0 )) || \
    checkex ${WORKSPACE_ROOT}/report || \
    rm -rf ${WORKSPACE_ROOT}/report
mkdir -p ${WORKSPACE_ROOT}/report

# Small script, use txt suffix to be able to upload to multiple platforms
SS=${WORKSPACE_ROOT}/report/${CARATE_CARACAL_TEST_ID}.sh.txt

# Empty ss into the small script
[[ ! -e ${SS} ]] || (( ${FORCE} == 0 )) || checkex ${SS} || rm -rf ${SS}
echo "$ss" >> ${SS}

# Sysinfo
SYA=${WORKSPACE_ROOT}/report/${CARATE_CARACAL_TEST_ID}_sysinfo.txt

# Empty into the sysinfo
[[ ! -e ${SYA} ]] || (( ${FORCE} == 0 )) || checkex ${SYA} || rm -rf ${SYA}
echo "$sya" >> ${SYA}

# Search for test data and set variable accordingly
if [[ -e ${CARATE_TEST_DATA_DIR} ]]
then

    # Check if there are any ms files
    mss=`find ${CARATE_TEST_DATA_DIR} -name *.ms`
    [[ -n "$mss" ]] || [[ -n ${OD} ]] || { echo "Test data required in ${CARATE_TEST_DATA_DIR}"; echo ""; kill "$PPID"; exit 1; }

    # This generates the dataid string
    [[ -z "$mss" ]] && dataidstr="" || dataidstr=`ls -d ${CARATE_TEST_DATA_DIR}/*.ms | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}' | sed '{s/ /\x27,\x27/g; s/$/\x27\]/; s/^/dataid: \[\x27/}'`
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    [[ ! -z "$mss" ]] || sya=`ls -d ${CARATE_TEST_DATA_DIR}/*.ms | sed '{s=.*/==}' | sed '{:q;N;s/\n/, /g;t q}'`
    [[ ! -z "$mss" ]] && echo "Test data directory was not defined." >> ${SYA} || echo "Files found in test data directory: ${sya}" >> ${SYA}

    # Size of test data
    outsize=`du -ms ${CARATE_TEST_DATA_DIR} | awk '{print $1}'`
    echo "Total size of test data directory: ${outsize} MB" >> ${SYA}
    [[ -z ${KC} ]] || { echo "--keep-config-source or -kc switch is set."; \
			echo "The real test data might hence be different."; \
    }
    echo "" >> ${SYA}
fi

function cleanup {
    if (( success == 0 ))
    then
	echo "##########################################"
	echo ""
	echo ${kkfailquotes[$(( $RANDOM % 16 ))]}
	echo "Caratekit failed."
	echo ""
	echo "##########################################"
	echo ""
	echo "###########" >> ${SYA}
	echo >> ${SYA}
	echo "Test failed." >> ${SYA}
	echo >> ${SYA}
    else
	echo "###########################################################"
	echo ""
	echo ${kksuccessquotes[$(( $RANDOM % 2 ))]}
	echo "Caratekit succeeded."
	echo ""
	echo "###########################################################"
	echo
	echo "###############" >> ${SYA}
	echo >> ${SYA}
	echo "Test succeeded." >> ${SYA}
	echo >> ${SYA}
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

[[ -n ${KH} ]] || echo "export HOME=\${workspace_root}/home" >> ${SS}
[[ -n ${KH} ]] || export HOME=$WORKSPACE_ROOT/home
if (( ${FORCE} != 0 ))
then
    [[ -n ${KH} ]] || \
	[[ -n ${ORSR} ]] || \
	checkex ${WORKSPACE_ROOT}/home || \
	echo "rm -rf \${WORKSPACE_ROOT}/home" >> ${SS}
    # We could write rm -rf ${HOME} but we are not crazy, some young hacker makes one mistake...
    [[ -n ${KH} ]] || \
	[[ -n ${ORSR} ]] || \
	[[ -n ${FS} ]] || \
	checkex ${WORKSPACE_ROOT}/home || \
	rm -rf ${WORKSPACE_ROOT}/home
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

[[ ! -z ${CARATE_VIRTUALENV} ]] || CARATE_VIRTUALENV=${WORKSPACE_ROOT}/caracal_venv

# Set the virtual environment
if (( ${FORCE} != 0 ))
then
    [[ -n ${ORSR} ]] || \
	[[ -n ${OV} ]] || \
	[[ -n ${OC} ]] || { \
	    checkex ${CARATE_VIRTUALENV} && \
		[[ ${CARATE_VIRTUALENV} != ${WORKSPACE_ROOT}/caracal_venv ]]; \
	} || \
	echo "rm -rf \${cvirtualenv}" >> ${SS}

    [[ -n ${ORSR} ]] || \
	[[ -n ${OV} ]] || \
	[[ -n ${OC} ]] || \
	[[ -n ${FS} ]] || { \
	checkex ${CARATE_VIRTUALENV} && \
	    [[ ${CARATE_VIRTUALENV} != ${WORKSPACE_ROOT}/caracal_venv ]]; \
	} || \
	rm -rf ${CARATE_VIRTUALENV}
fi
if [[ ! -d ${CARATE_VIRTUALENV} ]]
then
    [[ -n ${FS} ]] && echo "python3 -m venv \${cvirtualenv}" >> ${SS}
    [[ -n ${FS} ]] || { python3 -m venv ${CARATE_VIRTUALENV} && echo "python3 -m venv \${cvirtualenv}" >> ${SS}; } || { echo 'Using "python3 -m venv" failed when instaling virtualenv.'; echo 'Trying "virtualenv -p python3"'; virtualenv -p python3 ${CARATE_VIRTUALENV} && echo "virtualenv -p python3 \${cvirtualenv}" >> ${SS}; }
fi

# Report on virtualenv
if [[ -f ${CARATE_VIRTUALENV}/pyvenv.cfg ]]
then
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    echo "Virtualenv info (from pyvenv.cfg):" >> ${SYA}
    cat ${CARATE_VIRTUALENV}/pyvenv.cfg >> ${SYA}
    echo "" >> ${SYA}
fi

echo "Entering virtualenv in $WORKSPACE_ROOT"
echo ". \${cvirtualenv}/bin/activate" >> ${SS}
[[ -n ${FS} ]] || . ${CARATE_VIRTUALENV}/bin/activate

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
echo ""

# Install software
echo "##################"
echo " Fetching CARACal "
echo "##################"
echo
if (( ${FORCE} == 1 ))
then
    checkex ${WORKSPACE_ROOT}/carate || \
	[[ -n ${OF} ]] || \
	echo "rm -rf \${workspace_root}/carate" >> ${SS}
    [[ -n ${FS} ]] || \
	checkex ${WORKSPACE_ROOT}/carate || \
	[[ -n ${OF} ]] || \
	rm -rf ${WORKSPACE_ROOT}/carate
fi

echo "cd \${workspace_root}" >> ${SS}
cd ${WORKSPACE_ROOT}
if [[ -n "$CARATE_LOCAL_SOURCE" ]]
then
    if [[ -e ${WORKSPACE_ROOT}/carate ]]
    then
        echo "Not re-fetching CARACal, use -f if you want that or"
        echo "omit -of if you have set it."
	echo ""
    else
	echo "Fetching CARACal from local source ${local_source}"
	echo
        echo "cp -r \${local_source} \${workspace_root}/"  >> ${SS}
	[[ -n ${FS} ]] || cp -r ${CARATE_LOCAL_SOURCE} ${WORKSPACE_ROOT}/
    fi
else
    if [[ -e ${WORKSPACE_ROOT}/carate ]]
    then
        if (( ${FORCE} == 0 ))
        then
            echo "Not re-fetching CARACal, use -f if you want that."
	elif [[ -n ${OF} ]]
	then
	    echo "Not re-fetching CARACal, turn off -of if you want that."
        else
	    echo "Fetching CARACal from https://github.com/ska-sa/caracal.git"
	    checkex ${WORKSPACE_ROOT}/carate || \
		echo "rm -rf \${workspace_root}/carate" >> ${SS}
	    [[ -n ${FS} ]] || \
		checkex ${WORKSPACE_ROOT}/carate || \
		rm -rf ${WORKSPACE_ROOT}/carate

            checkex ${WORKSPACE_ROOT}/carate || \
		echo "git clone https://github.com/ska-sa/caracal.git" >> ${SS}
            [[ -n ${FS} ]] || \
		checkex ${WORKSPACE_ROOT}/carate || \
		git clone https://github.com/ska-sa/caracal.git
        fi
    else
	echo "Fetching CARACal from https://github.com/ska-sa/caracal.git"
	echo "git clone https://github.com/ska-sa/caracal.git" >> ${SS}
	[[ -n ${FS} ]] || git clone https://github.com/ska-sa/caracal.git
    fi
fi

if [[ -n "$CARATE_CARACAL_BUILD_NUMBER" ]]
then
    echo "cd \${workspace_root}/carate" >> ${SS}
    cd ${WORKSPACE_ROOT}/carate
    [[ -z $CARATE_LOCAL_SOURCE ]] || { \
	echo "If an error occurs here, it likely means that the local installation";\
	echo "of CARACal does not contain the build number. You may want to use the";\
	echo "master branch and unset the environmrnt variable CARATE_CARACAL_BUILD_NUMBER:";\
	echo "In bash: $ unset CARATE_CARACAL_BUILD_NUMBER";\
	echo "Also, you might have used switches --caracal-build-number or -cb, which";\
	echo "you should not";\
    }
    echo "git checkout ${CARATE_CARACAL_BUILD_NUMBER}" >> ${SS}
    git checkout ${CARATE_CARACAL_BUILD_NUMBER}
fi


if [[ -d ${WORKSPACE_ROOT}/carate ]]
then
    # Report on CARACal build
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    cd ${WORKSPACE_ROOT}/carate
    if [[ -n "$CARATE_LOCAL_SOURCE" ]]
    then
	sya="CARACal build: local"; sya+=$'\n';
    else
        sya="CARACal build: "; sya+=`git log -1 --format=%H`; sya+=$'\n';
	sya+="from: https://github.com/ska-sa/caracal"; sya+=$'\n';
    fi
    echo "${sya}" >> ${SYA}
#    echo ""  >> ${SYA}

    # Get Stimela tag. This can be simplified...
    if [[ -n ${US} ]]
    then
        stimelaline=`grep "https://github.com/ratt-ru/Stimela" stimela_last_stable.txt | sed -e 's/.*Stimela@\(.*\)#egg.*/\1/'` || true
        if [[ -z ${stimelaline} ]]
        then
	    # Stimela tag depends on whether the repository is in or not
            stimelaline=`grep "stimela==" setup.py | sed -e 's/.*==\(.*\)\x27.*/\1/'` || true
            [[ -z ${stimelaline} ]] || echo "Stimela release: $stimelaline" >> ${SYA}
            stimelaline=`grep https://github.com/ratt-ru/Stimela setup.py` || true
            [[ -z ${stimelaline} ]] || stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'` || true
            [[ -z ${stimelaline} ]] || echo "Stimela build: ${stimelabuild}" >> ${SYA}
        else
            echo "Stimela build: ${stimelaline}" >> ${SYA}
        fi
    elif [[ -n ${UM} ]]
    then
        # Stimela tag depends on whether the repository is in or not
        stimelaline=`grep https://github.com/ratt-ru/Stimela stimela_master.txt` || true
	[[ -z ${stimelaline} ]] || stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'`
        [[ -z ${stimelaline} ]] || echo "Stimela build: ${stimelabuild}" >> ${SYA}
    elif [[ -n ${CARATE_LOCAL_STIMELA} ]]
    then
	echo "Stimela build: ${CARATE_LOCAL_STIMELA} (local)" >> ${SYA}
    else
        # Stimela tag depends on whether the repository is in or not
        stimelaline=`grep "stimela==" setup.py | sed -e 's/.*==\(.*\)\x27.*/\1/'` || true
        [[ -z ${stimelaline} ]] || echo "Stimela release: $stimelaline" >> ${SYA}

        [[ -n ${stimelaline} ]] || stimelaline=`grep 'https://github.com/ratt-ru/Stimela' setup.py` || true
        [[ -z ${stimelaline} ]] || [[ -n ${stimelabuild} ]] || stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'` || true
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
if [[ -z ${OC} ]]
then
    echo "Installing CARACal using pip install"
    echo "pip install -U --force-reinstall \${workspace_root}/carate" >> ${SS}
    [[ -n ${FS} ]] || pip install -U --force-reinstall ${WORKSPACE_ROOT}/caracal
fi

if [[ -n ${UM} ]]
then
    echo "Installing stimela_master.txt"
    echo "pip install -U --force-reinstall -r \${workspace_root}/carate/stimela_master.txt" >> ${SS}
    [[ -n ${FS} ]] || pip install -U --force-reinstall -r ${WORKSPACE_ROOT}/caracal/stimela_master.txt
fi

if [[ -n ${US} ]]
then
    echo "Installing stimela_last_stable.txt"
    echo "pip install -U --force-reinstall -r \${workspace_root}/carate/stimela_last_stable.txt" >> ${SS}
    [[ -n ${FS} ]] || pip install -U --force-reinstall -r ${WORKSPACE_ROOT}/carate/stimela_last_stable.txt
fi
if [[ -n ${CARATE_LOCAL_STIMELA} ]]
then
    echo "Installing local stimela ${CARATE_LOCAL_STIMELA}"
    echo "pip install -U --force-reinstall \${local_stimela}" >> ${SS}
    [[ -n ${FS} ]] || pip install -U --force-reinstall ${CARATE_LOCAL_STIMELA}
fi

if [[ -z $DM ]] && [[ -z $DA ]] && [[ -z $DI ]] && [[ -z $SM ]] && [[ -z $SA ]] && [[ -z $SI ]] && [[ -z $DSC ]] && [[ -z $SSC ]]
then
    echo "You have not defined a test:"
    echo "--docker-minimal or -dm"
    echo "--docker-alternative or -da"
    echo "--docker-installation or -di"
    echo "--singularity-minimal or -sm"
    echo "--singularity-alternative or -sa"
    echo "--singularity-installation or -si"
    echo "--docker-sample-configs or -dsc"
    echo "--singularity-sample-configs or -ssc"
    echo "Use -h flag for more information"
    kill "$PPID"; exit 0
fi

if [[ -z ${ORSR} ]]
then
    checkex ${HOME}/.stimela || { \
	echo "#############################" ; \
	echo " Removing Stimela directory " ; \
	echo "#############################" ; \
	echo "" ; \
	echo "Removing \${HOME}/.stimela/*" ; \
	echo "rm -f \${HOME}/.stimela/*" >> ${SS} ; \
    }
    [[ -n ${FS} ]] || checkex ${HOME}/.stimela || rm -f ${HOME}/.stimela/*
fi

if [[ -n $DM ]] || [[ -n $DA ]] || [[ -n $DI ]] || [[ -n $DSC ]]
then
    # Prevent special characters to destroy installation
    stimela_ns=`whoami`
    stimela_bs=`whoami | sed 's/@/_/g'`
    [[ ${stimela_ns} == ${stimela_bs} ]] && stimela_ns="" || stimela_ns=" -bl ${stimela_bs}"

    if [[ -n ${ORSR} ]]
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
	    if (( ${ii} > ${IA} ))
	    then
		echo "Maximum number of pull attempts for Stimela reached."
		echo "Maximum number of pull attempts for Stimela reached." >> ${SYA}
		exit 1
	    fi
        fi
	ii=1
        until (( ${ii} > ${IA} ))
        do
            echo "Running stimela build"
            echo "stimela build${stimela_ns}" >> ${SS}
            if [[ -z ${FS} ]]
	    then
	        stimela build${stimela_ns} && break || {
			echo "stimela build failed"
			(( ii++ ))
		    }
	    else
		break
	    fi
        done
	if (( ${ii} > ${IA} ))
	then
	    echo "Maximum number of build attempts for Stimela reached."
	    exit 1
	fi
    fi
    echo ""
fi

testingoutput () {

    # Function to test output after running a pipeline
    # Argument 1: $WORKSPACE_ROOT
    # Argument 2: Test directory, e.g. test_extendedConfig_docker
    local caracallog
    local allogs
    local total
    local log
    local hadcaracal
    local hadcaracal2
    local reporting
    local caracallogsh
    local worker_runs
    local worker_fins

    echo
    echo "###################"
    echo " Counting logfiles "
    echo "###################"
    echo
    echo "Counting logfiles in directory ${1}/${2}"
    # Check here
    allogs=`[[ -e ${1}/${2}/output/logs ]] && ls -t ${1}/${2}/output/logs/ || echo ""`
#    allogs=`ls -t ${1}/${2}/output/logs/` || true
    caracallog=`[[ -e ${1}/${2}/output/logs ]] && ls -t ${1}/${2}/output/logs/log-caracal-*.txt | head -1 || echo ""`
    [[ ${caracallog} == "" ]] && unset caracallog
#    caracallog=`ls -t ${1}/${2}/output/logs/log-caracal-*.txt.txt | head -1 || true`
    [[ -z ${caracallog} ]] || { reporting+="This CARACal run is logged in ${caracallog}"; reporting+=$'\n'; }
    [[ -z ${caracallog} ]] || { caracallogsh=`echo ${caracallog} | sed '{s=.*/==;}'`; }
    total=0

    for log in ${allogs}
    do
        (( total+=1 ))
	[[ -z $hadcaracal2 ]] || { reporting+="$log is the second last log before ${caracallogsh}"; \
				     reporting+=$'\n'; unset hadcaracal2; }
	[[ -z $hadcaracal ]] || { reporting+="$log is the last log before ${caracallogsh}"; reporting+=$'\n'; \
				    hadcaracal2=1; unset hadcaracal; }
        [[ ${log} != ${caracallogsh} ]] || hadcaracal=1
    done
    reporting+="Total number of logfiles: $total";
    echo "$reporting"
    echo "$reporting" >> ${SYA}

    # Count number of runs of workers and the number of finishes
    worker_runs=0
    worker_fins=0
    [[ -z ${caracallog} ]] && { \
        reporting="This CARACal run is missing a summary logfile"; reporting+=$'\n'; \
	reporting+="Returning error";\
	reporting+=$'\n'; \
	echo "${reporting}"; \
	echo "${reporting}" >> ${SYA}; \
	return 1; \
    } || { \
	worker_runs=`grep ": initializing" ${caracallog} | wc | sed 's/^ *//; s/ .*//'`; \
	worker_fins=`grep ": finished" ${caracallog} | wc | sed 's/^ *//; s/ .*//'`; \
	reporting="CARACal logfile indicates ${worker_runs} workers starting"; \
	reporting+=$'\n'; \
	reporting+="and ${worker_fins} workers ending."; reporting+=$'\n'; \
	echo "${reporting}"; \
	echo "${reporting}" >> ${SYA}; \
    }


    (( $worker_runs == $worker_fins )) || { reporting="Workers starting (${worker_runs}) and ending (${worker_fins}) are unequal in log-caracal.txt"; \
					    reporting+=$'\n'; \
					    echo "${reporting}"; \
					    echo "${reporting}" >> ${SYA}; \
					    return 1; }
    [[ -z $caracallog ]] || (( $worker_runs > 0 )) || { reporting="No workers have started according to log-caracal.txt"; \
				reporting+=$'\n'; \
				reporting+="Returning error";\
				reporting+=$'\n'; \
				echo "${reporting}"; \
				echo "${reporting}" >> ${SYA}; \
				return 1; }

    # Notice that 0 is true in bash
    (( $total > 0 )) || { reporting="No logfiles produced. Returning error."; reporting+=$'\n'; \
                          "echo ${reporting}" \
			  "echo ${reporting}" >> ${SYA}; \
                          return 1; }
    [[ -n ${caracallog} ]] || { reporting="No CARACal main log produced. Returning error."; reporting+=$'\n'; \
                          "echo ${reporting}" \
			  "echo ${reporting}" >> ${SYA}; \
                          return 1; }
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
    # Argument 8: Running test run number
    # Argument 9: Switches to pass to caracal

    local greetings_line=$1
    local WORKSPACE_ROOT=$2
    local configfilename=$3
    local contarch=$4
    local FORCE=$5
    local configlocation=$6
    local configlocationstring=$7
    local testruns=$8
    local caracalswitches=$9

    local sya
    local trname
    local failedrun
    local dirs
    local d
    local e
    local failed
    local mes
    local caracallog
    local failedoutput
    local outsize

    echo "##########################################"
    echo " $greetings_line "
    echo "##########################################"
    echo

    failedrun=0

    if [[ -n ${CARATE_CARACAL_RUN_PREFIX} ]]
    then
	(( ${testruns} == 1 )) && trname=${CARATE_CARACAL_RUN_PREFIX} || trname=${CARATE_CARACAL_RUN_PREFIX}_${testruns}
    else
	trname=test_${configfilename}_${contarch}
    fi


    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    sya="${trname} preparation start time:";sya+=$'\n'; sya+=`date -u`;
    echo "${sya}" >> ${SYA}

    if [[ -e ${WORKSPACE_ROOT}/${trname} ]] && (( ${FORCE} == 0 ))
    then
        echo "Will not re-create existing directory ${WORKSPACE_ROOT}/${trname}"
        echo "and use old results. Use -f to override."
    else
	[[ -z ${OD} ]] || \
	    [[ ! -d ${WORKSPACE_ROOT}/${trname}/msdir ]] && { \
		CARATE_TEST_DATA_DIR=${CARATE_TEST_DATA_DIR_OLD}; \
	    } || { \
		CARATE_TEST_DATA_DIR=${WORKSPACE_ROOT}/${trname}/msdir;\
	    }

	#Check if the test directory is a parent of any of the supplied directories
	if checkex ${WORKSPACE_ROOT}/${trname}
	then
	    # Go through the files and remove individually
	    # continue here
	    dirs=(input msdir output stimela_parameter_files)
	    for dire in ${dirs[@]}
	    do
		checkex ${WORKSPACE_ROOT}/${trname}/${dire} || \
		    echo "rm -rf \${workspace_root}/${trname}/${dire}" >> ${SS}
		[[ -n ${FS} ]] || \
		    checkex ${WORKSPACE_ROOT}/${trname}/${dire} || \
		    rm -rf ${workspace_root}/${trname}/${dire}
	    done
	else
	    echo "rm -rf \${workspace_root}/${trname}" >> ${SS}
            [[ -n ${FS} ]] || \
		rm -rf ${WORKSPACE_ROOT}/${trname}
	fi

        echo "Preparing ${contarch} test (using ${configfilename}.yml) in"
        echo "${WORKSPACE_ROOT}/${trname}"
	echo "mkdir -p \${workspace_root}/${trname}/msdir" >> ${SS}
        mkdir -p ${WORKSPACE_ROOT}/${trname}/msdir
	if [[ -d ${CARATE_INPUT_DIR} ]]
	then
	    mkdir -p ${WORKSPACE_ROOT}/${trname}/input
	    checkex ${WORKSPACE_ROOT}/${trname}/input || cp -r ${CARATE_INPUT_DIR}/* ${WORKSPACE_ROOT}/${trname}/input/
	fi

	# Check if user-supplied file is already the one that we are working with before working with it
	# This should in principle only affect the time stamps as if the dataid is not empty, the following
	# Would do nothing in the config file itself
	checkex ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml || { \
	    [[ -n ${KC} ]] || echo "sed \"s/dataid: \[.*\]/$dataidstr/\" ${configlocationstring} > \${workspace_root}/${trname}/${configfilename}.yml" >> ${SS}; \
	    [[ -z ${KC} ]] || echo "cp ${configlocationstring} \${workspace_root}/${trname}/${configfilename}.yml" >> ${SS}; \
	}

	[[ -n ${FS} ]] || \
	    checkex ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml || { \
		[[ -n ${KC} ]] || sed "s/dataid: \[.*\]/$dataidstr/" ${configlocation} > ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml; \
		[[ -z ${KC} ]] || cp ${configlocationstring} \${workspace_root}/${trname}/${configfilename}.yml; \
	    }

	# This prevents the script to stop if there -fs is switched on
	[[ ! -f ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml ]] || \
	    cp ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml ${WORKSPACE_ROOT}/report/${configfilename}_${contarch}.yml.txt

	# Check if source msdir is identical to the target msdir. If yes, don't copy
	d=`stat -c %i $CARATE_TEST_DATA_DIR`
	e=`stat -c %i ${WORKSPACE_ROOT}/${trname}/msdir`
	[[ $d == $e ]] || \
	    echo "cp -r \${test_data_dir}/*.ms \${workspace_root}/${trname}/msdir/" >> ${SS}
	[[ $d == $e ]] || \
	    [[ -n ${FS} ]] || \
	    cp -r $CARATE_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/${trname}/msdir/

	# Now run the test
        echo "Running ${contarch} test (using ${configfilename}.yml)"
	echo "cd \${workspace_root}/${trname}" >> ${SS}
        cd ${WORKSPACE_ROOT}/${trname}

        # Notice that currently all output will be false, such that || true is required to ignore this
	failed=0
	echo caracal -c ${configfilename}.yml ${caracalswitches} || true

	# Report CARACal start time
        sya="${trname} start time:"; sya+=$'\n'; sya+=`date -u`;
        echo "${sya}" >> ${SYA}

	echo "caracal -c ${configfilename}.yml ${caracalswitches}" >> ${SS}
	[[ -n ${FS} ]] || caracal -c ${configfilename}.yml ${caracalswitches} || { true; failedrun=1; }
    fi

    if [[ ${failedrun} == 0 ]]
    then
	mes="CARACal run ${trname} did not return an error."
	echo ${mes}
	echo ${mes} >> ${SYA}
    else
	mes="CARACal run ${trname} returned an error."
	echo ${mes}
	echo ${mes} >> ${SYA}
    fi

    # Make a copy of the logfile
    caracallog=`[[ -e ${WORKSPACE_ROOT}/${trname}/output/logs ]] && ls -t ${WORKSPACE_ROOT}/${trname}/output/logs/log-caracal-*.txt | head -1 || echo ""`
    [[ ! -f ${caracallog} ]] || cp ${caracallog} ${WORKSPACE_ROOT}/report/log-caracal_${trname}.txt
    echo "Checking output of ${configfilename} ${contarch} test"
    failedoutput=0
    testingoutput ${WORKSPACE_ROOT} ${trname} || { true; failedoutput=1; }

    #    failedoutput=$?
    if [[ ${failedoutput} == 0 ]]
    then
	mes="CARACal run ${trname} did not return a faulty output."
	echo ${mes}
	echo ${mes} >> ${SYA}
    else
	mes="This CARACal ${trname} returned a faulty output."
	echo ${mes}
	echo ${mes} >> ${SYA}
    fi
    echo ""

    sya="${trname} end time:"; sya+=$'\n'; sya+=`date -u`
    echo "${sya}" >> ${SYA}


    if (( ${failedrun} == 1 || ${failedoutput} == 1 ))
    then
#        echo "##################"
#        echo " caratekit failed "
#        echo "##################"
#        echo
#        echo "################" >> ${SYA}
#        echo >> ${SYA}
#        echo "caratekit failed " >> ${SYA}
#        echo >> ${SYA}
#        echo "################" >> ${SYA}
#        echo  >> ${SYA}
        kill "$PPID"
        exit 1
    fi

    # Size of test
    outsize=`du -ms ${WORKSPACE_ROOT}/${trname} | awk '{print $1}'`
    echo "Final test folder size (${trname}): ${outsize} MB" >> ${SYA}
    echo "" >> ${SYA}
}

runtestsample () {
    # Running a specific caracal test using a specific combination of configuration file, architecture, and containerization
    # Argument 1: Line appearing at the start of function
    # Argument 2: $WORKSPACE_ROOT
    # Argument 3: configuration file name without "yml"
    # Argument 4: containerisation architecture "docker" or "singularity"
    # Argument 5: delete existing files or not
    # Argument 6: Location of where the modified configfile is written
    # Argument 7: Location of the configfile to be checked, string to pass to the output
    # Argument 8: Switches to pass to caracal

    local greetings_line=$1
    local WORKSPACE_ROOT=$2
    local configfilename=$3
    local contarch=$4
    local FORCE=$5
    local configlocation=$6
    local inputconfiglocation=$7
    local caracalswitches=$8

    local sya
    local trname
    local failedrun
    local dirs
    local d
    local e
    local failed
    local mes
    local caracallog
    local failedoutput
    local outsize

    echo "##########################################"
    echo " $greetings_line "
    echo "##########################################"
    echo

    failedrun=0

    if [[ -n ${CARATE_CARACAL_RUN_PREFIX} ]]
    then
	(( ${testruns} == 1 )) && trname=${CARATE_CARACAL_RUN_PREFIX} || trname=${CARATE_CARACAL_RUN_PREFIX}_${testruns}
    else
	trname=test_config_sample_${contarch}
    fi

    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    sya="${trname} preparation start time:";sya+=$'\n'; sya+=`date -u`;
    echo "${sya}" >> ${SYA}

    if [[ -e ${WORKSPACE_ROOT}/${trname} ]] && (( ${FORCE} == 0 ))
    then
        echo "Will not re-create existing directory ${WORKSPACE_ROOT}/${trname}"
    else
	[[ -z ${OD} ]] || \
	    [[ ! -d ${WORKSPACE_ROOT}/${trname}/msdir ]] && \
		CARATE_TEST_DATA_DIR=${CARATE_TEST_DATA_DIR_OLD} || \
		    CARATE_TEST_DATA_DIR=${WORKSPACE_ROOT}/${trname}/msdir

	#Check if the test directory is a parent of any of the supplied directories
	if checkex ${WORKSPACE_ROOT}/${trname}
	then
	    # Go through the files and remove individually
	    # continue here
	    dirs=(input  msdir  output  stimela_parameter_files)
	    for dire in ${dirs[@]}
	    do
		checkex ${WORKSPACE_ROOT}/${trname}/${dire} || \
		    echo "rm -rf \${workspace_root}/${trname}/${dire}" >> ${SS}
		[[ -n ${FS} ]] || \
		    checkex ${WORKSPACE_ROOT}/${trname}/${dire} || \
		    rm -rf \${workspace_root}/${trname}/${dire}
	    done
	else
	    echo "rm -rf \${workspace_root}/${trname}" >> ${SS}
            [[ -n ${FS} ]] || \
	    rm -rf ${WORKSPACE_ROOT}/${trname}
	fi

	echo "Preparing ${contarch} test (using ${configfilename}.yml) in"
	echo "${WORKSPACE_ROOT}/${trname}"
	echo "mkdir -p \${workspace_root}/${trname}/msdir" >> ${SS}
	mkdir -p ${WORKSPACE_ROOT}/${trname}/msdir

	if [[ -d ${CARATE_INPUT_DIR} ]]
	then
	    mkdir -p ${WORKSPACE_ROOT}/${trname}/input
	    checkex ${WORKSPACE_ROOT}/${trname}/input || cp -r ${CARATE_INPUT_DIR}/* ${WORKSPACE_ROOT}/${trname}/input/
	fi
    fi

    # Check if source msdir is identical to the target msdir. If yes, don't copy
    d=`stat -c %i $CARATE_TEST_DATA_DIR`
    e=`stat -c %i ${WORKSPACE_ROOT}/${trname}/msdir`
    [[ $d == $e ]] || \
	echo "cp -r \${test_data_dir}/*.ms \${workspace_root}/${trname}/msdir/" >> ${SS}
    [[ $d == $e ]] || \
	[[ -n ${FS} ]] || \
	cp -r $CARATE_TEST_DATA_DIR/*.ms ${WORKSPACE_ROOT}/${trname}/msdir/

    #We need to take the config file from the meerkat input to our test directoru, always
    cp ${inputconfiglocation} ${configlocation}

    # then we need to setup for the test
    # first put in the data ID
    # sed "s/dataid: \[\x27\x27\]/$dataidstr/"
    sed -i "s/dataid: \[.*\]/${dataidstr}/" ${configlocation}

    # then replace all enable true with false
    sed -i "s/enable: true/enable: false/gI" ${configlocation}

    # And then run caracal
    echo "Running ${contarch} test (using ${configfilename}.yml)"
    cd ${WORKSPACE_ROOT}/${trname}

    # Notice that currently all output will be false, such that || true is required to ignore this
    failed=0
    echo caracal -c ${configfilename}.yml ${caracalswitches} || true

    # Report CARACal start time
    sya="${trname} start time:"; sya+=$'\n'; sya+=`date -u`;
    echo "${sya}" >> ${SYA}
	  [[ -n ${FS} ]] || caracal -c ${configfilename}.yml ${caracalswitches} || { true; failedrun=1; }
    echo "${failedrun}"
    if [[ ${failedrun} == 0 ]]
    then
    	mes="CARACal test for ${configfilename}.yml did not return an error."
	echo ${mes}
    else
	mes="CARACal test for ${configfilename}.yml was unsuccesful."
	echo ${mes}
	kill "$PPID"
	exit 1
    fi
    echo "" >> ${SYA}
}

# Test run number
testrunnumber=0

if [[ -n ${DM} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Docker: minimalConfig"
    confilename="minimalConfig"
    contarch="docker"
    caracalswitches="${stimela_ns}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/carate/caracal/sample_configurations/${confilename}.yml" "\{workspace_root}/carate/caracal/sample_configurations/${confilename}.yml" "${testrunnumber}" "${caracalswitches}"
fi

if [[ -n ${DA} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Docker: (alternative) carateConfig"
    confilename="carateConfig"
    contarch="docker"
    caracalswitches="${stimela_ns}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/carate/caracal/sample_configurations/${confilename}.yml" "\${workspace_root}/carate/caracal/sample_configurations/${confilename}.yml" "${testrunnumber}" "${caracalswitches}"
fi

if [[ -n ${DI} ]] && [[ -n ${configfilename} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Docker: $configfilename"
    confilename=$configfilename
    contarch="docker"
    caracalswitches="${stimela_ns}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${CARATE_CONFIG_SOURCE}" "\${config_source}" "${testrunnumber}" "${caracalswitches}"
fi

if [[ -n ${DSC} ]]
then
    (( testrunnumber+=1 ))
    echo "####################################"
    echo " Testing all sample configurations "
    echo "####################################"
    greetings_line="Docker: Testing Sample configurations"
    contarch="docker"
    caracalswitches="${stimela_ns}"
    # First we need to know all the sample configurations present that are not old
    if [[ -n "$CARATE_LOCAL_SOURCE" ]]
    then
      echo "Checking the configurations in ${CARATE_LOCAL_SOURCE}/caracal/sample_configurations/"
      sample_location="${CARATE_LOCAL_SOURCE}/caracal/sample_configurations"
    else
      echo "You are checking the configurations in the remote master"
      echo "That seems silly but ok."
      echo ""
      sample_location="${WORKSPACE_ROOT}/carate/caracal/sample_configurations"
    fi
    for entry in "${sample_location}"/*
    do
      filename=${entry##*/}
      # check that it is not old
      if [[ $filename != *"old"* ]]
      then
        #Check it is a yml file
        if [[ $filename == *".yml"* ]]
        then
          confilename=${filename%.yml}
          runtestsample "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/${trname}/${confilename}.yml" "${sample_location}/${confilename}.yml" "${testrunnumber}" "${caracalswitches}"
          #make sure that only in the first instance a new directory is created
          FORCE=0
        fi
      fi
    done
fi

if [[ -n $SM ]] || [[ -n $SA ]] || [[ -n $SI ]] || [[ -n $SSC ]]
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
    if (( ${FORCE} == 0 )) || [[ -n ${ORSR} ]]
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
	checkex ${singularity_loc} || \
	    echo "rm -rf ${singularity_locstring}" >> ${SS}
        [[ -n ${FS} ]] || checkex ${singularity_loc} || \
	    checkex ${singularity_loc} || \
	    rm -rf ${singularity_loc}
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
	if (( ${ii} > ${IA} ))
	then
	    echo "Maximum number of pull attempts for Stimela reached."
	    echo "Maximum number of pull attempts for Stimela reached." >> ${SYA}
	    exit 1
	fi
    fi

    # Size of images
    outsize=`du -ms ${singularity_loc} | awk '{print $1}'`
    echo "Singularity image folder size: ${outsize} MB" >> ${SYA}
    echo "" >> ${SYA}
fi

if [[ -n ${SM} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Singularity: minimalConfig_singularity"
    confilename="minimalConfig"
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${singularity_loc}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/carate/caracal/sample_configurations/${confilename}.yml" "\${workspace_root}/carate/caracal/sample_configurations/${confilename}.yml" "${testrunnumber}" "${caracalswitches}"
fi

if [[ -n ${SA} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Singularity: (alternative) carateConfig"
    confilename="carateConfig"
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${singularity_loc}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/carate/caracal/sample_configurations/${confilename}.yml" "\${workspace_root}/carate/caracal/sample_configurations/${confilename}.yml" "${testrunnumber}" "${caracalswitches}"
fi

if [[ -n ${SI} ]] && [[ -n ${configfilename} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Singularity: $configfilename"
    confilename=$configfilename
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${singularity_loc}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${CARATE_CONFIG_SOURCE}" "\${config_source}" "${testrunnumber}" "${caracalswitches}"
fi

if [[ -n ${SSC} ]]
then
    (( testrunnumber+=1 ))
    echo "####################################"
    echo " Testing all sample configurations "
    echo "####################################"
    greetings_line="Singularity: Testing Sample configurations"
    contarch="singularity"
    caracalswitches="--container-tech singularity -sid ${singularity_loc}"
    # First we need to know all the sample configurations present that are not old
    if [[ -n "$CARATE_LOCAL_SOURCE" ]]
    then
      echo "Checking the configurations in ${CARATE_LOCAL_SOURCE}/caracal/sample_configurations/"
      sample_location="${CARATE_LOCAL_SOURCE}/caracal/sample_configurations"
    else
      echo "You are checking the configurations in the remote master"
      echo "That seems silly but ok."
      echo ""
      sample_location="${WORKSPACE_ROOT}/carate/caracal/sample_configurations"
    fi
    for entry in "${sample_location}"/*
    do
      filename=${entry##*/}
      # check that it is not old
      if [[ $filename != *"old"* ]]
      then
        #Check it is a yml file
        if [[ $filename == *".yml"* ]]
        then
          confilename=${filename%.yml}
          runtestsample "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/${trname}/${confilename}.yml" "${sample_location}/${confilename}.yml" "${testrunnumber}" "${caracalswitches}"
          #make sure that only in the first instance a new directory is created
          FORCE=0
        fi
      fi
    done
fi


success=1

exit 0
