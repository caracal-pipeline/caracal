#!/usr/bin/env bash
# The following ensures the script to stop on errors
set -e

# Rule: Two newlines before ###, none at end except for the very end

# Find out which caratekit this is, will be needed later
caratekit_install=`which caratekit.sh` || caratekit_install=""
[[ -n ${caratekit_install} ]] || unset caratekit_install

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
echo "#########################"
echo " Testing CARACal at home "
echo "#########################"
echo ""
echo "caratekit.sh $*"
echo ""

sya="############################" ; sya+=$'\n'
sya+=$'\n'
sya+="Caratekit system information" ; sya+=$'\n'
sya+=$'\n'
sya+="############################" ; sya+=$'\n'
sya+=$'\n'
sya+="Start time: "; sya+=`date -u`; sya+=$'\n'
sya+="Call executing: caratekit.sh $*"; sya+=$'\n'
sya+=$'\n'
sya+="############################" ; sya+=$'\n'
sya+=$'\n'
hostnamectl &>/dev/null && hostinfo=`hostnamectl | grep -Ev "Machine ID"'|'"Boot ID"` || hostinfo="Not accessible"
sya+="Host info: ";sya+=$'\n'
sya+=${hostinfo}
#sya+=$'\n'
# Control if help should be switched on
#(( $# > 0 )) || NOINPUT=1

# Default variables
FORCE=0
SS="/dev/null"
IA=5
endimessage=""
ALTERNATIVE_TEST_DATA_IDS=( "1524929477" "1524947605" "1532022061" )
SHORT_TEST_DATA_IDS=( "1532022061_subset" )

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
    if [[ "$arg" == "--verbose-help" ]] || [[ "$arg" == "-v" ]]
    then
        VE=1
    fi
        if [[ "$arg" == "--data-reduction-info" ]] || [[ "$arg" == "-dr" ]]
    then
        DR=1
    fi
    if [[ "$arg" == "--install" ]] || [[ "$arg" == "-i" ]]
    then
        IN=1
    fi
    if [[ "$arg" == "--home-original" ]] || [[ "$arg" == "-ho" ]]
    then
        HO=1
	# This cannot be deleted, double security
	HN=1
    fi
    if [[ "$arg" == "--home-workspace" ]] || [[ "$arg" == "-hw" ]]
    then
        HW=1
    fi
    if [[ "$arg" == "--home-no-delete" ]] || [[ "$arg" == "-hn" ]]
    then
        HN=1
    fi
    if [[ "$arg" == "--home-folder" ]] || [[ "$arg" == "-hf" ]]
    then
	(( nextcount=argcount+1 ))
	(( $nextcount <= $# )) || { echo "Argument expected for --home-folder or -hf switch, stopping."; kill "$PPID"; exit 1; }

	CARATE_HOME_FOLDER=${!nextcount}
 	firstletter=`echo ${CARATE_HOME_FOLDER} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_HOME_FOLDER="${cwd}/${CARATE_HOME_FOLDER}"
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

    if [[ "$arg" == "--singularity-cache-workspace" ]] || [[ "$arg" == "-scw" ]]
    then
	SCW=1
    fi

    if [[ "$arg" == "--singularity-localcache-workspace" ]] || [[ "$arg" == "-slw" ]]
    then
	SLW=1
    fi

    if [[ "$arg" == "--singularity-tmp-workspace" ]] || [[ "$arg" == "-stw" ]]
    then
	STW=1
    fi
    
    if [[ "$arg" == "--stimela-pull-workspace" ]] || [[ "$arg" == "-spw" ]]
    then
	SPW=1
    fi
    
    if [[ "$arg" == "--singularity-cache-home" ]] || [[ "$arg" == "-sch" ]]
    then
	SCH=1
    fi
    
    if [[ "$arg" == "--singularity-localcache-home" ]] || [[ "$arg" == "-slh" ]]
    then
	SLH=1
    fi
    
    if [[ "$arg" == "--singularity-tmp-home" ]] || [[ "$arg" == "-sth" ]]
    then
	STH=1
    fi
    
    if [[ "$arg" == "--stimela-pull-test" ]] || [[ "$arg" == "-spt" ]]
    then
	SPT=1
    fi
    
    if [[ "$arg" == "--singularity-cachedir" ]] || [[ "$arg" == "-scd" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --singularity-cachedir or -scd switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_SINGULARITY_CACHEDIR=${!nextcount}
    fi

    if [[ "$arg" == "--singularity-localcachedir" ]] || [[ "$arg" == "-sld" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --singularity-localcachedir or -sld switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_SINGULARITY_LOCALCACHEDIR=${!nextcount}
    fi

    if [[ "$arg" == "--singularity-tmpdir" ]] || [[ "$arg" == "-std" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --singularity-tmpdir or -std switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_SINGULARITY_TMPDIR=${!nextcount}
    fi

    if [[ "$arg" == "--singularity-nocache" ]] || [[ "$arg" == "-snc" ]]
    then
	SNC=1
    fi

    if [[ "$arg" == "--stimela-pullfolder" ]] || [[ "$arg" == "-spf" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --stimela-pullfolder or -spf switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_STIMELA_PULLFOLDER=${!nextcount}
    fi

    if [[ "$arg" == "--stimela-delete-pullfolder" ]] || [[ "$arg" == "-sdp" ]]
    then
	SKP=1
    fi
    
    if [[ "$arg" == "--install-attempts" ]] || [[ "$arg" == "-ia" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --install-attempt or -ia switch, stopping."; kill "$PPID"; exit 1; }
        IA=${!nextcount}
    fi

    if [[ "$arg" == "--container-all-workspace" ]] || [[ "$arg" == "-caw" ]]
    then
	SCW=1
	SLW=1
	STW=1
	SPW=1
    fi
    
    if [[ "$arg" == "--container-all-test" ]] || [[ "$arg" == "-cat" ]]
    then
	SCH=1
	SLH=1
	STH=1
	SPT=1
    fi

    if [[ "$arg" == "--singularity-all-workspace" ]] || [[ "$arg" == "-saw" ]]
    then
	SCW=1
	SLW=1
	STW=1
    fi
    
    if [[ "$arg" == "--singularity-all-home" ]] || [[ "$arg" == "-sah" ]]
    then
	SCH=1
	SLH=1
	STH=1
    fi

    ####
    ####
    ####   
    if [[ "$arg" == "--use-stimela-stable" ]] || [[ "$arg" == "-us" ]]
    then
        US=1
    fi
    if [[ "$arg" == "--use-stimela-master" ]] || [[ "$arg" == "-um" ]]
    then
	[[ -z ${US} ]] ||  { echo "You can use only one of -us (--use-stimela-stable) or -um (--use-stimela-master), stopping."; kill "$PPID"; exit 1; }
        UM=1
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
    if [[ "$arg" == "--stimela-pull-force" ]] || [[ "$arg" == "-pf" ]]
    then
        PF=1
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
    if [[ "$arg" == "--keep-report-dir" ]] || [[ "$arg" == "-kr" ]]
    then
        KR=1
    fi
    if [[ "$arg" == "--keep-processing-dir" ]] || [[ "$arg" == "-kp" ]]
    then
        KP=1
    fi
    if [[ "$arg" == "--no-pull" ]] || [[ "$arg" == "-np" ]]
    then
        NP=1
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
    if [[ "$arg" == "--copy-config-data-id" ]] || [[ "$arg" == "-cc" ]]
    then
        OD=1
    fi
    if [[ "$arg" == "--copy-data-id" ]] || [[ "$arg" == "-ci" ]]
    then
	(( nextcount=argcount+1 ))
	(( $nextcount <= $# )) || { echo "Argument expected for --copy-data-id or -ci switch, stopping."; kill "$PPID"; exit 1; }
	CARATE_COPY_DATA_ID=${!nextcount}
    fi
    if [[ "$arg" == "--move-test-data" ]] || [[ "$arg" == "-md" ]]
    then
        MD=1
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
    if [[ "$arg" == "--caracal-build-id" ]] || [[ "$arg" == "-cb" ]]
    then
 (( nextcount=argcount+1 ))
 (( $nextcount <= $# )) || { echo "Argument expected for --caracal-build-id or -cb switch, stopping."; kill "$PPID"; exit 1; }
 CARATE_CARACAL_BUILD_ID=${!nextcount}
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
    if [[ "$arg" == "--caracal-release" ]] || [[ "$arg" == "-cr" ]]
    then
        CR=1
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
	# This is a name
	# 	firstletter=`echo ${CARATE_CARACAL_RUN_PREFIX} | head -c 1`
	#	[[ ${firstletter} == "/" ]] || CARATE_CARACAL_RUN_PREFIX="${cwd}/${CARATE_CARACAL_RUN_PREFIX}"
    fi
    if [[ "$arg" == "--caracal-former-run" ]] || [[ "$arg" == "-cf" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --caracal-former-run or -rf switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_CARACAL_FORMER_RUN=${!nextcount}
	# Same as before, this is a name
# 	firstletter=`echo ${CARATE_CARACAL_FORMER_RUN} | head -c 1`
#	[[ ${firstletter} == "/" ]] || CARATE_CARACAL_FORMER_RUN="${cwd}/${CARATE_CARACAL_FORMER_RUN}"
    fi
    if [[ "$arg" == "--caracal-run-medifix" ]] || [[ "$arg" == "-rm" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --caracal-run-medifix or -rm switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_CARACAL_RUN_MEDIFIX=${!nextcount}
    fi
    if [[ "$arg" == "--local-caracal" ]] || [[ "$arg" == "-lc" ]]
    then
        (( nextcount=argcount+1 ))
        (( $nextcount <= $# )) || { echo "Argument expected for --local-caracal or -lc switch, stopping."; kill "$PPID"; exit 1; }
        CARATE_LOCAL_CARACAL=${!nextcount}
 	firstletter=`echo ${CARATE_LOCAL_CARACAL} | head -c 1`
	[[ ${firstletter} == "/" ]] || CARATE_LOCAL_CARACAL="${cwd}/${CARATE_LOCAL_CARACAL}"
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

    # Convenience switches
    if [[ "$arg" == "--caracal-datared" ]] || [[ "$arg" == "-cd" ]]
    then
	CD=1
	OC=1
	OF=1
	ORSR=1
	KR=1
	OR=1
	OD=1
	KP=1
	FORCE=1
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
    echo "caratekit.sh"
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

    echo "  CARATE_VIRTUALENV:           Location of the virtualenv directory"
    echo "                               (optional)"
    echo ""

    echo "  CARATE_LOCAL_CARACAL:         Local CARACal copy to use. If not"
    echo "                               set, CARACal will be downloaded"
    echo "                               from https://github.com/caracal-pipeline/caracal"
    echo ""

    echo "  CARATE_CARACAL_BUILD_ID: Build number to test. If not set, master"
    echo "                               will be tested"
    echo ""

    echo "  CARATE_LOCAL_STIMELA:        Location of a local stimela"
    echo ""

    echo "  CARATE_HOME_FOLDER:          Location of \$HOME during test"

    echo "  CARATE_SINGULARITY_CACHE:      Location of \$SINGULARITY_CACHEDIR"
    echo "  CARATE_SINGULARITY_LOCALCACHE: Location of \$SINGULARITY_LOCALCACHEDIR"
    echo "  CARATE_SINGULARITY_TMPDIR:     Location of \$SINGULARITY_TMPDIR"
    echo "  CARATE_STIMELA_PULLFOLDER:     Location of \$STIMELA_PULLFOLDER"
    echo ""

    echo "  CARATE_CARACAL_TEST_ID:      Only specify if CARATE_CARACAL_BUILD_ID"
    echo "                               is undefined. All data and installations for"
    echo "                               a specific test will be saved in the directory"
    echo "                               \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID."
    echo "                               CARATE_CARACAL_TEST_ID is changed to"
    echo "                               CARATE_CARACAL_BUILD_ID"
    echo "                               if CARATE_CARACAL_BUILD_ID is defined."
    echo ""

    echo "  CARATE_CARACAL_RUN_PREFIX:   The name prefix of an individual carate run. Will"
    echo "                               be automatically generated if not supplied."
    echo ""

    echo "  CARATE_CARACAL_FORMER_RUN:   Name of a previous run prefix. If set, the test"
    echo "                               directory of this run will be renamed into"
    echo "                               \$CARATE_CARACAL_FORMER_RUN_suffix prior to running"
    echo "                               the test and a soft link with the name of the former" 
    echo "                               directory will be created, pointing to the current"
    echo "                               directory."
    echo ""

    echo "  CARATE_CARACAL_RUN_MEDIFIX:  The additional identifyer for logfiles of an"
    echo "                               individual carate run. Empty if not supplied."
    echo ""

    echo "  CARATE_COPY_MS:              Names of the measurement sets to be copied"
    echo "                               (comma-separated list without .ms extensions)"
    echo ""

    echo "  CARATE_TEST_DATA_DIR:        Directory containing test data (ms"
    echo "                               format)"
    echo ""

    echo "  CARATE_INPUT_DIR:            Directory containing input data"
    echo "                               (optional)"
    echo ""

    echo "  CARATE_CONFIG_SOURCE:        Local configuration file copy to use for an"
    echo "                               additional test"
    echo ""

    echo "Switches:"
    echo ""
    echo "  --help -h                           Show help"
    echo ""
    echo "  --verbose-help -v                   Show verbose help"
    echo ""
    echo "  --workspace ARG -ws ARG             Use ARG instead of environment variable"
    echo "                                      CARATE_WORKSPACE"
    echo ""
    echo "  --home-original -ho                 Do not change the HOME environment"
    echo "                                      variable during installation test"
    echo ""
    echo "  --home-workspace -hw                Use \$CARATE_WORKSPACE/home"
    echo "                                      as home folder during caratekit test"
    echo ""
    echo "  --home-folder ARG -hf ARG           Use ARG as home folder during caratekit"
    echo "                                      test"
    echo ""
    echo "  --home-no-delete -hn                Do not delete the home folder (automati-"
    echo "                                      cally applied if --home-original or"
    echo "                                      -ho are set)"
    echo ""
    echo "  --install-attempts -ia              Allowed number of attempts to pull images,"
    echo "                                      to re-invoke pip, to run stimela pull,"
    echo "                                      etc."
    echo ""
    echo "  --virtualenv ARG -ve ARG            Use ARG instead of internal virtualenv"
    echo "                                      variable CARATE_VIRTUALENV"
    echo ""
    echo "  --omit-venv-reinstall -ov           Do not re-install the virtual environment"
    echo ""
    echo "  --caracal-release -cr               Install latest CARACal release version"
    echo "                                      Default is to install current master"
    echo ""
    echo "  --local-caracal ARG -lc ARG         Use ARG instead of environment variable"
    echo "                                      CARATE_LOCAL_CARACAL"
    echo ""
    echo "  --omit-caracal-reinstall -oc        Do not pip install caracal"
    echo ""
    echo "  --omit-caracal-fetch -of            Do not fetch or copy caracal directory"
    echo "                                      if present"
    echo ""
    echo "  --caracal-build-id ARG -cb ARG      Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_BUILD_ID"
    echo ""
    echo "  --use-stimela-master -um            Use"
    echo "                                      pip install -U --force-reinstall -r"
    echo "                                                  (...)stimela-master.txt"
    echo "                                      when installing CARACal"
    echo ""
    echo "  --use-stimela-stable -us            Use"
    echo "                                      pip install -U --force-reinstall -r"
    echo "                                                  (...)stimela-last_stable.txt"
    echo "                                      when installing CARACal"
    echo ""
    echo "  --local-stimela ARG -lst ARG        Use pip install -U --force-reinstall ARG"
    echo "                                      to install a local stimela (or whatever"
    echo "                                      is in ARG) when installing CARACal"
    echo ""
    echo "  --omit-stimela-reinstall -os        Do not re-install stimela"
    echo ""
    echo "  --stimela-pull-force -pf            Use -f switch when invoking stimela pull"
    echo ""
    echo "  --docker-installation -di           Test Docker installation/install Docker"
    echo "                                      Stimela (no installation if -os is set)"
    echo ""
    echo "  --no-pull -np                       Do not run stimela pull prior to running"
    echo "                                      CARACal"
    echo ""
    echo "  --omit-docker-prune -op             Do not prune system during docker install"
    echo ""
    echo "  --singularity-installation -si      Test Singularity installation/install"
    echo "                                      Singularity Stimela"
    echo ""
    echo "  --singularity-cache-workspace -scw  Use \$CARATE_WORKSPACE/.singularity/cache"
    echo "                                      as \${SINGULARITY_CACHEDIR}"
    echo ""
    echo "  --singularity-localcache-workspace -slw Use"
    echo "                                          \$CARATE_WORKSPACE/.singularity/localcache"
    echo "                                          as \${SINGULARITY_LOCALCACHEDIR}"
    echo ""
    echo "  --singularity-tmp-workspace -stw    Use \$CARATE_WORKSPACE/.singularity/tmp"
    echo "                                      as \${SINGULARITY_TMPDIR}"
    echo ""
    echo "  --singularity-pull-workspace -spw   Use \$CARATE_WORKSPACE/stimela_pullfolder"
    echo "                                      as \${STIMELA_PULLFOLDER}"
    echo ""
    echo "  --singularity-cache-home -sch       Use \${home}/.singularity/cache"
    echo "                                      as \${SINGULARITY_CACHEDIR}"
    echo "                                      \$home=your_test_directory/.home"
    echo ""
    echo "  --singularity-localcache-home -slh  Use \${home}/.singularity/localcache"
    echo "                                      as \${SINGULARITY_LOCALCACHEDIR}"
    echo "                                      \$home=your_test_directory/.home"
    echo ""
    echo "  --singularity-tmp-home -sth         Use \${home}/.singularity/tmp"
    echo "                                      as \${SINGULARITY_TMPDIR}"
    echo "                                      \$home=your_test_directory/.home"
    echo ""
    echo "  --singularity-nocache -snc          Set \${SINGULARITY_DISABLE_CACHE}"
    echo "                                      to 'True' (otherwise unset)"
    echo ""
    echo "  --stimela-pull-test -spt            Use your_test_directory/stimela_pullfolder"
    echo "                                      as \${STIMELA_PULLFOLDER}"
    echo ""
    echo "  --singularity-cachedir ARG -scd ARG Use ARG instead of environment variable"
    echo "                                      CARATE_SINGULARITY_CACHEDIR"
    echo "                                      Location of \$SINGULARITY_CACHEDIR"
    echo ""
    echo "  --singularity-localcachedir ARG -sld ARG   Use ARG instead of environment"
    echo "                                             variable"
    echo "                                             CARATE_SINGULARITY_LOCALCACHEDIR"
    echo "                                             Location of"
    echo "                                             \$SINGULARITY_LOCALCACHEDIR"
    echo ""
    echo "  --singularity-tmpdir ARG -std ARG   Use ARG instead of environment variable"
    echo "                                      CARATE_SINGULARITY_TMPDIR"
    echo "                                      Location of \$SINGULARITY_TMPDIR"
    echo ""
    echo "  --stimela-pullfolder ARG -spf ARG   Use ARG instead of environment variable"
    echo "                                      CARATE_STIMELA_PULLFOLDER"
    echo "                                      Location of \$STIMELA_PULLFOLDER"
    echo ""
    echo "  --stimela-delete-pullfolder -sdp    Delete the content of"
    echo "                                      \$STIMELA_PULLFOLDER"
    echo "                                      (and reinstall)"
    echo ""
    echo "  --singularity-all-home -sah         Alias for -sch -slh -sth"
    echo ""
    echo "  --singularity-all-workspace -saw    Alias for -scw -slw -stw"
    echo ""
    echo "  --container-all-test -cat           Alias for -sch -slh -sth -spt"
    echo ""
    echo "  --container-all-workspace -caw      Alias for -scw -slw -stw -spw"
    echo ""
    echo "  --caracal-test-id ARG -ct ARG       Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_TEST_ID"
    echo ""
    echo "  --caracal-run-prefix ARG -rp ARG    Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_RUN_PREFIX"
    echo ""
    echo "  --caracal-former-run ARG -cf ARG    Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_FORMER_RUN"
    echo ""
    echo "  --caracal-run-medifix ARG -rp ARG   Use ARG instead of environment variable"
    echo "                                      CARATE_CARACAL_RUN_MEDIFIX"
    echo ""
    echo "  --keep-report-dir -kr               Do not delete the report directory if it"
    echo "                                      exists"
    echo ""
    echo "  --keep-report-dir -kp               Do not delete any of the content of a"
    echo "                                      project directory (input, msdir, output,"
    echo "                                      stimela_parameter_files) prior to"
    echo "                                      starting a test"
    echo ""
    echo "  --config-source ARG -cs ARG         Use ARG instead of environment variable"
    echo "                                      CARATE_CONFIG_SOURCE"
    echo ""
    echo "  --keep-config-source -kc            Do not autodetect measurement sets but"
    echo "                                      keep the confic source as it is. Only"
    echo "                                      if --config-source is supplied."
    echo ""
    echo "  --test-data-dir ARG -td ARG         Use ARG instead of environment variable"
    echo "                                      CARATE_TEST_DATA_DIR"
    echo ""
    echo "  --omit-copy-test-data -od           Do not re-copy test data and preserve msdir"
    echo ""
    echo "  --move-test-data -md                Move test data instead of creating a copy"
    echo ""
    echo "  --copy-config-data -cc              Copy test data as specified in the config-"
    echo "                                      file parameter dataid unless"
    echo "                                      CARATE_COPY_DATA_ID is defined"
    echo ""
    echo "  --copy-data-id ARG -ci ARG          Use ARG instead of environment variable"
    echo "                                      CARATE_COPY_DATA_ID"
    echo ""
    echo "  --copy-config-data -cc              Copy test data as specified in the"
    echo "                                      parameter dataid from CARATE_CONFIG_SOURCE"
    echo "                                      unless CARATE_COPY_DATA_ID is defined."
    echo "                                      Ignored if CARATE_CONFIG_SOURCE is not"
    echo "                                      defined and switches --config-source -cs"
    echo "                                      are not used"
    echo ""
    echo "  --copy-data-id ARG -ci ARG          Use ARG instead of environment variable"
    echo "                                      CARATE_COPY_DATA_ID. Note that this over-"
    echo "                                      rides --copy-config-data or -cc"
    echo ""
    echo "  --input-dir ARG -id ARG             Use ARG instead of environment variable"
    echo "                                      CARATE_INPUT_DIR"
    echo ""
    echo "  --docker-minimal -dm                Test Docker installation and test run with"
    echo "                                      minimal test configuration"
    echo ""
    echo "  --docker-alternative -da            Test Docker installation and test run with"
    echo "                                      alternative configuration carateConfig.yml"
    echo ""
    echo "  --singularity-minimal -sm           Test Singularity installation and test run"
    echo "                                      with minimal test configuration"
    echo ""
    echo "  --singularity-alternative -sa       Test Singularity installation and test run"
    echo "                                      alternative configuration carateConfig.yml"
    echo ""
    echo "  --docker-sample-configs -dsc         Check all sample configurations to pass"
    echo "                                       observation config with docker."
    echo ""
    echo "  --singularity-sample-configs -ssc    Check all sample configurations to pass"
    echo "                                       observation config with singiularity."
    echo ""
    echo "  --override -or                      Override security question (showing root"
    echo "                                      directory and asking whether to proceed.)"
    echo ""
    echo "  --fastsim -fs                       Omit all time-consuming steps"
    echo ""
    echo "  --force -f                          Force replacement and re-installation of"
    echo "                                      all components if possible (see below)"
    echo ""
    echo "  --caracal-datared -cd               Short for -oc -of -os -kr -or -kp -od -f"
    echo ""
    echo ""
fi

if [[ -n "$VE" ]]
then
    echo ""
echo " The script creates a root directory (Notice that all environment"
echo " variables can also be supplied via the command line)"
echo " \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID, where CARATE_WORKSPACE"
echo " is an environment variable containing the path of a parent directory"
echo " to all tests done with this script. The variable"
echo " CARATE_CARACAL_TEST_ID is identical to the environment variable"
echo " CARATE_CARACAL_BUILD_ID if that is set by the user, and has to be"
echo " supplied independently (i.e. to be defined prior to the script call"
echo " or supplied using switches --caracal-test-id or -ct) as an"
echo " environment variable if CARATE_CARACAL_BUILD_ID is not defined. The"
echo " rationale behind that is that the test directory is always linked to"
echo " a git(hub) build number if that exists. Otherwise, if"
echo " CARATE_CARACAL_BUILD_ID is not defined, the user can supply an"
echo " alternative name \$CARATE_CARACAL_TEST_ID. In the test root directory"
echo " \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID, by default a home directory"
echo "  called home, a virtual environment called caracal_virtualenv, a CARACal"
echo " copy caracal, and up to six test directories are created, within which"
echo " the tests are conducted. If the --force or -f switch is set, existing"
echo " directories and installations are deleted and replaced, if not, only"
echo " those directories and files are created, which do not exist"
echo " yet. Exceptions from that rule exist. If the --omit-stimela-reinstall"
echo " or -os switch is set, a re-installation of stimela is preventedeven"
echo " if -f is set. This includes the Re-installation of the virtual"
echo " environment, the home directory, and the file .stimela in the home"
echo " directory. "
echo " The location of the home directory can be changed using environment variables"
echo " and switches. The environment variable CARATE_HOME_FOLDER defines directly the"
echo " home folder. It can be overridden by the switches --home-folder ARG or -hf ARG,"
echo " where ARG is the location of the home folder. Switch -hw defines the home"
echo " folder to be the directory home in the workspace folder. Switch --home-original"
echo " or -ho leaves the home folder as it is. The default, as said above, is to"
echo " define the home folder as \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID/home"
echo " If the option --home-no-delete or -hn is set, the home folder"
echo " is preserved. -hn is automatically set if switches --home-original or -ho are set."
echo ""
echo " If"
echo " the option --keep-report or -kr is set, the directory"
echo " \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID/report is pre-"
echo " served. Switches --omit-virtualenv-reinstall and -ov prevent the"
echo " deletion of the virtualenv, --omit-caracal-reinstall will prevent the"
echo " deletion (and re- installation of CARACal. If option"
echo " --fast-simulation (-fs) is set, only the directory"
echo " \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID/report is deleted if"
echo " option -f is set. No directories or files supplied to caratekit.sh"
echo " (including their parent directories) are deleted."
echo ""
echo "  In detail (all installations in the root directory"
echo "  \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID):"
echo ""
echo "  - a directory called home is created (if not existing or if -f is"
echo "    set) and the HOME environment variable set to that home directory"
echo "    unless the --keep-home or -kh switches are set."
echo ""
echo "  - a python 3 virtual environment name caracal_venv is created (if"
echo "    not existing or if -f is set) and activated. It is not re-created"
echo "    if switches --omit-stimela-reinstall (-os) or"
echo "    --omit-virtualenv-reinstall (-ov) are set."
echo ""
echo "  - caracal is either downloaded to the root directory (if not"
echo "    existing or if -f is set) from https://github.com/caracal-pipeline/caracal"
echo "    or, if the CARATE_LOCAL_CARACAL environment variable is set,"
echo "    \$CARATE_LOCAL_CARACAL is copied to the root directory if not"
echo "    existing or if -f is set (notice that the directory tree should be"
echo "    a valid caracal tree, ready for installation). If switches"
echo "    --omit-caracal-fetch or -of are set, the sources are also not"
echo "    copied across."
echo ""
echo "  - the caracal version CARATE_CARACAL_BUILD_ID is checked out using"
echo "    git if CARATE_CARACAL_BUILD_ID is defined. If switches"
echo "    --omit-caracal-reinstall or -oc are set, this is not done"
echo ""
echo "  - the latest CARACal release version is checked out and installed if"
echo "    switches --caracal-release or -cr are set"
echo ""
echo "  - caracal is installed via pip. If switches --omit-caracal-reinstall"
echo "    or -oc are set, this is not done"
echo ""
echo "  - if --use-stimela-master or -um switch is set,"
echo "    caracal/stimela-master.txt is installed via pip. If switches"
echo "    --omit-stimela-reinstall or -os are set, this is not done"
echo ""
echo "  - if --use-stimela-stable or -us switch is set,"
echo "    caracal/stimela-last_stable.txt is installed via pip. If switches"
echo "    --omit-stimela-reinstall or -os are set, this is not done"
echo ""
echo "  - when switches --docker-minimal, -dm, --docker-alternative, -da,"
echo "    --docker-installation, -di are set, home/.stimela is removed, docker"
echo "    system prune is invoked, and docker stimela is installed (stimela"
echo "    build). If switches --omit-stimela-reinstall or -os are set, this is"
echo "    not done"
echo ""
echo "  - when switches --no-pull, -np are set, stimela pull is"
echo "    not invoked for a Stimela installation, otherwise a stimela pull"
echo "    is executed prior to running CARACal. If switches"
echo "    --omit-stimela-reinstall or -os are set, stimela pull is not"
echo "    invoked either."
echo ""
echo "  - There are four environment variables connected to the use of"
echo "    Singularity, which can be temporarily be replaced while "
echo "    using caratekit.sh. After running caratekit.sh"
echo "    they will be changed back to their original values."
echo "    \$SINGULARITY_CACHEDIR, \$SINGULARITY_LOCALCACHEDIR and"
echo "    \$SINGULARITY_TMPDIR are environment"
echo "    variables recognized by Singularity, while"
echo "    \$STIMELA_PULLFOLDER is the directory used by Stimela to store"
echo "    Singularity images in. By default the values of these variables are"
echo "    not changed, implying that if they are unset, they are left unset."
echo "    If switch --singularity-cache-workspace or -scw is set,"
echo "    \$CARATE_WORKSPACE/.singularity/cache is used"
echo "    as \${SINGULARITY_CACHEDIR}"
echo "    If switch --singularity-localcache-workspace or -slw is set,"
echo "    \$CARATE_WORKSPACE/.singularity/localcache is used"
echo "    as \${SINGULARITY_LOCALCACHEDIR}"
echo "    If switch --singularity-tmp-workspace or -stw is set,"
echo "    \$CARATE_WORKSPACE/.singularity/tmp is used"
echo "    as \${SINGULARITY_TMPDIR}"
echo "    If switch --stimela-pull-workspace or -spw is set,"
echo "    \$CARATE_WORKSPACE/stimela_pullfolder is used"
echo "    as \${STIMELA_PULLFOLDER}."
echo "    If switch --singularity-cache-home or -sch is set,"
echo "    \${home}/.singularity/cache is used as \${SINGULARITY_CACHEDIR},"
echo "    where \$home denotes the home folder in the test directory."
echo "    If switch --singularity-localcache-home or -slh is set,"
echo "    \${home}/.singularity/localcache is used as \${SINGULARITY_LOCALCACHEDIR},"
echo "    where \$home denotes the home folder in the test directory."
echo "    If switch --singularity-tmp-home or -sth is set,"
echo "    \${home}/.singularity/tmp is used as \${SINGULARITY_TMPDIR},"
echo "    where \$home denotes the home folder in the test directory."
echo "    If switch --stimela-pull-test -spt is set,"
echo "    your_test_directory//stimela_pullfolder is used as"
echo "    \${STIMELA_PULLFOLDER},"
echo "    where your_test_directory denotes the test directory."
echo "    The environment variable CARATE_SINGULARITY_CACHEDIR or the"
echo "    argument ARG of switches --singularity-cachedir ARG or -scd ARG"
echo "    are used to directly define \$SINGULARITY_CACHEDIR."
echo "    The environment variable CARATE_SINGULARITY_LOCALCACHEDIR or the"
echo "    argument ARG of switches --singularity-localcachedir ARG or -sld ARG"
echo "    are used to directly define \$SINGULARITY_LOCALCACHEDIR."
echo "    The environment variable CARATE_SINGULARITY_TMPDIR or the"
echo "    argument ARG of switches --singularity-tmpdir ARG or -std ARG"
echo "    are used to directly define \$SINGULARITY_TMPDIR."
echo "    The environment variable CARATE_STIMELA_PULLFOLDER or the"
echo "    argument ARG of switches --stimela-pullfolder ARG or -spf ARG"
echo "    are used to directly define \$STIMELA_PULLFOLDER."
echo "    The definition of the environment variables using the switches"
echo "    above is mutually exclusive."
echo "    If \${STIMELA_PULLFOLDER} is not defined by any switch and it"
echo "    is not defined prior to invoking caratekit.sh, it defaults to"
echo "    STIMELA_PULLFOLDER=\$CARATE_WORKSPACE/stimela_pullfolder"
echo "    If switches --singularity-nocache or -snc are set, the"
echo "    environment variable \${SINGULARITY_DISABLE_CACHE} is set to"
echo "    True for the time of the caratekit run (disabling caching)"
echo "    and then reset to its original value (or unset). If"
echo "    --singularity-nocache or -snc are not set, the environment variable"
echo "    \${SINGULARITY_DISABLE_CACHE} is unset for the duration of the"
echo "    caratekit run."
echo ""
echo "  - when switches --singularity-minimal, -sm,"
echo "    --singularity-alternative, -se, --singularity-installation, -si"
echo "    are set, home/.stimela is removed. If switches"
echo "    --omit-stimela-reinstall or -os are set, this is not done"
echo ""
echo "  - by default, the content of \$CARATE_STIMELA_PULLFOLDER is"
echo "    not deleted. This can be prevented using the switch"
echo "    --stimela-delete-pullfolder or -sdp" 
echo ""
echo "  - when switch --singularity-minimal or -sm is set, a directory"
echo "    minimal_singularity is created (if not existing or if -f is set),"
echo "    the configuration file"
echo "    caracal/caracal/sample_configurations/minitestConfig.yml is copied"
echo "    to that directory, all .ms files from \$CARATE_TEST_DATA_DIR are"
echo "    copied into the msdir directory in the minimal_singularity"
echo "    directory and minitestConfig.yml is edited to point to those .ms"
echo "    files in the variable dataid, then caracal is run with"
echo "    minitestConfig.yml and declared successful if certain expected"
echo "    files are created (see exceptions below)."
echo ""
echo "  - when switch --singularity_extended or -se is set, a directory"
echo "    extended_singularity is created (if not existing or if -f is"
echo "    set), the configuration file"
echo "    caracal/caracal/sample_configurations/extendedConfig.yml is"
echo "    copied to that directory, all .ms files from \$CARATE_TEST_DATA_DIR"
echo "    are copied into the msdir directory in the extended_singularity"
echo "    directory and extendedConfig.yml is edited to point to those .ms"
echo "    files in the variable dataid, then caracal is run with"
echo "    extendedConfig.yml and declared successful if certain expected files"
echo "    are created (see exceptions using variable CARATE_COPY_DATA_ID or"
echo "     switches --keep-config-source, -ks, --copy-config-data -cc,"
echo "     --copy-data-id, -ci below)."
echo ""
echo "  - when switch --docker-minimal or -dm is set, a directory \-"
echo "    minimal_docker is created (if not existing or if -f is set), the"
echo "    configuration file"
echo "    caracal/caracal/sample_configurations/minitestConfig.yml is copied"
echo "    to that directory, all .ms files from \$CARATE_TEST_DATA_DIR are"
echo "    copied into the msdir directory in the minimal_docker directory"
echo "    and minitestConfig.yml is edited to point to those .ms files in the"
echo "    variable dataid, then caracal is run with minitestConfig.yml and"
echo "    declared successful if certain expected files are created (see"
echo "    exceptions using variable CARATE_COPY_DATA_ID or switches"
echo "    --keep-config-source, -ks, --copy-config-data -cc, --copy-data-id"
echo "    -ci below)."
echo ""
echo "  - when switch --docker_extended or -da is set, a directory"
echo "    extended_docker is created (if not existing or if -f is set), the"
echo "    configuration file"
echo "    caracal/caracal/sample_configurations/extendedConfig.yml is copied"
echo "    to that directory, all .ms files from \$CARATE_TEST_DATA_DIR are"
echo "    copied into the msdir directory in the extended_docker directory"
echo "    and extendedConfig.yml is edited to point to those .ms files in"
echo "    the variable dataid, then caracal is run with extendedConfig.yml"
echo "    and declared successful if certain expected files are created (see"
echo "    exceptions using variable CARATE_COPY_DATA_ID or switches"
echo "    --keep-config-source, -ks, --copy-config-data -cc, --copy-data-id"
echo "    -ci below)."
echo ""
echo "  - when environment variable CARATE_CONFIG_SOURCE is set in"
echo "    combination with switches --singularity-installation or -si set,"
echo "    then that yaml configuration source is used for a further"
echo "    singularity test in the directory prefix_singularity, where prefix"
echo "    is the prefix of the yaml file. The line dataid: ['...','...'] in"
echo "    that file is replaced by the appropriate line to process the test"
echo "    data sets in \$CARATE_TEST_DATA_DIR unless expressively prevented or"
echo "    changed using variable CARATE_COPY_DATA_ID or switches"
echo "    --keep-config-source, -ks, --copy-config-data -cc, --copy-data-id"
echo "    -ci . See below for those exceptions."
echo ""
echo "  - when environment variable CARATE_CONFIG_SOURCE is set in"
echo "    combination with switches --docker-installation or -di set, then"
echo "    that yaml configuration source is used for a further singularity"
echo "    test in the directory prefix_singularity, where prefix is the"
echo "    prefix of the yaml file. The line dataid: ['...','...'] in that"
echo "    file is replaced by the appropriate line to process the test data"
echo "    sets in \$CARATE_TEST_DATA_DIR unless expressively prevented or"
echo "    changed using variable CARATE_COPY_DATA_ID or switches"
echo "    --keep-config-source, -ks, --copy-config-data -cc, --copy-data-id"
echo "    -ci . See below for those exceptions."
echo ""
echo "  - when environment variable CARATE_INPUT_DIR is set the contents of"
echo "    that direcory will be copied into the input directory of CARACal"
echo "    prior to starting the test"
echo ""
echo "  - when environment variable CARATE_VIRTUALENV is set the that"
echo "    direcory is used as the location of the virtual environment"
echo ""
echo "  - when switch omit-venv-reinstall or -ov is set, the virtual"
echo "    environment will not be re-installed if already present."
echo ""
echo "  - when environment variable CARATE_LOCAL_STIMELA is set the that"
echo "    direcory is used as the Stimela location. Notice that all environ-"
echo "    ment variables can be re-defined by using appropriate switches,"
echo "    in this case --local-stimela or -lst." 
echo ""
echo "  - when environment variable CARATE_COPY_DATA_ID is set, it contains"
echo "    a comma- or space-separated list of dataids (Measurement Set names"
echo "    without the .ms suffixes). Instead of using all data sets in the"
echo "    test data directory, only data sets with that ID are used and the"
echo "    configuration file adjusted accordingly. Alternatively to setting"
echo "    the environment variable, it is possible to use switches"
echo "    --copy-config-data or -cc."
echo ""
echo "  - when switches --copy-config-data or -cc are set, the dataids"
echo "    (Measurement Set names without the .ms suffixes) are read from"
echo "    the configuration file parameter dataid unless it is not empty."
echo "    Dataids set in CARATE_COPY_DATA_ID or by using switches"
echo "    --copy-config-data or -cc get preference and --copy-config-data"
echo "    or -cc is ignored if they are set."
echo ""
echo "  - when switches --omit-copy-test-data -od are set, the test data"
echo "    will not be copied if for a test run the directory with the test"
echo "    data does already exist, the directory will not be changed before"
echo "    the data reduction starts."
echo ""
echo "  - when switches --move-test-data or -md are set, the test data will"
echo "    not be copied but moved. This is only possible if one test run is"
echo "    performed"
echo ""
echo "  - when environment variable CARATE_CARACAL_RUN_PREFIX is set (or"
echo "    defined via --caracal-run-prefix or -rp switches) tests will be"
echo "    named using that variable as a prefix and a suffix indicating the"
echo "    number of the test."
echo ""
echo "  - when environment variable CARATE_CARACAL_FORMER_RUN is set (or"
echo "    defined via --caracal-former-run or -cf switches) a former test"
echo "    directory with the name \${CARATE_CARACAL_FORMER_RUN}_suffix will"
echo "    be re-named into \${CARATE_CARACAL_RUN_PREFIX}_suffix if"
echo "    existent. suffix is the run number.  variable as a prefix and a"
echo "    suffix indicating the number of the test.  Consecutively, the"
echo "    directories msdir, input, output, and stimela_parameter_files and"
echo "    their contents in \${CARATE_CARACAL_RUN_PREFIX}_suffix will not be"
echo "    deleted. A symbolic link with the name"
echo "    \${CARATE_CARACAL_FORMER_RUN}_suffix pointing to"
echo "    \${CARATE_CARACAL_RUN_PREFIX}_suffix will be created in addition."
echo ""
echo " For each test run, log-caracal.txt is searched for keywords"
echo " indicating the start and the end of a worker and those numbers are"
echo " reported.  The test is declared failed and caratekit.sh returns 1 if:"
echo " "
echo "   - No logfiles are produced before CARACal finishes"
echo "   "
echo "   - log-caracal.txt does not contain any keyword indicating that a"
echo "     worker has started"
echo ""
echo "   - The number of keywords in log-caracal.txt indicating the start of"
echo "     a worker differs from the number of keywords in log-caracal.txt"
echo "     indicating the end of a worker."
echo "     "
echo "   - If the exit status of CARACal is not 0 (success)"
echo ""
echo " caratekit will create or add files to a report directory"
echo " \$CARATE_WORKSPACE/\$CARATE_CARACAL_TEST_ID/report containing report"
echo " files:"
echo " "
echo "   - a shell script reproducing all shell commands initiated by carate.sh, if"
echo "     CARATE_CARACAL_RUN_PREFIX is set (as an environment variable or through"
echo "     switches --caracal-run-prefix or -cp) called"
echo "     \${CARATE_CARACAL_RUN_PREFIX}-${CARATE_CARACAL_RUN_MEDIFIX}.sh.txt, called"
echo "     \${CARATE_CARACAL_TEST_ID}-${CARATE_CARACAL_RUN_MEDIFIX}.sh.txt otherwise."
echo "     If ${CARATE_CARACAL_RUN_MEDIFIX} is not defined, -${CARATE_CARACAL_RUN_MEDIFIX}"
echo "     is left out."
echo "     "
echo "   - a file with information about the computer and the environment"
echo "     that was used for the test, if CARATE_CARACAL_RUN_PREFIX is set"
echo "     (as an environment variable or through switches"
echo "     --caracal-run-prefix or -cp) called"
echo "     \${CARATE_CARACAL_RUN_PREFIX}-${CARATE_CARACAL_RUN_MEDIFIX}-sysinfo.txt, called"
echo "     \${CARATE_CARACAL_TEST_ID}-${CARATE_CARACAL_RUN_MEDIFIX}-sysinfo.txt"
echo "     otherwise"
echo "     If ${CARATE_CARACAL_RUN_MEDIFIX} is not defined, -${CARATE_CARACAL_RUN_MEDIFIX}"
echo "     is left out."
echo "     "
echo "   - a file ${CARATE_CARACAL_TEST_ID}-${CARATE_CARACAL_RUN_MEDIFIX}-sysinfo.txt with information"
echo "     about the computer and the environment that was used for the test"
echo "     "
echo "   - copies of the configuration files, one per test"
echo "   "
echo "   - copies of the output log-caracal.txt, one per test"
echo "   "
echo " Note that in particular Stimela has components that are external to"
echo " the root directory and will be touched by this test."
echo ""
echo " Example 1:"
echo " "
echo " Testing a pull request with commit/build number 6d562c... using"
echo " Docker and installing using requirements.txt. The user is permanently"
echo " testing CARACal and has therefore a standard directory with test"
echo " files and a standard test location. Someone issues a pull"
echo " request. The user looks up the build number of the corresponding"
echo " branch commit in github:"
echo " "
echo " In the user's rc:"
echo ""
echo "   export CARATE_WORKSPACE=/home/tester/software/caracal_tests"
echo "   export CARATE_TEST_DATA_DIR=/home/jozsa/software/caracal_tests/rawdata"
echo "   export PATH=\$PATH:/home/user/software/caracal/caracal/utils/carate.sh"
echo ""
echo " or"
echo ""
echo "   setenv CARATE_WORKSPACE \"/home/tester/software/caracal_tests\""
echo "   setenv CARATE_TEST_DATA_DIR=\"/home/jozsa/software/caracal_tests/rawdata\""
echo "   set path = ( \$path /home/user/software/caracal/caracal/utils/carate.sh)"
echo ""
echo " Then start carate"
echo ""
echo "   \$carate.sh -dm -ur -f -cb 6d562c 2>&1 | tee carate_run.log"
echo ""
echo ""
echo " Example 2:"
echo ""
echo " Testing a local installation using Docker only with an own"
echo " configuration file and installing using requirements.txt. The user is"
echo " permanently testing CARACal and has therefore a standard directory"
echo " with test files and a standard test location.:"
echo ""
echo " In the user's rc:"
echo "   export CARATE_WORKSPACE=/home/tester/software/caracal_tests"
echo "   export CARATE_TEST_DATA_DIR=/home/jozsa/software/caracal_tests/rawdata"
echo "   export PATH=\$PATH:/home/user/software/caracal/caracal/utils/carate.sh"
echo ""
echo " or"
echo ""
echo "   setenv CARATE_WORKSPACE \"/home/tester/software/caracal_tests\""
echo "   setenv CARATE_TEST_DATA_DIR=\"/home/jozsa/software/caracal_tests/rawdata\""
echo "   set path = ( \$path /home/user/software/caracal/caracal/utils/carate.sh)"
echo ""
echo " Then start carate"
echo ""
echo "   \$ carate.sh -di -ur -f -cs $wherever/bla.yml"
echo "                -ct mynewthing 2>&1 | tee carate_run.log"
echo ""
echo " Notice that bla.yml should contain the line dataid: [''], which will be"
echo " replaced by a line containing the appropriate data sets from ../rawdata"
echo ""
fi

if [[ -n "$DR" ]]
then
    echo " Data reduction with caratekit"
    echo ""
    echo " It is possible to use caratekit.sh to perform a data reduction."
    echo " The advantage is that it is easy to report issues at"
    echo " https://github.com/caracal-pipeline/caracal/issues"
    echo ""
    echo " To create an installation the user chooses"
    echo "   - The location \${workspace} of a parent directory to a "
    echo "     caratekit test directory."
    echo "   - A name \${caracal_testdir} of the caracal test directory"
    echo " Installation/upgrade with [Docker](https://www.docker.com/) as"
    echo " containerisation technology:"
    echo " "
    echo " \$ caratekit.sh -ws \${workspace} -cr -di -ct \${caracal_testdir}"
    echo "                -rp install -f"
    echo " "
    echo " Installation/upgrade with Singularity as containerisation"
    echo " technology:"
    echo " "
    echo " \$ caratekit.sh -ws \${workspace} -cr -si -ct \${caracal_testdir}"
    echo "                -rp install -f"
    echo " "
    echo " "
    echo " Data reduction using caratekit.sh"
    echo ""
    echo " Multiple variants are possible, here we present three. See"
    echo " https://github.com/caracal-pipeline/caracal/blob/master/README.md for more"
    echo " details."
    echo ""
    echo " The user uses the same"
    echo ""
    echo "   - Workspace directory \${workspace} as has been used to install"
    echo "     caratekit.sh"
    echo ""
    echo "   - The same target directory \${carate_target} that caratekit.sh has"
    echo "     been installed in."
    echo ""
    echo " The user chooses:"
    echo ""
    echo "   - The name \${project} of the data reduction project"
    echo ""
    echo "   - The location \${configfile}.yml of a CARACal configuration"
    echo "     file. Templates can be found in the directory"
    echo "     \${workspace}/\${caracal_testdir}/caracal/caracal/sample_configurations. A"
    echo "     choice to start with is the file minitestConfig.yml."
    echo ""
    echo "   - The name \${rawdata} of a directory containing the measurement"
    echo "     sets (which have to have the suffix .ms) that are supposed to be"
    echo "     processed in the data reduction."
    echo ""
    echo " Simple data reduction"
    echo ""
    echo " If the user assumes to run CARACal only once but also at the"
    echo " beginning of any other data reduction process the user edits the file"
    echo " \${configfile}.yml following the CARACal description. Notice that using"
    echo " caratekit.sh the default is that the contents of the parameter dataid"
    echo " will be replaced to reflect the measurement sets found in the"
    echo " \${rawdata} directory. This can be overridden by using the caratekit.sh"
    echo " -kc switch. A (partial) data reduction is then conducted following the"
    echo " command (Docker):"
    echo ""
    echo "   \$ caratekit.sh -ws \${workspace} -cd -di -ct \${caracal_testdir} -rp"
    echo "                \${project} -cs \${configfile}.yml -td \${rawdata}"
    echo ""
    echo " (Singularity):"
    echo ""
    echo "   \$ caratekit.sh -ws \${workspace} -cd -si -ct \${caracal_testdir} -rp"
    echo "                  \${project} -cs \${configfile}.yml -td \${rawdata}"
    echo ""
    echo " More detail can be found at:"
    echo " https://github.com/caracal-pipeline/caracal/blob/master/README.md"
    echo ""
fi

if [[ -n "$HE" ]] || [[ -n "$VE" ]] || [[ -n "$DR" ]]
then
    echo "Stopping. Do not set switches --help"
    echo "--verbose-help -h -v"
    echo "--data-reduction-info -dr to continue."
    echo ""
    kill "$PPID"; exit 1;
fi

function isin {
    
    # Checks if all elements of $1 are in $2
    # return 0 (true) if yes, return 1 (false) if no
    local i
    local j
    local anerror
    local -n firstset=$1
    local -n secondset=$2

    for i in ${firstset[@]}
    do
	anerror=1
	for j in ${secondset[@]}
	do
	    [[ ${i} != ${j} ]] || { anerror=0; break; }
	done
	[[ ${anerror} == 0 ]] || break
    done

    return ${anerror}
}

function waitforresponse {
    local mes=${1}
    local no_response
    local proceed
    
    [[ ${mes} == "" ]] || mes=${mes}' '
    echo "${mes}(Yes/No)"
    no_response=true
    while [ "$no_response" == true ]; do
	read proceed
	case "$proceed" in
	    [Yy][Ee][Ss]|[Yy]) # Yes or Y (case-insensitive).
		return 0
		;;
	    [Nn][Oo]|[Nn])  # No or N.
		return 1
		;;
	    *) # Anything else (including a blank) is invalid.
		{ echo "That is not a valid response."; }
		;;
	esac
    done
}

# Upgrade caratekit if requested
if [[ -n ${IN} ]]
then
    echo "Upgrade/Installation requested,"
    echo "upgrading/installing and then stopping."
    echo ""

        # Create a tempdir to pip clone caracal
    mytmpdir=$(mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir')
    
    # Create a trap to delete mytmpdir
    function cleanup {      
	rm -rf ${mytmpdir}
	kill "$PPID"; \
	exit 0; \
    }
    
    # register the cleanup function to be called on the EXIT signal
    trap cleanup EXIT
    
    # Now clone caracal
    if [[ -n "$CARATE_LOCAL_CARACAL" ]]
    then
	    [[ -n ${FS} ]] || cp -r ${CARATE_LOCAL_CARACAL}/* ${mytmpdir}/
    else
    [[ -n ${FS} ]] || git clone https://github.com/caracal-pipeline/caracal.git ${mytmpdir}
#    [[ -n ${FS} ]] || cp /home/jozsa/software/caracal/caratekit.sh ${mytmpdir}/
    fi
    
    # Check out desired branch
    cd ${mytmpdir}

    # Upon pypi release this has to be updated
    #    [[ -z ${CR} ]] || { \
    #	CR=`pip search caracal | grep "LATEST:" | awk '{print $2}'`; \
    #	thabuild=`git ls-remote --tags https://github.com/caracal-pipeline/caracal | grep ${CR} | awk '{print $1}'`; \
    #    }
    
    [[ -z ${CR} ]] || \
	{ \
	  [[ -z ${CARATE_CARACAL_BUILD_ID} ]] || \
	      { \
		"Not possible to both define a build id and requesting release branch at the same time."; \
		kill "$PPID"; \
		exit 1; \
	      }
	  CR="0.1.0" ; \
	  thabuild=`git ls-remote --tags https://github.com/caracal-pipeline/caracal | grep ${CR} | awk '{print $1}'`; \
	  }
    
    [[ -z ${CARATE_CARACAL_BUILD_ID} ]] || thabuild=${CARATE_CARACAL_BUILD_ID}
    
    [[ -z ${thabuild} ]] || git checkout ${thabuild}

    echo ""
    # Now ask if you can actually do this
    [[ -z ${caratekit_install} ]] || \
	{ \
	  echo "The current caratekit.sh"; \
	  echo "${caratekit_install}"; \
	  echo "will be replaced by"; \
	}
    echo "caratekit.sh from"
    echo "https://github.com/caracal-pipeline/caracal"
    [[ -z ${CR} ]] || echo "Release ${CR}"
    [[ -z ${thabuild} ]] || echo "Build ${thabuild}"
    echo "will be installed"
    [[ -z ${caratekit_install} ]] || echo "replacing the former one."
    echo ""

    if [[ -z ${OR} ]]
    then
	waitforresponse "Is that ok?" || { echo "Cowardly quitting"; kill "$PPID"; exit 1; }
    fi

    if [[ -z ${caratekit_install} ]]
    then	
    	echo "Please provide installation directory (/usr/local/bin)"
	no_response=true
	while [ "$no_response" == true ]; do
	    read proceed
	    proceed=`echo ${proceed} | sed 's:/*$::'`
	    if [[ -d ${proceed} ]]
	    then
		proceed=${proceed}/caratekit.sh
		no_response=false
	    elif [[ -z ${proceed} ]]
	    then
		proceed="/usr/local/bin/caratekit.sh"
		no_response=false
	    else
		echo "Please provide an existing path to a directory"
	    fi
	done
    else
	proceed=${caratekit_install}
    fi
    
    # Now try to copy the file
    success=false
    echo "Trying:"
    echo "\$ cp caratekit.sh ${proceed}"
    cp caratekit.sh ${proceed} &>/dev/null && success=true || true
    [[ ${success} == true ]] || \
	{ \
	  echo "Normal copy failed, trying to use sudo:"; \
	  echo "\$ sudo cp caratekit.sh ${proceed}"; \
	  #	sudo cp caratekit.sh ${proceed} &>/dev/null && success=true || true ; \
	  sudo cp caratekit.sh ${proceed} && success=true || true ; \
	}
    [[ ${success} == true ]] || \
	{ \
	  echo "No success, aborting"; \
	  kill "$PPID"; \
	  exit 1; \
	}
    echo ""
    
    caracalpath=`echo ${proceed} | sed 's|/[^/]*$||'`
    
    # Check if the directory is in path
    case :$PATH: # notice colons around the value
    in *:${caracalpath}:*) ;; # do nothing, it's there
       *:${caracalpath}/:*) ;; # do nothing, it's there
       *) notinpath=true ;;
    esac
    if [[ ${notinpath} == true ]]
    then
	echo "None of the directories in \$PATH contains caratekit.sh"
	echo "This means that you need to call"
	echo "$ ${caracalpath}/caratekit.sh"
	echo "instead of just"
	echo "$ caratekit.sh"
	echo "Shall this be prevented by adding"
	echo "${caracalpath}"
	echo "in your path, editing .bahrc and .cshrc ?"
	echo "(this will modify/create the hidden files ~/.bahrc and ~/.cshrc)"
	if waitforresponse
	then
	    echo "" >> ~/.cshrc
	    echo "set path = ( ${caracalpath} \${path} )" >> ~/.cshrc
	    echo "" >> ~/.cshrc
	    echo "export PATH=${caracalpath}:\$PATH" >> ~/.bashrc
	    echo "" >> ~/.bashrc
	fi
    fi
    echo "Installation successful"
    echo ""
    echo "To learn about caratekit.sh type:"
    echo " $ caratekit.sh -h"
    echo " $ caratekit.sh -v"
    echo ""
    echo "To learn quickly about data reduction with caratekit.sh type:"
    echo " $ caratekit.sh -dr"
    echo ""
    echo "#######################"
    echo ""
    
    exit 0
fi

echo "##########"
echo " Starting "
echo "##########"
echo ""

# Environment variables
[[ -n "$CARATE_WORKSPACE" ]] || \
    { \
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
if [[ -z "${CARATE_TEST_DATA_DIR}" ]]
then
    if [[ -n ${DM} ]] || [[ -n ${DA} ]] || [[ -n ${SM} ]] || [[ -n ${SA} ]] || [[ -n ${DSC} ]] || [[ -n ${SSC} ]]
    then
	tdfault=1
    else
	[[ -z ${CARATE_CONFIG_SOURCE} ]] || tdfault=1
    fi
    CARATE_TEST_DATA_DIR_OLD=""
else
    [[ -e ${CARATE_TEST_DATA_DIR} ]] || tdfault=2
    CARATE_TEST_DATA_DIR_OLD=${CARATE_TEST_DATA_DIR}
fi

# If there is an error of the first kind, check if the od switch is set
if [[ ${tdfault} == 1 ]]
then
    [[ -z ${OD} ]] || \
	{ \
	  tdfault = 0;
	}
fi

(( $tdfault == 0 )) || \
    { \
      echo "You have to define a CARATE_TEST_DATA_DIR variable, like (if";\
      echo "you're using bash):";\
      echo "$ export CARATE_TEST_DATA_DIR=\"/home/username/caracal_tests/rawdata\"";\
      echo "Or use the -td switch.";\
      echo "You also have to create that directory $CARATE_TEST_DATA_DIR";\
      echo "and put test rawdata therein: a.ms  b.ms c.ms ...";\
      echo "These test data will be copied across for the test.";\
      kill "$PPID"; \
      exit 1; \
    }

[[ ! -n "${CARATE_TEST_DATA_DIR}" ]] || ss+="test_data_dir=${CARATE_TEST_DATA_DIR}"
[[ ! -n "${CARATE_TEST_DATA_DIR}" ]] || ss+=$'\n'

# Do not force test id to be identical with build number any more, if it is defined
[[ -z "$CARATE_CARACAL_BUILD_ID" ]] || { \
    echo "(${CARATE_CARACAL_TEST_ID})"; \
    [[ -n ${CARATE_CARACAL_TEST_ID} ]] || { \
	CARATE_CARACAL_TEST_ID=${CARATE_CARACAL_BUILD_ID}; \
    }; \
    [[ -z ${CR} ]] || { \
	echo "You cannot define a CARATE_CARACAL_BUILD_ID (through an environment"; \
	echo "variable or using --caracal-build-id or -cb switches and use the"; \
	echo "--caracal-release or -cr switches. Exiting."; \
	kill "$PPID"; exit 1;
    } ; \
}

# Do not define more than one home
counts=0
[[ -z ${HW} ]] || (( counts+=1 ))
[[ -z ${HO} ]] || { (( counts+=1 )); }
[[ -z ${CARATE_HOME_FOLDER} ]] || (( counts+=1 ))
(( ${counts} < 2 )) || { \
    echo "Use maximally one of switches and variables defined with -hw -ho -hf."; \
    kill "$PPID"; \
    exit 1; \
    }
(( ${counts} > 0 )) || \
    { \
      HT=1 \
    }

[[ -z ${CR} ]] || { \
    # Upon pypi release this has to be updated
    # CR=`pip search caracal | grep "LATEST:" | awk '{print $2}'`; \
    CR="0.1.0"
    [[ -n CARATE_CARACAL_TEST_ID ]] || { \
	CARATE_CARACAL_TEST_ID=${CR}; \
    }; \
}

[[ -n "$CARATE_CARACAL_TEST_ID" ]] || { \
    echo "Without build number you have to define a global CARATE_CARACAL_TEST_ID"; \
    echo "variable, giving your test directory a name, like (if you're using bash):"; \
    echo "$export CARATE_CARACAL_TEST_ID=\"b027661de6ff93a183ff240b96af86583932fc1e\""; \
    echo "Otherwise choose any unique identifyer. You can also use the -ct switch."; \
    echo "";\
    kill "$PPID"; exit 1;
}
ss+="caracal_test_id=${CARATE_CARACAL_TEST_ID}"
ss+=$'\n'

[[ -n "${DM}" ]] || [[ -n "${DA}" ]] || [[ -n "${DI}" ]] || [[ -n "${SM}" ]] || [[ -n "${SA}" ]] || [[ -n "${SI}" ]] || [[ -n "${DSC}" ]] || [[ -n "${SSC}" ]] || {\
    echo "Please use one of the switches -dm, -da, -di, -sm, -sa, -si, -dsc, -ssc";\
    echo "";\
    kill "$PPID"; exit 1;
}

if [[ -n "$CARATE_LOCAL_CARACAL" ]]
then
    [[ -d "${CARATE_LOCAL_CARACAL}" ]] || { \
	   echo "\$CARATE_LOCAL_CARACAL: ${CARATE_LOCAL_CARACAL}"; \
	   echo "is specified (environment variable \${CARATE_LOCAL_CARACAL})"; \
	   echo "is set or switches --local-caracal or -lc are used). But it"; \
	   echo "does not exist. Stopping."; \
	   kill "$PPID"; exit 1;
       }
       [[ -z ${CR} ]] || { \
	   echo "Warning: both \${CARATE_LOCAL_CARACAL} and switches --caracal-release or -cr"; \
	   echo "are set. This means that carate will try to check out the latest release from"; \
	   echo "your local CARACal version. It might not be what you want."; \
	   }
    ss+="local_caracal=${CARATE_LOCAL_CARACAL}"
    ss+=$'\n'
else
    [[ -n ${OF} ]] || { \
	echo "The variable CARATE_LOCAL_CARACAL is not set, meaning that CARACal"; \
	echo "will be downloaded from https://github.com/caracal-pipeline/caracal"; \
	echo ""; \
    }
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

# Determine the number of testruns
numberoftests=0
[[ -z ${DM} ]] || (( numberoftests+=1 ))
[[ -z ${DA} ]] || (( numberoftests+=1 ))
[[ -z ${SM} ]] || (( numberoftests+=1 ))
[[ -z ${SA} ]] || (( numberoftests+=1 ))
[[ -z ${DI} ]] || (( numberoftests+=1 ))
[[ -z ${SI} ]] || (( numberoftests+=1 ))
[[ -z ${DSC} ]] || (( numberoftests+=1 ))
[[ -z ${SSC} ]] || (( numberoftests+=1 ))
(( ${numberoftests} < 2 )) || \
    [[ -z ${MD} ]] || {\
	echo "Number of tests cannot be larger than 1 if switch --move-test-data or -md is set";\
	echo "Exiting";\
	echo "";\
	kill "$PPID"; exit 1;
    }

# Singularity directories, avoid conflicts
counts=0
[[ -z ${STW} ]] || (( counts+=1 ))
[[ -z ${STH} ]] || { (( counts+=1 )); }
[[ -z ${CARATE_SINGULARITY_TMPDIR} ]] || (( counts+=1 ))
(( ${counts}<2 )) || { \
    echo "Use maximally one of switches and variables defined with -stw -sth -std."; \
    kill "$PPID"; \
    exit 1; \
    }

counts=0
[[ -z ${SCW} ]] || (( counts+=1 ))
[[ -z ${SCH} ]] || { (( counts+=1 )); }
[[ -z ${CARATE_SINGULARITY_CACHEDIR} ]] || (( counts+=1 ))
(( ${counts}<2 )) || { \
    echo "Use maximally one of switches and variables defined with -scw -sch -scd."; \
    kill "$PPID"; \
    exit 1; \
    }

counts=0
[[ -z ${SLW} ]] || (( counts+=1 ))
[[ -z ${SLH} ]] || { (( counts+=1 )); }
[[ -z ${CARATE_SINGULARITY_LOCALCACHEDIR} ]] || (( counts+=1 ))
(( ${counts}<2 )) || { \
    echo "Use maximally one of switches and variables defined with -slw -slh -sld."; \
    kill "$PPID"; \
    exit 1; \
    }

spfcounts=0
[[ -z ${SPW} ]] || (( spfcounts+=1 ))
[[ -z ${SPH} ]] || { (( spfcounts+=1 )); }
[[ -z ${CARATE_STIMELA_PULLFOLDER} ]] || (( spfcounts+=1 ))
(( ${spfcounts}<2 )) || { \
    echo "Use maximally one of switches and variables defined with -spw -sph -spf."; \
    kill "$PPID"; \
    exit 1; \
    }

# Default
(( ${spfcounts} > 0 )) || { \
			 [[ -n ${STIMELA_PULLFOLDER} ]] || SPW=1 ; \
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
if [[ -z ${HO} ]]
then
    [[ -z ${HOME} ]] && HOME_del=1 || \
	    { \
	      ss+="HOME_OLD=\${HOME}" ; \
	      ss+=$'\n' ; \
	      HOME_OLD=${HOME} ; \
	    }
fi

[[ -z ${PYTHONPATH} ]] && PYTHONPATH_del=1 || \
	{ \
	  ss+="PYTHONPATH_OLD=\${PYTHONPATH}" ; \
	  ss+=$'\n' ; \
	  PYTHONPATH_OLD=${PYTHONPATH} ; \
	}

if [[ -n ${SM} ]] || [[ -n ${SA} ]] || [[ -n ${SI} ]] || [[ -n ${SSC} ]]
then
    
    # Save them if they are present
    [[ -z ${SINGULARITY_CACHEDIR} ]] && CARACAL_SINGULARITY_CACHEDIR_del=1 || { \
	ss+="CARATE_SINGULARITY_CACHEDIR_OLD=\${SINGULARITY_CACHEDIR}" ; \
	ss+=$'\n'
	CARATE_SINGULARITY_CACHEDIR_OLD=${SINGULARITY_CACHEDIR} ; \
	}
    [[ -z ${SINGULARITY_DISABLE_CACHE} ]] && CARACAL_SINGULARITY_DISABLE_CACHE_del=1 || { \
	ss+="CARATE_SINGULARITY_DISABLE_CACHE_OLD=\${SINGULARITY_DISABLE_CACHE}" ; \
	ss+=$'\n'
	CARATE_SINGULARITY_DISABLE_CACHE_OLD=${SINGULARITY_DISABLE_CACHE} ; \
	}
    [[ -z ${SINGULARITY_LOCALCACHEDIR} ]] && CARACAL_SINGULARITY_LOCALCACHEDIR_del=1 || { \
	ss+="CARATE_SINGULARITY_LOCALCACHEDIR_OLD=\${SINGULARITY_LOCALCACHEDIR}" ; \
	ss+=$'\n'
	CARATE_SINGULARITY_LOCALCACHEDIR_OLD=${SINGULARITY_LOCALCACHEDIR} ; \
	}
    [[ -z ${SINGULARITY_TMPDIR} ]] && CARACAL_SINGULARITY_TMPDIR_del=1 || { \
	ss+="CARATE_SINGULARITY_TMPDIR_OLD=\${SINGULARITY_TMPDIR}" ; \
	ss+=$'\n'
        CARATE_SINGULARITY_TMPDIR_OLD=${SINGULARITY_TMPDIR} ; \
	}
    [[ -z ${STIMELA_PULLFOLDER} ]]  && STIMELA_PULLFOLDER_del=1 || { \
	ss+="CARATE_STIMELA_PULLFOLDER_OLD=\${STIMELA_PULLFOLDER}" ; \
	ss+=$'\n'
        CARATE_STIMELA_PULLFOLDER_OLD=${STIMELA_PULLFOLDER} ; \
	}
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
        configfilename=`echo ${CARATE_CONFIG_SOURCE} | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}'`
    fi
fi

# Just make sure this enters the script if necessary
[[ ! -n "${CARATE_INPUT_DIR}" ]] || ss+="input_dir=${CARATE_INPUT_DIR}"
[[ ! -n "${CARATE_INPUT_DIR}" ]] || ss+=$'\n'

# Determine CARATE_CARACAL_RUN_MEDIFIX to be something
[[ -n ${CARATE_CARACAL_RUN_MEDIFIX} ]] && CARATE_CARACAL_RUN_MEDIFIX="-${CARATE_CARACAL_RUN_MEDIFIX}" || CARATE_CARACAL_RUN_MEDIFIX=""

use_pull_force=""
[[ -z ${PF} ]] || (( ${FORCE} == 0 )) || use_pull_force=" -f"

# Variable defininition ends here in script
ss+=""
ss+=$'\n'

# Check if all's well, force reply by user
echo "The directory"
echo "$CARATE_WORKSPACE/$CARATE_CARACAL_TEST_ID"
echo "and its content will be created/changed."

if [[ -n ${SM} ]] || [[ -n ${SA} ]] || [[ -n ${SI} ]] || [[ -n ${SSC} ]]
then
[[ -z ${SDP} ]] || { \
       [[ -z ${SPW} ]] || echo "The Singularity pull folder ${CARATE_WORKSPACE}/stimela_pullfolder" ; \
    [[ -z ${SPT} ]] || echo "The Singularity pull folder ${CARATE_WORKSPACE}/${CARATE_CARACAL_TEST_ID}/stimela_pullfolder" ; \
    [[ -z ${CARATE_STIMELA_PULLFOLDER} ]] || echo "The Stimela pull folder ${CARATE_STIMELA_PULLFOLDER}" ; \
    [[ -z ${STIMELA_PULLFOLDER} ]] || (( ${spfcounts} > 0 )) || echo "The Stimela pull folder ${STIMELA_PULLFOLDER}" ; \
    echo "will be deleted and a reinstallation will be attempted." ; \
    }
fi
[[ -z ${use_pull_force} ]] || echo "The -f switch will be used if stimela pull is used"
echo "Depending on the settings, other directories"
echo "will also be changed."
echo ""

if [[ -z ${OR} ]]
then
    waitforresponse "Is that ok?" || { echo "Cowardly quitting"; kill "$PPID"; exit 1; }
fi

# Check if workspace_root exists if we do not use force
if (( ${FORCE} == 0 ))
then
    if [[ -d $WORKSPACE_ROOT ]]
    then
        echo "Be aware that no existing file will be replaced, use -f to override"
        echo ""
    fi
fi

# Protect existing directories and files
checkex () {
    # Check if the input $1 is a file and if yes if it is one of the user-
    # supplied files ${CARATE_CONFIG_SOURCE}, return 0 (true) if yes
    # Check the input is a directory and if yes if it is a parent or one of
    # the user-supplied files and directories ${CARATE_INPUT_DIR}
    # ${CARATE_LOCAL_CARACAL} ${CARATE_WORKSPACE} ${CARATE_TEST_DATA_DIR},
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
 files=(${CARATE_VIRTUALENV} ${CARATE_LOCAL_STIMELA} ${CARATE_INPUT_DIR} ${CARATE_LOCAL_CARACAL} ${CARATE_WORKSPACE} ${CARATE_TEST_DATA_DIR} ${CARATE_CONFIG_SOURCE})
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
     [[ ${e} != ${d} ]] || { return 0; }
 done
    fi
    return 1
}

echo ""
echo "##########################################"
echo " Setting up build in $WORKSPACE_ROOT"
echo "##########################################"

ss+="mkdir -p \${workspace_root}"
mkdir -p ${WORKSPACE_ROOT}

[[ ! -d ${WORKSPACE_ROOT}/report ]] || (( ${FORCE} == 0 )) || \
    [[ -n ${KR} ]] || \
    checkex ${WORKSPACE_ROOT}/report || \
    rm -rf ${WORKSPACE_ROOT}/report

mkdir -p ${WORKSPACE_ROOT}/report

# Small script, use txt suffix to be able to upload to multiple platforms
[[ -n ${CARATE_CARACAL_RUN_PREFIX} ]] && SS=${WORKSPACE_ROOT}/report/${CARATE_CARACAL_RUN_PREFIX}${CARATE_CARACAL_RUN_MEDIFIX}.sh.txt || \
SS=${WORKSPACE_ROOT}/report/${CARATE_CARACAL_TEST_ID}${CARATE_CARACAL_RUN_MEDIFIX}.sh.txt

# Empty ss into the small script
[[ ! -e ${SS} ]] || (( ${FORCE} == 0 )) || checkex ${SS} || rm -rf ${SS}
echo "$ss" >> ${SS}

# Sysinfo
[[ -n ${CARATE_CARACAL_RUN_PREFIX} ]] && SYA=${WORKSPACE_ROOT}/report/${CARATE_CARACAL_RUN_PREFIX}${CARATE_CARACAL_RUN_MEDIFIX}-sysinfo.txt || \
SYA=${WORKSPACE_ROOT}/report/${CARATE_CARACAL_TEST_ID}${CARATE_CARACAL_RUN_MEDIFIX}-sysinfo.txt

# Empty into the sysinfo
[[ ! -e ${SYA} ]] || (( ${FORCE} == 0 )) || checkex ${SYA} || rm -rf ${SYA}
echo "${sya}" >> ${SYA}

# Search for test data and set variable accordingly
if [[ -d ${CARATE_TEST_DATA_DIR} ]]
then

    # Check if there are any ms files
    mss=`find ${CARATE_TEST_DATA_DIR} -name *.ms`
    [[ -n "$mss" ]] || [[ -n ${OD} ]] || { echo "Test data required in ${CARATE_TEST_DATA_DIR}"; echo ""; kill "$PPID"; exit 1; }

    # Secondly, get the potential test data names

    # This generates the dataid string, first find all mss in the directory
    [[ -z "$mss" ]] && idlist=( ) || idlist=( `ls -d ${CARATE_TEST_DATA_DIR}/*.ms | sed '{s=.*/==;s/\.[^.]*$//}' | sed '{:q;N;s/\n/ /g;t q}'` )

    # First default it so define the final list of mss as this
    dataid_final=( ${idlist[@]} )
    
    # Second attempt: identify dataids from the input config file
    dataid_config=( )
    [[ -z ${CARATE_CONFIG_SOURCE} ]] || dataid_config=( `cat ${CARATE_CONFIG_SOURCE} | grep dataid | sed {'s/]/ /g;s/\[/ /g;s/\x27/ /g;s/\,/ /g;s/dataid:/ /g'}` )

    [[ -z ${CC} ]] || (( ${#dataid_config[@]} == 0 )) || dataid_final=( ${dataid_config[@]} )
    
    dataid_provided=( )
    # Third attempt: use dataids from the command line
    [[ -z ${CARATE_COPY_DATA_ID} ]] || dataid_provided=( `echo ${CARATE_COPY_DATA_ID} | sed {'s/]/ /g;s/\[/ /g;s/\x27/ /g;s/\,/ /g;s/dataid:/ /g'}` )

    echo ${CARATE_COPY_DATA_ID} | grep dataid | sed {'s/]/ /g;s/\[/ /g;s/\x27/ /g;s/\,/ /g;s/dataid:/ /g'}
    
    # More than one found, means that the final data ids are changed
    (( ${#dataid_provided[@]} == 0 )) || dataid_final=( ${dataid_provided[@]} )

    # OK, now we check if the data sets requested are present in the directory
    [[ -n ${OD} ]] || \
 isin dataid_final idlist || { \
     echo "The test data specified either directly or using"; \
     echo "the configuration file with the ids"; \
     echo "${dataid_final[@]}"; \
     echo "are not present in the"; \
     echo "test data directory"; \
     echo "$CARATE_TEST_DATA_DIR"; \
     echo "Quitting."; \
     echo ""; \
     kill "$PPID"; \
     exit 1; \
     }

    isalternative=0
    
    # Check if the test data are the standard data and ask if this is ok
    if isin dataid_final ALTERNATIVE_TEST_DATA_IDS && isin ALTERNATIVE_TEST_DATA_IDS dataid_final
    then
	isalternative=1
    fi

    # Warn if these are unusual data sets
    if [[ -n ${DA} ]] || [[ -n ${SA} ]]
    then
	if [[ ${isalternative} != 1 ]]
	then
	    echo "You intend to use unusual data sets for switches"
	    echo "--docker-alternative, -da, --singularity-alternative, or -sa"
	    echo "The dataids are:"
	    echo ${dataid_final[@]}
	    if [[ -z ${OR} ]]
	    then
		waitforresponse "Proceed?" || { echo "OK, stopping."; kill "$PPID"; exit 1; }
		echo ""
	    fi
	fi
    fi
    
    isshort=0
    # Check if the test data are the standard data and ask if this is ok
    if isin dataid_final SHORT_TEST_DATA_IDS && isin SHORT_TEST_DATA_IDS dataid_final
    then
 isshort=1
    fi

    # Warn if these are unusual data sets
    if [[ -n ${DM} ]] || [[ -n ${SM} ]]
    then
 if [[ ${isshort} != 1 ]]
 then
     echo "You intend to use unusual data sets for switches"
     echo "--docker-minimal, -dm, --singularity-minimal, or -sm"
     echo "The dataids are:"
     echo ${dataid_final[@]}
     if [[ -z ${OR} ]]
     then
  waitforresponse "Proceed?" || { echo "OK, stopping."; kill "$PPID"; exit 1; }
     fi
     echo ""
 fi
    fi

    # Finally, if there is a difference between the chosen data sets and the ones in the config file, this will be commented.
    if (( ${#dataid_config[@]} != 0 ))
    then
 notequalconfs=0
 isin dataid_config dataid_final && isin dataid_final dataid_config || notequalconfs=1
 if (( notequalconfs == 1 ))
 then
     echo "You intend to use different data sets in your test"
     echo "run than are specified in your configuration file"
     echo "The dataids in the configuration file are:"
     echo ${dataid_config[@]}
     echo "The dataids that will be used are:"
     echo ${dataid_final[@]}
     if [[ -z ${OR} ]]
     then
  waitforresponse "Proceed?" || { echo "OK, stopping."; kill "$PPID"; exit 1; }
     fi
  echo ""
 fi
    fi

    dataidstr=`echo ${dataid_final[@]} | sed '{s/ /\x27,\x27/g; s/$/\x27\]/; s/^/dataid: \[\x27/}'`

    # Finally finally we create the copy or move string
    copytestdatastr=""
    for ii in ${dataid_final[@]}
    do
 copytestdatastr+="${CARATE_TEST_DATA_DIR}/${ii}.ms "
 stringcopytestdatastr+="\${test_data_dir}/${ii}.ms "
    done

    #echo copytestdatastr ${copytestdatastr}
    #echo stringcopytestdatastr ${stringcopytestdatastr}
    
    echo "" >> ${SYA}
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    [[ -z "$mss" ]] || sya=`ls -d ${CARATE_TEST_DATA_DIR}/*.ms | sed '{s=.*/==}' | sed '{:q;N;s/\n/, /g;t q}'`
    [[ -z "$mss" ]] || echo "Files found in test data directory: ${sya}" >> ${SYA} || echo "Test data directory was not defined." >> ${SYA}

    # Size of test data
    outsize=`du -ms ${CARATE_TEST_DATA_DIR} | awk '{print $1}'`
    echo "Total size of test data directory: ${outsize} MB" >> ${SYA}
    [[ -z ${KC} ]] || { echo "--keep-config-source or -kc switch is set."; \
   echo "The real test data might hence be different."; \
    }
    [[ -z ${OD} ]] || { echo "--omit-copy-test-data or -od switch is set."; \
   echo "The real test data might hence be different."; \
    }
    echo ""
fi

# This contains a list of tests made
listoftestreps=()
ss_docker=""
ss_sing=""
sya_docker=""
sya_sing=""

function cleanup {

    local trname
    
    [[ -z ${ss_docker} ]] || echo "${ss_docker}" >> ${SS}
    [[ -z ${ss_sing} ]] || echo "${ss_sing}" >> ${SS}
    [[ -z ${sya_docker} ]] || echo "${sya_docker}" >> ${SYA}
    [[ -z ${sya_sing} ]] || echo "${sya_sing}" >> ${SYA}

    if (( success == 0 ))
    then
 echo ""
 echo "##########################################"
 echo ""
 echo ${kkfailquotes[$(( $RANDOM % 16 ))]}
 echo "Caratekit failed."
 echo ""
 echo "##########################################"
 echo ""
    echo "" >> ${SYA}
        echo "############" >> ${SYA}
        echo "" >> ${SYA}
        echo "Test failed." >> ${SYA}
    else
 echo ""
 echo "###########################################################"
 echo ""
 echo ${kksuccessquotes[$(( $RANDOM % 2 ))]}
 echo "Caratekit succeeded."
 echo ""
 echo "###########################################################"
 echo ""
 echo "" >> ${SYA}
 echo "###############" >> ${SYA}
 echo "" >> ${SYA}
 echo "Test succeeded." >> ${SYA}
 echo "" >> ${SYA}
    fi

    [[ -z ${endimessage} ]] || echo "${endimessage}"

    echo "" >> ${SYA}
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    sya=" End time: "; sya+=`date -u`;
    echo "${sya}" >> ${SYA}
    echo "" >> ${SYA}
    echo "##########################################" >> ${SYA}

    if [[ -z ${HO} ]]
       if [[ -n ${HOME_OLD} ]]
       then
	   if [[ -z ${HOME_del} ]]
	   then
               echo "export HOME=\${HOME_OLD}" >> ${SS}
               export HOME=${HOME_OLD}
	   else
	       echo "unset HOME" >> ${SS}
	       unset HOME
	   fi
       fi
    fi
       
    if [[ -n ${PYTHONPATH_OLD} ]]
    then
	if [[ -z ${PYTHONPATH_del} ]]
	then
            echo "export PYTHONPATH=\${PYTHONPATH_OLD}" >> ${SS}
            export PYTHONPATH=${PYTHONPATH_OLD}
	else
	    [[ -z ${PYTHONPATH} ]] || \
		{ \
		  echo "unset PYTHONPATH" >> ${SS} ; \
		  unset PYTHONPATH ; \
		}
	fi
    fi

    if [[ -n ${CARATE_SINGULARITY_CACHEDIR_OLD} ]]
    then
	if [[ -z ${CARACAL_SINGULARITY_CACHEDIR_del} ]]
	then
            echo "export SINGULARITY_CACHEDIR=\${CARATE_SINGULARITY_CACHEDIR_OLD}" >> ${SS}
            export SINGULARITY_CACHEDIR=${CARATE_SINGULARITY_CACHEDIR_OLD}
	else
	    [[ -z ${SINGULARITY_CACHEDIR} ]] || \
		{ \
		  echo "unset SINGULARITY_CACHEDIR" >> ${SS} ; \
		  unset SINGULARITY_CACHEDIR ; \
		}
	fi
    fi

    if [[ -n ${CARATE_SINGULARITY_DISABLE_CACHE_OLD} ]]
    then
	if [[ -z ${CARACAL_SINGULARITY_DISABLE_CACHE_del} ]]
	then
            echo "export SINGULARITY_DISABLE_CACHE=\${CARATE_SINGULARITY_DISABLE_CACHE_OLD}" >> ${SS}
            export SINGULARITY_DISABLE_CACHE=${CARATE_SINGULARITY_DISABLE_CACHE_OLD}
	else
	    [[ -z ${SINGULARITY_DISABLE_CACHE} ]] || \
		{ \
		  echo "unset SINGULARITY_DISABLE_CACHE" >> ${SS} ; \
		  unset SINGULARITY_DISABLE_CACHE ; \
		}
	fi
    fi

    if [[ -n ${CARATE_SINGULARITY_LOCALCACHEDIR_OLD} ]]
    then
	if [[ -z ${CARACAL_SINGULARITY_LOCALCACHEDIR_del} ]]
	then
            echo "export SINGULARITY_LOCALCACHEDIR=\${CARATE_SINGULARITY_LOCALCACHEDIR_OLD}" >> ${SS}
            export SINGULARITY_LOCALCACHEDIR=${CARATE_SINGULARITY_LOCALCACHEDIR_OLD}
	else
	    [[ -z ${SINGULARITY_LOCALCACHEDIR} ]] || { \
						  echo "unset SINGULARITY_LOCALCACHEDIR" >> ${SS} ; \
						  unset SINGULARITY_LOCALCACHEDIR ; \
		}
	fi
    fi

    if [[ -n ${CARATE_SINGULARITY_TMPDIR_OLD} ]]
    then
 if [[ -z ${CARATE_SINGULARITY_TMPDIR_del} ]]
 then
     echo "export SINGULARITY_TMPDIR=\${CARATE_SINGULARITY_TMPDIR_OLD}" >> ${SS} 
     export SINGULARITY_TMPDIR=${CARATE_SINGULARITY_TMPDIR_OLD}
 else
     [[ -z ${SINGULARITY_TMPDIR} ]] || { \
  echo "unset SINGULARITY_TMPDIR" >> ${SS} ; \
  unset SINGULARITY_TMPDIR ; \
  }
 fi
    fi

    if [[ -n ${CARATE_STIMELA_PULLFOLDER_OLD} ]]
    then
 if [[ -z CARATE_STIMELA_PULLFOLDER_del ]]
 then
     echo "export STIMELA_PULLFOLDER=\${CARATE_STIMELA_PULLFOLDER_OLD}" >> ${SS} 
     export STIMELA_PULLFOLDER=${CARATE_STIMELA_PULLFOLDER_OLD}
 else
     [[ -z ${STIMELA_PULLFOLDER} ]] || { \
                echo "unset STIMELA_PULLFOLDER" >> ${SS} ; \
                unset STIMELA_PULLFOLDER ; \
  }
 fi
    fi

    # If the number of tests is 0 we create the report directory
    if [[ ${#listoftestreps[@]} == 0 ]]
    then
 if [[ -n ${CARATE_CARACAL_RUN_PREFIX} ]]
 then
     mkdir -p ${WORKSPACE_ROOT}/report/${CARATE_CARACAL_RUN_PREFIX}
     mv ${SS} ${WORKSPACE_ROOT}/report/${CARATE_CARACAL_RUN_PREFIX}/
     mv ${SYA} ${WORKSPACE_ROOT}/report/${CARATE_CARACAL_RUN_PREFIX}/
 fi
    else
	rm ${SS} ${SYA}
    fi
}
trap cleanup EXIT

if [[ -n ${HT} ]]
then
    echo "export HOME=\${workspace_root}/home"
    echo "export HOME=\${workspace_root}/home" >> ${SS}
    export HOME=${CARATE_WORKSPACE}/${CARATE_CARACAL_TEST_ID}
fi
if [[ -n ${HW} ]]
then
    echo "export HOME=\${workspace}/home"
    echo "export HOME=\${workspace}/home" >> ${SS}
    export HOME=${CARATE_WORKSPACE}/home
fi
if [[ -n ${CARATE_HOME_FOLDER} ]]
then
    echo "export HOME=${CARATE_HOME_FOLDER}"
    echo "export HOME=${CARATE_HOME_FOLDER}" >> ${SS}
    export HOME=${CARATE_HOME_FOLDER}
fi

[[ -n ${HOME} ]] || \
    { \
      echo "No home directory defined. Cannot continue."; \
      echo ""; \
      kill "$PPID"; \
      exit 1; \
    }		      

if (( ${FORCE} != 0 ))
then
    # This is dangerous, use with great care, poison
    [[ -n ${HN} ]] || \
	[[ -n ${ORSR} ]] || \
	{ \
	  [[ ${HOME} == ${HOME_OLD} ]] && \
	      echo "We do not delete ${HOME_OLD}"; \
	} || \
	{ \
	  [[ ${HOME} == /home/${USER} ]] && \
	      echo "We do not delete /home/${USER}"; \
	} || \
	{ \
	  echo "rm -rf \${HOME}" >> ${SS}; \
          [[ -n ${FS} ]] || \
	      { \
		rm -rf ${HOME}; \
	      } \
	}
fi

# Check if home exists, otherwise make it
[[ -e ${HOME} ]] || \
    { \
      echo "mkdir -p \${HOME}" >> ${SS}; \
      mkdir -p ${HOME} >> ${SS}; \
    }

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
 } || { \
     echo "rm -rf \${cvirtualenv}" >> ${SS}; \
     [[ -n ${FS} ]] || \
  rm -rf ${CARATE_VIRTUALENV}; \
     }
fi
if [[ ! -d ${CARATE_VIRTUALENV} ]]
then
    [[ -n ${FS} ]] || \
	{ echo "python3 -m venv \${cvirtualenv}" >> ${SS} && python3 -m venv ${CARATE_VIRTUALENV}; } || \
	{ echo 'Using "python3 -m venv" failed when instaling virtualenv.'; \
	  echo 'Trying "virtualenv -p python3"'; \
	  echo "rm -rf \${cvirtualenv}" >> ${SS}; \
	  rm -rf ${CARATE_VIRTUALENV}; \
	  echo "virtualenv -p python3 \${cvirtualenv}" >> ${SS} && virtualenv -p python3 ${CARATE_VIRTUALENV}; \
	}
fi

# Report on virtualenv
if [[ -f ${CARATE_VIRTUALENV}/pyvenv.cfg ]]
then
    echo "" >> ${SYA}
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    echo "Virtualenv info (from pyvenv.cfg):" >> ${SYA}
    cat ${CARATE_VIRTUALENV}/pyvenv.cfg >> ${SYA}
fi

echo "Entering virtualenv in ${WORKSPACE_ROOT}"
echo ". \${cvirtualenv}/bin/activate" >> ${SS}
# [[ -n ${FS} ]] || . ${CARATE_VIRTUALENV}/bin/activate
[[ -n ${FS} ]] || . ${CARATE_VIRTUALENV}/bin/activate

echo "export PYTHONPATH=''" >> ${SS}
export PYTHONPATH=''
echo "pip install pip setuptools wheel -U"  >> ${SS}
[[ -n ${FS} ]] || pip install pip setuptools wheel -U

# Report on Python version
echo "" >> ${SYA}
echo "##########################################" >> ${SYA}
echo "" >> ${SYA}
python --version >> ${SYA}

# Report on pip version
pip --version | awk '{print $1, $2}' >> ${SYA}
echo ""

# Install software
echo "##################"
echo " Fetching CARACal "
echo "##################"
echo
if (( ${FORCE} == 1 ))
then
    checkex ${WORKSPACE_ROOT}/caracal || \
 [[ -n ${OF} ]] || \
 echo "rm -rf \${workspace_root}/caracal" >> ${SS}
    [[ -n ${FS} ]] || \
 checkex ${WORKSPACE_ROOT}/caracal || \
 [[ -n ${OF} ]] || \
 rm -rf ${WORKSPACE_ROOT}/caracal
fi

echo "cd \${workspace_root}" >> ${SS}
cd ${WORKSPACE_ROOT}
if [[ -n "$CARATE_LOCAL_CARACAL" ]]
then
    if [[ -e ${WORKSPACE_ROOT}/caracal ]]
    then
        echo "Not re-fetching CARACal, use -f if you want that or"
        echo "omit -of if you have set it."
    else
 echo "Fetching CARACal from local source ${local_caracal}"
        echo "cp -r \${local_caracal} \${workspace_root}/" >> ${SS}
 [[ -n ${FS} ]] || cp -r ${CARATE_LOCAL_CARACAL} ${WORKSPACE_ROOT}/
    fi
else
    if [[ -e ${WORKSPACE_ROOT}/caracal ]]
    then
        if (( ${FORCE} == 0 ))
        then 
            echo "Not re-fetching CARACal, use -f if you want that."
 elif [[ -n ${OF} ]]
 then
     echo "Not re-fetching CARACal, turn off -of if you want that."
        else
     echo "Fetching CARACal from https://github.com/caracal-pipeline/caracal.git"
     checkex ${WORKSPACE_ROOT}/caracal || \
  echo "rm -rf \${workspace_root}/caracal" >> ${SS}
     [[ -n ${FS} ]] || \
  checkex ${WORKSPACE_ROOT}/caracal || \
  rm -rf ${WORKSPACE_ROOT}/caracal
     
            checkex ${WORKSPACE_ROOT}/caracal || \
  echo "git clone https://github.com/caracal-pipeline/caracal.git" >> ${SS}
            [[ -n ${FS} ]] || \
  checkex ${WORKSPACE_ROOT}/caracal || \
  git clone https://github.com/caracal-pipeline/caracal.git
        fi
    else
 echo "Fetching CARACal from https://github.com/caracal-pipeline/caracal.git"
 echo "git clone https://github.com/caracal-pipeline/caracal.git" >> ${SS}
 [[ -n ${FS} ]] || git clone https://github.com/caracal-pipeline/caracal.git
    fi
fi

if [[ -n "$CARATE_CARACAL_BUILD_ID" ]]
then
    echo "cd \${workspace_root}/caracal" >> ${SS}
    [[ -n ${FS} ]] || cd ${WORKSPACE_ROOT}/caracal
    [[ -z $CARATE_LOCAL_CARACAL ]] || { \
 echo "If an error occurs here, it likely means that the local installation";\
 echo "of CARACal does not contain the build number. You may want to use the";\
 echo "master branch and unset the environmrnt variable CARATE_CARACAL_BUILD_ID:";\
 echo "In bash: $ unset CARATE_CARACAL_BUILD_ID";\
 echo "Also, you might have used switches --caracal-build-id or -cb, which";\
 echo "you should not";\
    }
    echo "git checkout ${CARATE_CARACAL_BUILD_ID}" >> ${SS}
    [[ -n ${FS} ]] || git checkout ${CARATE_CARACAL_BUILD_ID}
fi

if [[ -n ${CR} ]]
then
    echo "cd \${workspace_root}/caracal" >> ${SS}
    cd ${WORKSPACE_ROOT}/caracal
    [[ -z $CARATE_LOCAL_CARACAL ]] || \
	{ \
	  echo "If an error occurs here, it likely means that the local installation" ; \
	  echo "of CARACal does not contain the latest release. Because you set the" ; \
	  echo "switches --caracal-release or -cr, caratekit is trying to install the" ; \
	  echo "latest release."; \
	}
    thabuild=`git ls-remote --tags https://github.com/caracal-pipeline/caracal | grep ${CR} | awk '{print $1}'`
    echo "git checkout ${thabuild}" >> ${SS}
    git checkout ${thabuild}
fi

echo ""

echo "###############################"
echo " Installing/activating CARACal "
echo "###############################"
echo

#PATH=${WORKSPACE}/projects/pyenv/bin:$PATH
#LD_LIBRARY_PATH=${WORKSPACE}/projects/pyenv/lib:$LD_LIBRARY_PATH
if [[ -z ${OC} ]]
then
    echo "Installing CARACal using pip install"
    ii=1
    until (( ${ii} > ${IA} ))
    do
 echo "Running pip install -U --force-reinstall \${workspace_root}/caracal"
 echo "pip install -U --force-reinstall \${workspace_root}/caracal" >> ${SS}
        if [[ -z ${FS} ]]
 then
     pip install -U --force-reinstall ${WORKSPACE_ROOT}/caracal && break || {
      echo "pip install -U --force-reinstall \${workspace_root}/caracal failed"
      (( ii++ ))
  }
 else
     break
 fi
    done
fi

# Checking if caratekit has changed
if [[ -z ${FS} ]]
then
    [[ -z ${caratekit_install} ]] || caratekit_install_changes=`diff ${caratekit_install} ${WORKSPACE_ROOT}/caracal/caratekit.sh` || true
    [[ -z ${caratekit_install_changes} ]] || { \
					       endimessage+="The installed caratekit.sh:"; endimessage+=$'\n'; \
					       endimessage+="  ${caratekit_install}"; endimessage+=$'\n'; \
					       endimessage+="differs from the one found in \${WORKSPACE_ROOT}/caracal:"; endimessage+=$'\n'; \
					       endimessage+="  ${WORKSPACE_ROOT}/caracal/caratekit.sh"; endimessage+=$'\n'; \
					       endimessage+="Consider updating your caratekit installation by typing:"; endimessage+=$'\n'; \
					       endimessage+="  $ caratekit.sh --install"; endimessage+=$'\n'; \
					       endimessage+=$'\n';
					       endimessage+="Details can be found at https://github.com/caracal-pipeline/caracal"; endimessage+=$'\n'; \
					       endimessage+=$'\n';
					       endimessage+="########################################################"
					       endimessage+=$'\n';
    }
fi

if [[ -n ${UM} ]]
then
    echo "Installing stimela-master.txt"
    ii=1
    until (( ${ii} > ${IA} ))
    do
 echo "Running pip install -U --force-reinstall -r \${workspace_root}/caracal/stimela-master.txt"
 echo "pip install -U --force-reinstall -r \${workspace_root}/caracal/stimela-master.txt" >> ${SS}
        if [[ -z ${FS} ]]
 then
     pip install -U --force-reinstall -r ${WORKSPACE_ROOT}/caracal/stimela-master.txt && break || {
      echo "pip install -U --force-reinstall -r \${workspace_root}/caracal/stimela-master.txt failed"
      (( ii++ ))
  }
 else
     break
 fi
    done
fi

if [[ -n ${US} ]]
then
    echo "Installing stimela-last_stable.txt"
    ii=1
    until (( ${ii} > ${IA} ))
    do
 echo "Running pip install -U --force-reinstall -r \${workspace_root}/caracal/stimela-last_stable.txt"
 echo "pip install -U --force-reinstall -r \${workspace_root}/caracal/stimela-last_stable.txt" >> ${SS}
        if [[ -z ${FS} ]]
 then
     pip install -U --force-reinstall -r ${WORKSPACE_ROOT}/caracal/stimela-last_stable.txt && break || {
      echo "pip install -U --force-reinstall -r \${workspace_root}/caracal/stimela-last_stable.txt failed"
      (( ii++ ))
  }
 else
     break
 fi
    done
fi

if [[ -n ${CARATE_LOCAL_STIMELA} ]]
then
    echo "Installing local stimela ${CARATE_LOCAL_STIMELA}"
    echo "pip install -U --force-reinstall \${local_stimela}" >> ${SS}
    [[ -n ${FS} ]] || pip install -U --force-reinstall ${CARATE_LOCAL_STIMELA}
    ii=1
    until (( ${ii} > ${IA} ))
    do
 echo "Running pip install -U --force-reinstall \${CARATE_LOCAL_STIMELA}"
 echo "pip install -U --force-reinstall -r  \${local_stimela}" >> ${SS}
        if [[ -z ${FS} ]]
 then
     pip install -U --force-reinstall ${CARATE_LOCAL_STIMELA} && break || {
      echo "pip install -U --force-reinstall \${local_stimela} failed"
      (( ii++ ))
  }
 else
     break
 fi
    done
fi

if [[ -d ${WORKSPACE_ROOT}/caracal ]]
then
    # Report on CARACal build
    echo "" >> ${SYA}
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    cd ${WORKSPACE_ROOT}/caracal
    if [[ -n ${CR} ]]
    then
 echo "CARACal release: ${CR}" >> ${SYA}
    else
 [[ -n ${CARATE_CARACAL_BUILD_ID} ]] || \
 [[ -n ${CARATE_LOCAL_CARACAL} ]] || { \
     echo "CARACal build from master at https://github.com/caracal-pipeline/caracal" >> ${SYA};\
 }
    fi
    
    if [[ -n "$CARATE_LOCAL_CARACAL" ]] && [[ -z ${CR} ]]
    then
 echo "CARACal build: local" >> ${SYA}
    else
        sya="CARACal build: "; sya+=`git log -1 --format=%H`; sya+=$'\n';
 sya+="from: https://github.com/caracal-pipeline/caracal"
        echo "${sya}" >> ${SYA}
    fi
    [[ -z ${OC} ]] || echo "CARACal has not been re-build, so this is a guess" >> ${SYA}

    # Get Stimela tag. This can be simplified...
    if [[ -n ${US} ]]
    then
 stimelaline=`grep "https://github.com/ratt-ru/Stimela" stimela-last_stable.txt | sed -e 's/.*Stimela@\(.*\)#egg.*/\1/'` || \
     true
        if [[ -z ${stimelaline} ]]
        then
     # Stimela tag depends on whether the repository is in or not
            stimelaline=`grep "stimela==" setup.py | sed -e 's/.*==\(.*\)\x27.*/\1/'` || \
  true
            [[ -z ${stimelaline} ]] || echo "Stimela release: $stimelaline" >> ${SYA}
            stimelaline=`grep https://github.com/ratt-ru/Stimela setup.py` || \
  true
            [[ -z ${stimelaline} ]] || \
  stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'` || \
  true
            [[ -z ${stimelabuild} ]] || \
  echo "Stimela build: ${stimelabuild}" >> ${SYA}
        else
            echo "Stimela build: ${stimelaline}" >> ${SYA}
        fi
    elif [[ -n ${UM} ]]
    then
        # Stimela tag depends on whether the repository is in or not
        stimelaline=`grep https://github.com/ratt-ru/Stimela stimela-master.txt` || \
     true
 [[ -z ${stimelaline} ]] || \
     stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'`
        [[ -z ${stimelabuild} ]] || \
     echo "Stimela build: ${stimelabuild}" >> ${SYA}
    elif [[ -n ${CARATE_LOCAL_STIMELA} ]]
    then
 echo "Stimela build: ${CARATE_LOCAL_STIMELA} (local)" >> ${SYA}
    else
 # Attempt 1: if setup.py points to master
        stimelaline=`grep 'https://github.com/ratt-ru/Stimela' setup.py` || \
     true
 [[ -z ${stimelaline} ]] || \
     echo "Stimela master at https://github.com/ratt-ru/Stimela" >> ${SYA}
        [[ -z ${stimelaline} ]] || \
     stimelabuild=`git ls-remote https://github.com/ratt-ru/Stimela | grep HEAD | awk '{print $1}'` || \
     true

        # Attempt 2: if setup.py points to a specific release
        [[ -n ${stimelaline} ]] || \
     stimelaline=`grep "stimela==" setup.py | sed -e 's/.*==\(.*\)\x27.*/\1/'` || \
     true

 # Attempt 3: otherwise, it will be a >= tag, fetch the tag of the latest release
 [[ -n ${stimelaline} ]] || { \
     stimelaline=`pip search Stimela | grep "LATEST" | awk '{print $2}'`; \
 } || true

 # Attempt 4: if above did not work, then the installed is already the latest
 [[ -n ${stimelaline} ]] || { \
     stimelaline=`pip search Stimela | grep "latest" | awk '{print $2}'`; \
 } || true

 # We can report on the release if this is not an installation from master and hence stimelabuild is defined
        [[ -z ${stimelaline} ]] || \
     [[ -n ${stimelabuild} ]] || echo "Stimela release: $stimelaline" >> ${SYA}
 
 # If Stimela build has not been defined, do it now
 [[ -z ${stimelaline} ]] || \
     [[ -n ${stimelabuild} ]] || stimelabuild=`git ls-remote --tags https://github.com/ratt-ru/Stimela | grep ${stimelaline} | awk '{print $1}'` || \
     true
 [[ -z ${stimelabuild} ]] || echo "Stimela build: ${stimelabuild}" >> ${SYA} 
    fi

    echo "from: https://github.com/ratt-ru/Stimela" >> ${SYA}
    [[ -z ${ORSR} ]] || echo "Stimela has not been re-build, so this is a guess" >> ${SYA}
    echo ""
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
    kill "$PPID"; exit 1
fi

if [[ -e ${HOME}/.stimela ]]
then
    [[ -n ${FS} ]] || [[ -n ${ORSR} ]] || { \
 echo "#######################" ; \
 echo " Running stimela clean " ; \
 echo "#######################" ; \
 echo "" ; \
 echo "stimela clean -ac" >> ${SS} ; \
 stimela clean -ac ; \
    }
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
    # Consider using stimela clean -ac here before running a docker prune
    # and not doing this:
    [[ -n ${FS} ]] || checkex ${HOME}/.stimela || rm -f ${HOME}/.stimela/*
fi

# These need to be added to sya and ss only for docker or singularity
sya_docker=""
ss_docker=""

if [[ -n $DM ]] || [[ -n $DA ]] || [[ -n $DI ]] || [[ -n $DSC ]]
then
    # Prevent special characters to destroy installation
    stimela_ns=`whoami`
    stimela_bs=`whoami | sed 's/@/_/g'`
    [[ ${stimela_ns} == ${stimela_bs} ]] && stimela_ns="" || stimela_ns=" -bl ${stimela_bs}"

    if [[ -n ${ORSR} ]]
    then
	sya_docker+=$'\n'
        sya_docker+="##########################################"; sya_docker+=$'\n'
        sya_docker+=$'\n'
 sya_docker+="Omitting re-installation of Stimela Docker images"; sya_docker+=$'\n'
#        sya_docker+=$'\n'
    else
        echo
        echo "##################################"
        echo " Installing Stimela Docker images "
        echo "##################################"
        echo
        echo "Installing Stimela (Docker)"

	sya_docker+=$'\n'
        sya_docker+="##########################################"; sya_docker+=$'\n'
        sya_docker+=$'\n'
	sya_docker+=`docker --version`; sya_docker+=$'\n'
        sya_docker+=$'\n'

        # Not sure if stimela listens to $HOME or if another variable has to be set.
        # This $HOME is not the usual $HOME, see above
 [[ -z ${OR} ]] || pruneforce='-f'
        [[ -n ${OP} ]] || echo "Running docker system prune"
        [[ -n ${OP} ]] || { ss_docker+="docker system prune ${pruneforce}"; ss_docker+=$'\n'; }
 
        [[ -n ${OP} ]] || [[ -n ${FS} ]] || docker system prune ${pruneforce}
        if [[ -z ${NP} ]]
        then
     ii=1
     until (( ${ii} > ${IA} ))
     do
         echo "Running stimela pull -d${use_pull_force}"
         ss_docker+="stimela pull -d${use_pull_force}"; ss_docker+=$'\n'
                if [[ -z ${FS} ]]
         then
      stimela pull -d${use_pull_force} && break || {
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
  sya_docker+="Maximum number of pull attempts for Stimela reached."; sya_docker+=$'\n'
  #echo "${ss_docker}" >> ${SS}
  #echo "${sya_docker}" >> ${SYA}
  exit 1
     fi
        fi
# ii=1
#        until (( ${ii} > ${IA} ))
#        do
#            echo "Running stimela build"
#            ss_docker+="stimela build${stimela_ns}"; ss_docker+=$'\n'
#            if [[ -z ${FS} ]]
#     then
#  stimela build${stimela_ns} && break || {
#   echo "stimela build failed ${ii}" ; \
#   (( ii++ )) ; \
#      }
#     else
#  break
#     fi
#        done
# if (( ${ii} > ${IA} ))
# then
#     echo "Maximum number of build attempts for Stimela reached."
#     echo "${ss_docker}" >> ${SS}
#     echo "${sya_docker}" >> ${SYA}
#     exit 1
# fi
    echo ""
    fi
fi

#echo ""

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
    local SYA_TESTING=${3}

    echo
    echo "###################"
    echo " Counting logfiles "
    echo "###################"
    echo
    echo "Counting logfiles in directory ${1}/${2}"
    # Check here
    allogs=`[[ -e ${1}/${2}/output/logs ]] && ls -t ${1}/${2}/output/logs/ || echo ""`
#    allogs=`ls -t ${1}/${2}/output/logs/` || true
    caracallog=`[[ -e ${1}/${2}/output/logs ]] && ls -t ${1}/${2}/output/logs/log-caracal*.txt | head -1 || echo ""`
    [[ ${caracallog} == "" ]] && unset caracallog
#    caracallog=`ls -t ${1}/${2}/output/logs/log-caracal*.txt.txt | head -1 || true`
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
    echo "$reporting" >> ${SYA_TESTING}

    # Count number of runs of workers and the number of finishes
    worker_runs=0
    worker_fins=0
    [[ -z ${caracallog} ]] && { \
        reporting="This CARACal run is missing a summary logfile"; reporting+=$'\n'; \
 reporting+="Returning error";\
 reporting+=$'\n'; \
 echo "${reporting}"; \
 echo "${reporting}" >> ${SYA_TESTING}; \
 return 1; \
    } || { \
 worker_runs=`grep ": initializing" ${caracallog} | wc | sed 's/^ *//; s/ .*//'`; \
 worker_fins=`grep ": finished" ${caracallog} | wc | sed 's/^ *//; s/ .*//'`; \
 reporting="CARACal logfile indicates ${worker_runs} workers starting"; \
 reporting+=$'\n'; \
 reporting+="and ${worker_fins} workers ending."; reporting+=$'\n'; \
 echo "${reporting}"; \
 echo "${reporting}" >> ${SYA_TESTING}; \
    }


    (( $worker_runs == $worker_fins )) || { \
 reporting="Workers starting (${worker_runs}) and ending (${worker_fins}) are unequal in log-caracal.txt"; \
 reporting+=$'\n'; \
 echo "${reporting}"; \
 echo "${reporting}" >> ${SYA_TESTING}; \
 return 1; \
    }
    [[ -z $caracallog ]] || (( $worker_runs > 0 )) || { \
 reporting="No workers have started according to log-caracal.txt"; \
 reporting+=$'\n'; \
 reporting+="Returning error";\
 reporting+=$'\n'; \
 echo "${reporting}"; \
 echo "${reporting}" >> ${SYA_TESTING}; \
 return 1; \
    }

    # Notice that 0 is true in bash
    (( $total > 0 )) || { reporting="No logfiles produced. Returning error."; reporting+=$'\n'; \
                          "echo ${reporting}" \
     "echo ${reporting}" >> ${SYA_TESTING}; \
                          return 1; }
    [[ -n ${caracallog} ]] || { reporting="No CARACal main log produced. Returning error."; reporting+=$'\n'; \
                          "echo ${reporting}" \
     "echo ${reporting}" >> ${SYA_TESTING}; \
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
    # Argument 9: sya variable from container tech installation
    # Argument 10: ss variable from container tech installation
    # Argument 11: Switches to pass to caracal

    local greetings_line=$1
    local WORKSPACE_ROOT=$2
    local configfilename=$3
    local contarch=$4
    local FORCE=$5
    local configlocation=$6
    local configlocationstring=$7
    local testruns=$8
    local syacontainer=$9
    local sscontainer=${10}
    local caracalswitches=${11}
    
    local sya
    local trname
    local frname
    local failedrun
    local dirs
    local d
    local e
    local failed
    local mes
    local caracallog
    local failedoutput
    local outsize
    local SS_RUNTEST
    local SYA_RUNTEST
    local reportname
    local faultylist
    local jj
    local reportprefy

    echo ""
    echo "##########################################"
    echo " $greetings_line "
    echo "##########################################"
    echo

    failedrun=0

    # This
    # [[ -z ${frname} ]] || unset frname
    
    if [[ -n ${CARATE_CARACAL_RUN_PREFIX} ]]
    then
 (( ${testruns} == 1 )) && trname=${CARATE_CARACAL_RUN_PREFIX} || trname="${CARATE_CARACAL_RUN_PREFIX}-${testruns}"
 if [[ -n ${CARATE_CARACAL_FORMER_RUN} ]]
 then
     (( ${testruns} == 1 )) && frname=${CARATE_CARACAL_FORMER_RUN} || frname="${CARATE_CARACAL_FORMER_RUN}-${testruns}"
 fi
 reportprefy="-${configfilename}"
    else
 trname="${configfilename}-${contarch}"
 reportprefy=""
    fi

    reportname=${trname}
    listoftestreps+=( ${reportname} )

    mkdir -p ${WORKSPACE_ROOT}/report/${reportname}

    SS_RUNTEST=${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}.sh.txt
    [[ ! -e ${SS_RUNTEST} ]] || (( ${FORCE} == 0 )) || checkex ${SS_RUNTEST} || rm -rf ${SS_RUNTEST}
    cat ${SS} >> ${SS_RUNTEST}
    [[ -z ${sscontainer} ]] || echo "${sscontainer}" >> ${SS_RUNTEST}
    
    SYA_RUNTEST=${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}-sysinfo.txt
    [[ ! -e ${SYA_RUNTEST} ]] || (( ${FORCE} == 0 )) || checkex ${SYA_RUNTEST} || rm -rf ${SYA_RUNTEST}
    cat ${SYA} >> ${SYA_RUNTEST}
    [[ -z ${syacontainer} ]] || echo "${syacontainer}" >> ${SYA_RUNTEST}

    echo "" >> ${SYA_RUNTEST}
    echo "##########################################" >> ${SYA_RUNTEST}
    echo "" >> ${SYA_RUNTEST}
    sya="${trname} preparation start time:";sya+=$'\n'; sya+=`date -u`;
    echo "${sya}" >> ${SYA_RUNTEST}

    if [[ -e ${WORKSPACE_ROOT}/${trname} ]] && (( ${FORCE} == 0 ))
    then
        echo "Will not re-create existing directory ${WORKSPACE_ROOT}/${trname}"
        echo "and use old results. Use -f to override."
    else
 if [[ -n ${frname} ]] && [[ -e ${WORKSPACE_ROOT}/${frname} ]]
 then
     [[ ${WORKSPACE_ROOT}/${frname} ==  ${WORKSPACE_ROOT}/${trname} ]] || { \
  mv ${WORKSPACE_ROOT}/${frname} ${WORKSPACE_ROOT}/${trname}; \
  ln -s ${WORKSPACE_ROOT}/${trname} ${WORKSPACE_ROOT}/${frname}
  CARATE_TEST_DATA_DIR=${WORKSPACE_ROOT}/${trname}/msdir; \
     }
 else 
     [[ -z ${OD} ]] || \
  [[ ! -d ${WORKSPACE_ROOT}/${trname}/msdir ]] && { \
      CARATE_TEST_DATA_DIR=${CARATE_TEST_DATA_DIR_OLD}; \
  } || { \
      CARATE_TEST_DATA_DIR=${WORKSPACE_ROOT}/${trname}/msdir;\
  }
     [[ -n ${CARATE_TEST_DATA_DIR} ]] || { \
  echo "No test data to process"; \
  echo "This likely means that you have set the -od switch"; \
  echo "But there are no data in \${WORKSPACE_ROOT}/${trname}/msdir."; \
  echo "Stopping."; \
  }
     
     #Check if the test directory is a parent of any of the supplied directories
     if checkex ${WORKSPACE_ROOT}/${trname} && [[ -z ${KP} ]]
     then
  # Go through the files and remove individually
  # continue here
  dirs=(input msdir output stimela_parameter_files)
  for dire in ${dirs[@]}
  do
      checkex ${WORKSPACE_ROOT}/${trname}/${dire} || \
   echo "rm -rf \${workspace_root}/${trname}/${dire}" >> ${SS_RUNTEST}
      [[ -n ${FS} ]] || \
   checkex ${WORKSPACE_ROOT}/${trname}/${dire} || \
   rm -rf ${workspace_root}/${trname}/${dire}
  done
     else
  [[ -n ${FS} ]] || \
      [[ -n ${KP} ]] || { \
   echo "rm -rf \${workspace_root}/${trname}" >> ${SS_RUNTEST}; \
   rm -rf ${WORKSPACE_ROOT}/${trname}; \
      }
     fi 
 fi
        echo "Preparing ${contarch} test (using ${configfilename}.yml) in"
        echo "${WORKSPACE_ROOT}/${trname}"
 echo "mkdir -p \${workspace_root}/${trname}/msdir" >> ${SS_RUNTEST}
        mkdir -p ${WORKSPACE_ROOT}/${trname}/msdir
 if [[ -d ${CARATE_INPUT_DIR} ]]
 then
     echo "mkdir -p \${workspace_root}/${trname}/input" >> ${SS_RUNTEST}
     mkdir -p ${WORKSPACE_ROOT}/${trname}/input
     checkex ${WORKSPACE_ROOT}/${trname}/input || \
  [[ -n ${KP} ]] || { \
      echo "cp -r \${input_dir}/* ${workspace_root}/${trname}/input/"; \
      cp -r ${CARATE_INPUT_DIR}/* ${WORKSPACE_ROOT}/${trname}/input/; \
  }
 fi
 # Check if user-supplied file is already the one that we are working with before working with it
 # This should in principle only affect the time stamps as if the dataid is not empty, the following
 # Would do nothing in the config file itself
 checkex ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml || { \
     [[ -n ${KC} ]] || echo "sed \"s/dataid: \[.*\]/$dataidstr/\" ${configlocationstring} > \${workspace_root}/${trname}/${configfilename}.yml" >> ${SS_RUNTEST}; \
     [[ -z ${KC} ]] || echo "cp ${configlocationstring} \${workspace_root}/${trname}/${configfilename}.yml" >> ${SS_RUNTEST}; \
 }

 [[ -n ${FS} ]] || \
     checkex ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml || { \
  [[ -n ${KC} ]] || sed "s/dataid: \[.*\]/$dataidstr/" ${configlocation} > ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml; \
  [[ -z ${KC} ]] || cp ${configlocationstring} \${workspace_root}/${trname}/${configfilename}.yml; \
     }

 # This prevents the script to stop if there -fs is switched on
 [[ ! -f ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml ]] || \
     cp ${WORKSPACE_ROOT}/${trname}/${configfilename}.yml ${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}${reportprefy}.yml.txt

 # Check if source msdir is identical to the target msdir. If yes, don't copy
 d=`stat -c %i ${CARATE_TEST_DATA_DIR}`
 e=`stat -c %i ${WORKSPACE_ROOT}/${trname}/msdir`
 
 [[ $d == $e ]] || { \
     [[ -n ${MD} ]] && { \
  echo "mv ${stringcopytestdatastr} \${workspace_root}/${trname}/msdir/" >> ${SS_RUNTEST}; \
  [[ -n ${FS} ]] || \
      mv ${copytestdatastr} ${WORKSPACE_ROOT}/${trname}/msdir/ ; \
     } || { \
  echo "cp -r ${stringcopytestdatastr} \${workspace_root}/${trname}/msdir/" >> ${SS_RUNTEST}; \
  [[ -n ${FS} ]] || \
      cp -r ${copytestdatastr} ${WORKSPACE_ROOT}/${trname}/msdir/ ; \
     } \
 }
 
 # Now run the test
        echo "Running ${contarch} test (using ${configfilename}.yml)"
 echo "cd \${workspace_root}/${trname}" >> ${SS_RUNTEST}
        cd ${WORKSPACE_ROOT}/${trname}
 
        # Notice that currently all output will be false, such that || true is required to ignore this
 failed=0
 echo caracal -c ${configfilename}.yml ${caracalswitches} || true

 # Report CARACal start time
        sya="${trname} starting to run CARACal:"; sya+=$'\n'; sya+=`date -u`;
        echo "${sya}" >> ${SYA_RUNTEST}

 echo "caracal -c ${configfilename}.yml ${caracalswitches}" >> ${SS_RUNTEST}
 [[ -n ${FS} ]] || caracal -c ${configfilename}.yml ${caracalswitches} || { true; failedrun=1; }
    fi

    sya="${trname} CARACal run end time:"; sya+=$'\n'; sya+=`date -u`
    echo "${sya}" >> ${SYA_RUNTEST}
#    echo "" >> ${SYA}
#    echo "" >> ${SYA_RUNTEST}

    if [[ ${failedrun} == 0 ]]
    then
 mes="CARACal run ${trname} did not return an error."
 echo ${mes}
 echo ${mes} >> ${SYA_RUNTEST}
 echo ${mes} >> ${SYA}
    else 
 mes="CARACal run ${trname} returned an error."
 echo ${mes}
 echo ${mes} >> ${SYA}
 echo ${mes} >> ${SYA_RUNTEST}
    fi
#    echo "" >> ${SYA}
#    echo "" >> ${SYA_RUNTEST}
    
    # Make a copy of the logfile
    caracallog=`[[ -e ${WORKSPACE_ROOT}/${trname}/output/logs ]] && ls -t ${WORKSPACE_ROOT}/${trname}/output/logs/log-caracal*.txt | head -1 || echo ""`
    [[ ! -f ${caracallog} ]] || cp ${caracallog} ${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}${reportprefy}-log-caracal.txt
    echo "Checking output of ${configfilename} ${contarch} test"
    failedoutput=0
    testingoutput ${WORKSPACE_ROOT} ${trname} ${SYA_RUNTEST} || { true; failedoutput=1; }

    #    failedoutput=$?
    if [[ ${failedoutput} == 0 ]]
    then
 mes="CARACal run ${trname} did not return a faulty output."
 echo ${mes}
 echo ${mes} >> ${SYA_RUNTEST}
    else 
	mes="CARACal run ${trname} returned a faulty output."
	
	echo ${mes}
	echo ${mes} >> ${SYA_RUNTEST}
	echo ${mes} >> ${SYA}
    fi
    #    echo ""
    #    echo "" >> ${SYA_RUNTEST}
    #    echo "" >> ${SYA}
    
    [[ ! -e ${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}-badlogs ]] || rm -r ${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}-badlogs

    if (( ${failedrun} == 1 || ${failedoutput} == 1 ))
    then
        echo "" >> ${SYA_RUNTEST}
        echo "############" >> ${SYA_RUNTEST}
        echo "" >> ${SYA_RUNTEST}
        echo "Test failed." >> ${SYA_RUNTEST}
	
	# Collect all files that indicate an error and dump them into the report directory
	[[ -d ${WORKSPACE_ROOT}/${trname}/output/logs/ ]] && faultylist=( `grep -l ERROR ${WORKSPACE_ROOT}/${trname}/output/logs/*` ) || faultylist=()
	if (( ${#faultylist[@]} > 0 ))
	then
	    mkdir -p ${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}-badlogs
	    rm -f ${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}-badlogs/*
	    for jj in "${faultylist[*]}"
	    do
		cp ${jj} ${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}-badlogs/
	    done
	    echo "Logfiles indicating strange behaviour have been collected in the directory"
	    echo "${WORKSPACE_ROOT}/report/${reportname}/${reportname}${CARATE_CARACAL_RUN_MEDIFIX}-badlogs/"
	    echo ""
	fi
	
        kill "$PPID"
        exit 1
    else
        echo "" >> ${SYA_RUNTEST}
	echo "###############" >> ${SYA_RUNTEST}
	echo "" >> ${SYA_RUNTEST}
	echo "Test succeeded." >> ${SYA_RUNTEST}
    fi
    
    # Size of test
    echo "" >> ${SYA_RUNTEST}
    echo "###############" >> ${SYA_RUNTEST}
    echo "" >> ${SYA_RUNTEST}
    outsize=`du -ms ${WORKSPACE_ROOT}/${trname} | awk '{print $1}'`
    echo "Final test folder size (${trname}): ${outsize} MB" >> ${SYA_RUNTEST}
    
    echo "" >> ${SYA_RUNTEST}
    echo "##########################################" >> ${SYA_RUNTEST}
    echo "" >> ${SYA_RUNTEST}
    sya="End time for this test: "; sya+=`date -u`;
    echo "${sya}" >> ${SYA_RUNTEST}
    
    echo "" >> ${SYA_RUNTEST}
    echo "##########################################" >> ${SYA_RUNTEST}

    # This should be at the end of a script even if we don't do it now
    # notice that this is only added to the shell script output.
    if [[ -z ${HO} ]]
       if [[ -n ${HOME_OLD} ]]
       then
	   if [[ -z ${HOME_del} ]]
	   then
               echo "export HOME=\${HOME_OLD}" >> ${SS_RUNTEST}
	   else
	       echo "unset HOME" >> ${SS_RUNTEST}
	   fi
       fi
    fi
       
    if [[ -n ${PYTHONPATH_OLD} ]]
    then
	if [[ -z ${PYTHONPATH_del} ]]
	then
            echo "export PYTHONPATH=\${PYTHONPATH_OLD}" >> ${SS_RUNTEST}
	else
	    [[ -z ${PYTHONPATH} ]] || \
		{ \
		  echo "unset PYTHONPATH" >> ${SS_RUNTEST} ; \
		}
	fi
    fi
    
    if (( ${failedrun} == 1 || ${failedoutput} == 1 ))
    then
        kill "$PPID"
        exit 1
    fi
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
 (( ${testruns} == 1 )) && trname=${CARATE_CARACAL_RUN_PREFIX} || trname=${CARATE_CARACAL_RUN_PREFIX}-cs-${testruns}
    else
 trname="config_sample-${contarch}"
    fi

    echo "" >> ${SYA}
    echo "##########################################" >> ${SYA}
    echo "" >> ${SYA}
    sya="${trname} preparation start time:";sya+=$'\n'; sya+=`date -u`;
    echo "${sya}" >> ${SYA}

    if [[ -e ${WORKSPACE_ROOT}/${trname} ]]
    then
 if (( ${FORCE} == 0 ))
 then
            echo "Will not re-create existing directory ${WORKSPACE_ROOT}/${trname}"
 else
     true
 fi
    else
 if (( ${FORCE} == 0 ))
 then
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
     fi     
 else
     echo "rm -rf \${workspace_root}/${trname}" >> ${SS}
            [[ -n ${FS} ]] || \
  rm -rf ${WORKSPACE_ROOT}/${trname}
 fi
    fi

    echo "Preparing ${contarch} test (using ${configfilename}.yml) in"
    echo "${WORKSPACE_ROOT}/${trname}"
    echo "mkdir -p \${workspace_root}/${trname}/msdir" >> ${SS}
    mkdir -p ${WORKSPACE_ROOT}/${trname}/msdir
    if [[ -d ${CARATE_INPUT_DIR} ]]
    then
 echo "mkdir -p \${workspace_root}/${trname}/input" >> ${SS}
 mkdir -p ${WORKSPACE_ROOT}/${trname}/input
 checkex ${WORKSPACE_ROOT}/${trname}/input || { \
     echo "cp -r \${CARATE_INPUT_DIR}/* \${workspace_root}/${trname}/input/" >> ${SS}; \
     cp -r ${CARATE_INPUT_DIR}/* ${WORKSPACE_ROOT}/${trname}/input/; \
     }
    fi
    # Check if source msdir is identical to the target msdir. If yes, don't copy
    d=`stat -c %i ${CARATE_TEST_DATA_DIR}`
    e=`stat -c %i ${WORKSPACE_ROOT}/${trname}/msdir`
    [[ $d == $e ]] || { \
 [[ -n ${MD} ]] && { \
     echo "mv \${test_data_dir}/*.ms \${workspace_root}/${trname}/msdir/" >> ${SS}; \
     [[ -n ${FS} ]] || \
      mv ${CARATE_TEST_DATA_DIR}/*.ms ${WORKSPACE_ROOT}/${trname}/msdir/ ; \
 } || { \
     echo "cp -r \${test_data_dir}/*.ms \${workspace_root}/${trname}/msdir/" >> ${SS}; \
     [[ -n ${FS} ]] || \
  cp -r ${CARATE_TEST_DATA_DIR}/*.ms ${WORKSPACE_ROOT}/${trname}/msdir/ ; \
 } \
    }

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
#    echo "" >> ${SYA}
}

# Test run number
testrunnumber=0

#container_ss=${ss_docker}
#container_sya=${sya_docker}

if [[ -n ${DM} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Docker: minitestConfig"
    confilename="minitestConfig"
    contarch="docker"
    caracalswitches="-ct docker ${stimela_ns}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/caracal/caracal/sample_configurations/${confilename}.yml" "\{workspace_root}/caracal/caracal/sample_configurations/${confilename}.yml" "${testrunnumber}" "${sya_docker}" "${ss_docker}" "${caracalswitches}"
fi

if [[ -n ${DA} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Docker: (alternative) carateConfig"
    confilename="carateConfig"
    contarch="docker"
    caracalswitches="-ct docker ${stimela_ns}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/caracal/caracal/sample_configurations/${confilename}.yml" "\${workspace_root}/caracal/caracal/sample_configurations/${confilename}.yml" "${testrunnumber}" "${sya_docker}" "${ss_docker}" "${caracalswitches}"
fi

if [[ -n ${DI} ]] && [[ -n ${configfilename} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Docker: $configfilename"
    confilename=$configfilename
    contarch="docker"
    caracalswitches="-ct docker ${stimela_ns}"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${CARATE_CONFIG_SOURCE}" "\${config_source}" "${testrunnumber}" "${sya_docker}" "${ss_docker}" "${caracalswitches}"
fi

if [[ -n ${DSC} ]]
then
    (( testrunnumber+=1 ))
    echo "####################################"
    echo " Testing all sample configurations "
    echo "####################################"
    greetings_line="Docker: Testing Sample configurations"
    contarch="docker"
    caracalswitches="-ct docker ${stimela_ns}"
    # First we need to know all the sample configurations present that are not old
    if [[ -n "$CARATE_LOCAL_CARACAL" ]]
    then
      echo "Checking the configurations in ${CARATE_LOCAL_CARACAL}/caracal/sample_configurations/"
      sample_location="${CARATE_LOCAL_CARACAL}/caracal/sample_configurations"
    else
      echo "You are checking the configurations in the remote master"
      echo "That seems silly but ok."
      echo ""
      sample_location="${WORKSPACE_ROOT}/caracal/caracal/sample_configurations"
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

sya_sing=""
ss_sing=""

if [[ -n $SM ]] || [[ -n $SA ]] || [[ -n $SI ]] || [[ -n $SSC ]]
then
    # This sets the singularity image folder to the test environment, but it does not work correctly
    # Not only the cache is moved there but also the images and it gets all convolved.
    
    [[ -z ${SCH} ]] || { \
			 ss_sing+="export SINGULARITY_CACHEDIR=\${HOME}/.singularity/cache" ; \
			 ss_sing+=$'\n' ; \
			 ss_sing+="mkdir -p \${SINGULARITY_CACHEDIR}" ; \
			 ss_sing+=$'\n' ; \
			 export SINGULARITY_CACHEDIR=${HOME}/.singularity/cache ; \
			 mkdir -p ${SINGULARITY_CACHEDIR} ; \
	}
    
    [[ -z ${SLH} ]] || { \
			 ss_sing+="export SINGULARITY_LOCALCACHEDIR=\${HOME}/.singularity/localcache" ; \
			 ss_sing+=$'\n' ; \
			 ss_sing+="mkdir -p \${SINGULARITY_LOCALCACHEDIR}" ; \
			 ss_sing+=$'\n' ; \
			 export SINGULARITY_LOCALCACHEDIR=${HOME}/.singularity/localcache ; \
			 mkdir -p ${SINGULARITY_LOCALCACHEDIR} ; \
	}
    
    [[ -z ${SCW} ]] || { \
			 ss_sing+="export SINGULARITY_CACHEDIR=\${workspace}/.singularity/cache" ; \
			 ss_sing+=$'\n' ; \
			 ss_sing+="mkdir -p \${SINGULARITY_CACHEDIR}" ; \
			 ss_sing+=$'\n' ; \
			 export SINGULARITY_CACHEDIR=${CARATE_WORKSPACE}/.singularity/cache ; \
			 mkdir -p ${SINGULARITY_CACHEDIR} ; \
	}
    
    
    [[ -z ${SLW} ]] || { \
			 ss_sing+="export SINGULARITY_LOCALCACHEDIR=\${workspace}/.singularity/localcache" ; \
			 ss_sing+=$'\n' ; \
			 ss_sing+="mkdir -p \${SINGULARITY_LOCALCACHEDIR}" ; \
			 ss_sing+=$'\n' ; \
			 export SINGULARITY_LOCALCACHEDIR=${CARATE_WORKSPACE}/.singularity/localcache ; \
			 mkdir -p ${SINGULARITY_LOCALCACHEDIR} ; \
	}
    
    
    [[ -z ${STH} ]] || { \
			 ss_sing+="export SINGULARITY_TMPDIR=\${HOME}/.singularity/tmp" ; \
			 ss_sing+=$'\n' ; \
			 ss_sing+="mkdir -p \${SINGULARITY_TMPDIR}" ; \
			 ss_sing+=$'\n' ; \
			 export SINGULARITY_TMPDIR=${HOME}/.singularity/tmp ; \
			 mkdir -p ${SINGULARITY_TMPDIR} ; \
	}
    [[ -z ${STW} ]] || { \
			 ss_sing+="export SINGULARITY_TMPDIR=\${workspace}/.singularity/tmp" ; \
			 ss_sing+=$'\n' ; \
			 ss_sing+="mkdir -p \${SINGULARITY_TMPDIR}" ; \
			 ss_sing+=$'\n' ; \
			 export SINGULARITY_TMPDIR=${CARATE_WORKSPACE}/.singularity/tmp ; \
			 mkdir -p ${SINGULARITY_TMPDIR} ; \
	}
    
    if [[ -n ${SNC} ]]
    then
	ss_sing+="export SINGULARITY_DISABLE_CACHE=\"True\""
	ss_sing+=$'\n'
	export SINGULARITY_DISABLE_CACHE="True"
    else
	[[ -z ${SINGULARITY_DISABLE_CACHE} ]] || unset SINGULARITY_DISABLE_CACHE
    fi
    

    [[ -z ${CARATE_STIMELA_PULLFOLDER} ]] || { \
					       ss_sing+="export STIMELA_PULLFOLDER=\${CARATE_STIMELA_PULLFOLDER}" ; \
					       ss_sing+=$'\n' ; \
					       STIMELA_PULLFOLDERstring="\${CARATE_STIMELA_PULLFOLDER}" ; \
					       export STIMELA_PULLFOLDER=${CARATE_STIMELA_PULLFOLDER} ; \
	}
    
    [[ -z ${SPT} ]] || \
	{ \
	  ss_sing+="export STIMELA_PULLFOLDER=\${workspace_root}/stimela_pullfolder" ; \
	  ss_sing+=$'\n' ; \
	  STIMELA_PULLFOLDERstring="\${workspace_root}/stimela_pullfolder" ; \
	  export STIMELA_PULLFOLDER=${WORKSPACE_ROOT}/stimela_pullfolder ; \
	}
    
    [[ -z ${SPW} ]] || { \
			 ss_sing+="export STIMELA_PULLFOLDER=\${workspace}/stimela_pullfolder" ; \
			 ss_sing+=$'\n' ; \
			 STIMELA_PULLFOLDERstring="\${workspace}/stimela_pullfolder" ; \
			 export STIMELA_PULLFOLDER=${CARATE_WORKSPACE}/stimela_pullfolder ; \
	}

    if (( ${FORCE} == 0 )) || [[ -n ${ORSR} ]] || [[ -z ${SDP} ]]
    then
        if [[ -e ${STIMELA_PULLFOLDER} ]]
        then
            echo "Will not re-create existing stimela pullfolder and potentially re-use its content."
            echo "Use -f to override and unset -or or --omit-stimela-reinstall flags or use -sdp."
	    sya_sing+=$'\n'
            sya_sing+="##########################################"; sya_sing+=$'\n'
	    sya_sing+=$'\n'
            sya_sing+="Will not re-create existing stimela pullfolder and potentially re-use its content."; sya_sing+=$'\n'
            sya_sing+="Use -f to override and unset -or or --omit-stimela-reinstall flags or use -sdp."; sya_sing+=$'\n'
	    sya_sing+=$'\n'
            sya_sing+="##########################################"; sya_sing+=$'\n'
        fi
    else
	checkex ${STIMELA_PULLFOLDER} || \
	    { ss_sing+="rm -rf ${STIMELA_PULLFOLDERstring}"; ss_sing+=$'\n' ; }
        [[ -n ${FS} ]] || checkex ${STIMELA_PULLFOLDER} || \
	    checkex ${STIMELA_PULLFOLDER} || \
	    rm -rf ${STIMELA_PULLFOLDER}
    fi

    if [[ ! -e "${STIMELA_PULLFOLDER}" ]]
    then
        echo
        echo "##################################"
        echo " Creating Singularity pull folder "
        echo "##################################"
        echo
	
	echo "Installing Stimela images in ${STIMELA_PULLFOLDERstring}"
	ss_sing+="mkdir -p ${STIMELA_PULLFOLDERstring}"; ss_sing+=$'\n'
	mkdir -p ${STIMELA_PULLFOLDER}
    fi

    sya_sing+=$'\n';
    sya_sing+="##########################################"; sya_sing+=$'\n'
    sya_sing+=$'\n'
    singvers=`singularity --version`
    sya_sing+="Singularity version: ${singvers}"; sya_sing+=$'\n'
    
    if [[ -z ${NP} ]]
    then
        echo
        echo "#########################################"
        echo " Installing Stimela images (Singularity) "
        echo "#########################################"
        echo
	echo "Installing Stimela images in ${STIMELA_PULLFOLDERstring}"
	ss_sing+="mkdir -p ${STIMELA_PULLFOLDERstring}"; ss_sing+=$'\n'
	ii=1
	until (( ${ii} > ${IA} ))
	do
	    echo stimela pull --singularity${use_pull_force}
	    ss_sing+="stimela pull --singularity${use_pull_force}"; ss_sing+=$'\n'
	    if [[ -z ${FS} ]]
	    then
		stimela pull --singularity${use_pull_force} && break || {
			echo "stimela pull --singularity${use_pull_force} failed"
			(( ii++ ))
		    }
	    else
		break
	    fi  
	done
	if (( ${ii} > ${IA} ))
	then
	    echo "Maximum number of pull attempts for Stimela reached."
	    sya_sing+="Maximum number of pull attempts for Stimela reached."
	    sya_sing+=$'\n'
	    echo "${sya_sing}" >> ${SYA}
	    echo "${ss_sing}" >> ${SS}     
	    exit 1
	fi
    fi
    
    # Size of images
    outsize=`du -ms ${STIMELA_PULLFOLDER} | awk '{print $1}'`
    sya_sing+="Singularity image folder size: ${outsize} MB"; sya_sing+=$'\n'

fi

if [[ -n ${SM} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Singularity: minitestConfig-singularity"
    confilename="minitestConfig"
    contarch="singularity"
    caracalswitches="-ct singularity"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/caracal/caracal/sample_configurations/${confilename}.yml" "\${workspace_root}/caracal/caracal/sample_configurations/${confilename}.yml" "${testrunnumber}" "${sya_sing}" "${ss_sing}" "${caracalswitches}"
fi

if [[ -n ${SA} ]]
then
    (( testrunnumber+=1 ))
    greetings_line='Singularity: (alternative) carateConfig'
    confilename="carateConfig"
    contarch="singularity"
    caracalswitches="-ct singularity"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${WORKSPACE_ROOT}/caracal/caracal/sample_configurations/${confilename}.yml" "\${workspace_root}/caracal/caracal/sample_configurations/${confilename}.yml" "${testrunnumber}" "${sya_sing}" "${ss_sing}" "${caracalswitches}"
fi

if [[ -n ${SI} ]] && [[ -n ${configfilename} ]]
then
    (( testrunnumber+=1 ))
    greetings_line="Singularity: $configfilename"
    confilename=$configfilename
    contarch="singularity"
    caracalswitches="-ct singularity"
    runtest "${greetings_line}" "${WORKSPACE_ROOT}" "${confilename}" "${contarch}" "${FORCE}" "${CARATE_CONFIG_SOURCE}" "\${config_source}" "${testrunnumber}" "${sya_sing}" "${ss_sing}" "${caracalswitches}"
fi

if [[ -n ${SSC} ]]
then
    (( testrunnumber+=1 ))
    echo "####################################"
    echo " Testing all sample configurations "
    echo "####################################"
    greetings_line="Singularity: Testing Sample configurations"
    contarch="singularity"
    caracalswitches="-ct singularity"
    # First we need to know all the sample configurations present that are not old
    if [[ -n "$CARATE_LOCAL_CARACAL" ]]
    then
      echo "Checking the configurations in ${CARATE_LOCAL_CARACAL}/caracal/sample_configurations/"
      sample_location="${CARATE_LOCAL_CARACAL}/caracal/sample_configurations"
    else
      echo "You are checking the configurations in the remote master"
      echo "That seems silly but ok."
      echo ""
      sample_location="${WORKSPACE_ROOT}/caracal/caracal/sample_configurations"
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
