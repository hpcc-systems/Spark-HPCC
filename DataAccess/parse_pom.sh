#!/bin/bash
#
# Common utilities used by tools for automating tagging and release
#

#We want any failures to be fatal
# Depends: maven

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

REMOTE=origin
FORCE=
DRYRUN=
IGNORE=
RETAG=
VERBOSE=
VERSIONFILE=pom.xml

POSITIONAL=()
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    -f|--force)
    FORCE=-f
    shift
    ;;
    -i|--ignore)
    IGNORE=$1
    shift
    ;;
    -v|--verbose)
    VERBOSE=$1
    shift
    ;;
    --retag)
    RETAG=$1
    shift
    ;;
    -d|--dryrun)
    DRYRUN=$1
    shift
    ;;
    -r|--remote)
    if [ $# -eq 1 ] ; then
      echo "$1 option requires an argument"
      exit 2
    fi
    REMOTE="$2"
    shift 
    shift
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
  esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [ "$#" -ge 1 ] ; then
  VERSIONFILE=$1
  shift 1
fi
if [ ! -f $VERSIONFILE ]; then
  echo "File $VERSIONFILE not found"
  exit 2
fi

if ! `which mvn 1>/dev/null 2>&1` ; then
  echo "Maven dependency not located"
  exit 2
fi

GIT_BRANCH=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)

function parse_pom()
{
  POM_PROJECT=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
  POM_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
  POM_MAJOR=$(echo $POM_VERSION | awk 'BEGIN {FS="[.-]"}; {print $1};')
  POM_MINOR=$(echo $POM_VERSION | awk 'BEGIN {FS="[.-]"}; {print $2};')
  POM_POINT=$(echo $POM_VERSION | awk 'BEGIN {FS="[.-]"}; {print $3};')
  POM_MATURITY=$(echo $POM_VERSION | awk 'BEGIN {FS="[.-]"}; {print $4};')
}

function doit()
{
    if [ -n "$VERBOSE" ] || [ -n "$DRYRUN" ] ; then echo $1 ; fi
    if [ -z "$DRYRUN" ] ; then eval $1 ; fi
}

function set_tag()
{
    if [ -n "$POM_MATURITY" ] ; then
      local _maturity=-$POM_MATURITY
    fi
    HPCC_SHORT_TAG=$POM_MAJOR.$POM_MINOR.$POM_POINT$_maturity
    HPCC_LONG_TAG=${POM_PROJECT}_$HPCC_SHORT_TAG
}

function update_version_file()
{
    # Update the pom.xml file
    local _new_maturity=$1
    local _new_point=$2
    local _new_minor=$3
    if [ -z "$_new_minor" ] ; then
      _new_minor=$POM_MINOR
    fi
    local _v="$POM_MAJOR.$_new_minor.$_new_point-$_new_maturity"
    local mvn_version_update_cmd="mvn versions:set -DnewVersion=$_v"
    if [ -n "$VERBOSE" ] ; then
      echo  "$mvn_version_update_cmd"
    fi
    if [ -z "$DRYRUN" ] ; then 
      eval "$mvn_version_update_cmd"
    else
      echo  "Update to $_v"
    fi
}

function do_tag()
{
    set_tag
    if [ "$FORCE" = "-f" ] ; then
      doit "git tag -d $HPCC_LONG_TAG"
    fi
    doit "git tag $HPCC_LONG_TAG"
    doit "git push $REMOTE $HPCC_LONG_TAG $FORCE"
}

function sync_git()
{
    doit "git fetch $REMOTE"
    doit "git merge --ff-only $REMOTE/$GIT_BRANCH"
}
