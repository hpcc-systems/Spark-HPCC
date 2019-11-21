#!/bin/bash
#
# Automatically tag an existing release candidate build as gold
#

set -x

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_pom.sh

sync_git
parse_pom

NEW_SEQUENCE=1

if [ "$POM_MATURITY" != "SNAPSHOT" ] ; then
  if [ -n "$POM_MATURITY" ] ; then
    NEW_SEQUENCE=$((POM_MATURITY+1))
    if [ -z "$RETAG" ] ; then
      echo "Current version is already at release level. Specify --retag to create $POM_MAJOR.$POM_MINOR.$POM_POINT-$NEW_SEQUENCE"
      exit 2
    fi
  else
    echo "Current version should be at SNAPSHOT level to go gold"
    exit 2
  fi
fi
if (( "$POM_POINT" % 2 == 1 )) ; then
  echo "Current version should have even point version to go gold"
  exit 2
fi
if [ "$GIT_BRANCH" != "candidate-$POM_MAJOR.$POM_MINOR.$POM_POINT" ]; then
  echo "Current branch should be candidate-$POM_MAJOR.$POM_MINOR.$POM_POINT"
  exit 2
fi

set_tag
if [ $(git rev-parse HEAD) != $(git rev-parse $HPCC_LONG_TAG) ] ; then 
  if [ -z "$IGNORE" ] ; then
    git diff $HPCC_LONG_TAG
    echo "There are changes on this branch since $HPCC_LONG_TAG. Use --ignore if you still want to tag Gold"
    exit 2
  fi
fi

update_version_file $NEW_SEQUENCE $POM_POINT
POM_MATURITY=$NEW_SEQUENCE
set_tag

# Commit the change
doit "git commit -s -a -m \"$HPCC_NAME $HPCC_SHORT_TAG Gold\""
doit "git push $REMOTE $GIT_BRANCH $FORCE"

# tag it
do_tag
