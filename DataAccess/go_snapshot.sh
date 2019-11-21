#!/bin/bash
#
# Automatically tag the first rc for a new point release
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_pom.sh

sync_git
parse_pom

# must create new SNAPSHOT branches from major.minor.x branches
# which will always be at SNAPSHOT maturity
# point release for the major.minor.x branch will always be odd num
# except in new major/minor release

if [ "$POM_MATURITY" = "SNAPSHOT" ] ; then
  if (( "$POM_POINT" % 2 != 1 )) ; then
    if [ "$POM_POINT" = "0" ] ; then
      # special case when creating new minor release
      NEW_POINT=0
      if (( "$POM_MINOR" % 2 == 1 )) ; then
        NEW_MINOR=$((POM_MINOR+1))
      else
        echo "A closedown version should have an odd point or minor version to create a new rc"
        exit 2
      fi
    else
      echo "A closedown version should have an odd point version to create a new rc"
      exit 2
    fi
  else
    NEW_POINT=$((POM_POINT+1))
  fi
  if [ "$GIT_BRANCH" != "candidate-$POM_MAJOR.$POM_MINOR.x" ]; then
    echo "Current branch should be candidate-$POM_MAJOR.$POM_MINOR.x"
    exit 2
  fi
  doit "git checkout -b candidate-$POM_MAJOR.$POM_MINOR.$NEW_POINT"
  doit "git checkout $GIT_BRANCH"
  update_version_file SNAPSHOT $((NEW_POINT+1)) $NEW_MINOR
  doit "git commit -s -a -m \"Split off $POM_MAJOR.$POM_MINOR.$NEW_POINT\""
  doit "git push $REMOTE"
  GIT_BRANCH=candidate-$POM_MAJOR.$POM_MINOR.$NEW_POINT
  doit "git checkout $GIT_BRANCH"
else
  echo "Current branch should have SNAPSHOT maturity"
  exit 2
fi

update_version_file SNAPSHOT $NEW_POINT $NEW_MINOR
POM_MATURITY=SNAPSHOT
POM_POINT=$NEW_POINT
set_tag

# Commit the change
doit "git commit -s -a -m \"$POM_PROJECT $HPCC_SHORT_TAG Release Candidate $HPCC_SEQUENCE\""
doit "git push $REMOTE $GIT_BRANCH $FORCE"

# tag it
do_tag
