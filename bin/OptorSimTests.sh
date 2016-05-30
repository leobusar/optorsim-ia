#!/bin/bash

# A suite of functional tests for OptorSim

echo "OptorSim Functional Tests"
echo "Please wait..."

# Make sure we have the simulation base set to the correct place.
SIMULATION_BASE="$(dirname $0)"/..
SIMULATION_BASE="$(cd $SIMULATION_BASE;pwd)"
. $SIMULATION_BASE/bin/Tests/functions

RUN=0
FAILED=0

function runTest() {
        $SIMULATION_BASE/bin/Tests/$1
        result=$?
        if [[ $result -ne " " ]]
        then
                FAILED=$(($FAILED+${result:1}))
                RUN=$(($RUN+${result:0:1}))
        fi
}

runTest ReplicationTest.sh
runTest NetworkTest.sh
runTest AccessPatternTest.sh
runTest SchedulerTest.sh
runTest OptimiserTest.sh

echo -e "${blue}$RUN tests were run, of which $(($RUN-$FAILED)) passed.${normal}"

if [[ $FAILED -gt 0 ]]
then exit 1
fi
