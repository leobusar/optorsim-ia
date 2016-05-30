#!/bin/bash

# Functional test for network routing. See configuration file for
# grid layout. Files should be transferred directly from Site1 to 
# Site0 without going through Site2.

# Preload useful functions
SIMULATION_BASE="$(dirname $0)"/../..
SIMULATION_BASE="$(cd $SIMULATION_BASE;pwd)"
. $SIMULATION_BASE/bin/Tests/functions

echo "Testing Network Infrastructure..."

TMP_FILE=/tmp/optorsim-$$

runOptorSim networktest.conf $TMP_FILE
if [ $? -eq 0 ]
then

	# Check that no files have gone via Site2.
	ROUTED_FILES=$(extractAttribute Site2 routedFiles $TMP_FILE)
	evaluate "$ROUTED_FILES" 0 "Network routing"

	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi
	rm $TMP_FILE
fi

exit "$Num_Run$Num_Fail"
