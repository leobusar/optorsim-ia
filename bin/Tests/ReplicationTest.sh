#!/bin/bash

# Functional test for time to replicate one file. 
# Should be ~9 seconds, will pass anything between 8 and 10 s.

# Preload useful functions
SIMULATION_BASE="$(dirname $0)"/../..
SIMULATION_BASE="$(cd $SIMULATION_BASE;pwd)"
. $SIMULATION_BASE/bin/Tests/functions

echo "Testing Replication Time..."

TMP_FILE=/tmp/optorsim-$$

runOptorSim reptest.conf $TMP_FILE
if [ $? -eq 0 ]
then

	# Strip out the appropriate job time information.
	JOB_TIME=$(extractAttribute CE1@Site0 totalJobTime $TMP_FILE)

	# Check time taken is equal to 8 seconds.
	TIME_FLAG=`echo "if($JOB_TIME != 8 ) print 1 else print 0;"| bc`
	evaluate $TIME_FLAG 0 "Replication time"

	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi
	rm $TMP_FILE
fi
exit "$Num_Run$Num_Fail"
