#!/bin/bash

# Functional test for the 4 scheduling algorithms used in OptorSim.

# Preload useful functions
SIMULATION_BASE="$(dirname $0)"/../..
SIMULATION_BASE="$(cd $SIMULATION_BASE;pwd)"
. $SIMULATION_BASE/bin/Tests/functions

TMP_FILE=/tmp/optorsim-$$

# run and evaluate schedulers
echo "Testing Scheduling Algorithms...Random"
runOptorSim schedtest_random.conf $TMP_FILE
if [ $? -eq 0 ]
then
	SITES=$(getSchedulerSites $TMP_FILE)
	evaluate $SITES "6;1;5;9;2;7;7;9;4;4;" "Random scheduler"
	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi
fi

echo "Testing Scheduling Algorithms...Queue Length"
runOptorSim schedtest_qlength.conf $TMP_FILE
if [ $? -eq 0 ]
then
	SITES=$(getSchedulerSites $TMP_FILE)
	evaluate $SITES "10;2;2;6;7;9;4;5;1;1;" "Queue Length scheduler"
	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi
fi

echo "Testing Scheduling Algorithms...Access Cost"
runOptorSim schedtest_acc_cost.conf $TMP_FILE
if [ $? -eq 0 ]
then
	SITES=$(getSchedulerSites $TMP_FILE)
	evaluate $SITES "1;1;1;1;1;1;1;1;1;1;" "Access Cost scheduler"
	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi
fi

echo "Testing Scheduling Algorithms...Queue Access Cost"
runOptorSim schedtest_q_acc_cost.conf $TMP_FILE
if [ $? -eq 0 ]
then
	SITES=$(getSchedulerSites $TMP_FILE)
	evaluate $SITES "1;1;2;1;2;1;2;1;1;2;" "Queue Access Cost scheduler"
	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi

	# clean up
	rm $TMP_FILE
fi

exit "$Num_Run$Num_Fail"
