#!/bin/bash

# Preload useful functions
SIMULATION_BASE="$(dirname $0)"/../..
SIMULATION_BASE="$(cd $SIMULATION_BASE;pwd)"
. $SIMULATION_BASE/bin/Tests/functions

TMP_FILE=/tmp/optorsim-$$
PASS="${green}PASS${normal}"
FAIL="${red}FAIL${normal}"

# -------------------------------- LFU -----------------------------------------------------
echo "Testing LFU Model..."
runOptorSim lfutest.conf $TMP_FILE
if [ $? -eq 0 ]
then

	STATUS=$PASS

	# Replication decision: check no. of replications 
	REPS=$(extractAttribute "the GridContainer" replications $TMP_FILE)
	evalSub "$REPS" 11 "Replication decision"
	if [ $? -ne 0 ]
	then STATUS=$FAIL
	fi

	# Replica selection: check none have been taken from Site2, which has lower bandwidth
	ACCESSES=$(extractAttribute Site2 fileAccesses $TMP_FILE)
	evalSub "$ACCESSES" 0 "Replica selection"
	if [ $? -ne 0 ]
	then STATUS=$FAIL
	fi

#	# File replacement: check which files are on the site at the end
#	FILES=$(awk '/containing/ { if(NF==10) print $0}' $TMP_FILE | sed -e 's/containing//' -e 's/\ //g')
#        echo $FILES
#	evalSub $FILES 'File9File8File7File6File5File4File3File2File10' "File replacement"
#	if [ $? -ne 0 ]
#	then STATUS=$FAIL
#	fi

	writeOutput "LFU model" $STATUS

	if [ $STATUS != $PASS ]
	then incrementFailCounter
	fi
fi
# ----------------------- Binomial economic model ---------------------------------------

echo "Testing Economic Model (Binomial)..."
runOptorSim ecobintest.conf $TMP_FILE
if [ $? -eq 0 ]
then
	STATUS=$PASS
	# Replication decision: a file should be deleted to replicate File10
	REPS=$(awk '/Deleting/ { print $11 }' $TMP_FILE)
	evalSub "$REPS" 'File10' "Replication decision"
	if [ $? -ne 0 ]
	then STATUS=$FAIL
	fi 

	# Replica selection: none should be replicated from Site2
	ACCESSES=$(extractAttribute Site2 fileAccesses $TMP_FILE)
	evalSub "$ACCESSES" 0 "Replica selection"
	if [ $? -ne 0 ]  
	then STATUS=$FAIL
	fi

	# File replacement: File1 should be deleted
	DELETED=$(awk '/Deleting/ { print $4 }' $TMP_FILE | sed 's/,//')
	evalSub "$DELETED" '[{File1' "File replacement"
	if [ $? -ne 0 ]
	then STATUS=$FAIL
	fi 

	writeOutput "Binomial economic model" $STATUS

	if [ $STATUS != $PASS ]
	then incrementFailCounter
	fi
fi
# ----------------------------- Zipf-based economic model ----------------------------------

echo "Testing Economic Model (Zipf-based)..."
runOptorSim zipfecotest.conf $TMP_FILE
if [ $? -eq 0 ]
then
	STATUS=$PASS
	# Replication decision: a file should be deleted to replicate File10
	REPS=$(awk '/Deleting/ { print $11 }' $TMP_FILE)
	evalSub $REPS 'File10' "Replication decision"
	if [ $? -ne 0 ]
	then STATUS=$FAIL
	fi 

	# Replica selection: none should be replicated from Site2
	ACCESSES=$(extractAttribute Site2 fileAccesses $TMP_FILE)
	evalSub "$ACCESSES" 0 "Replica selection"
	if [ $? -ne 0 ]  
	then STATUS=$FAIL
	fi

	# File replacement: File1 should be deleted
	DELETED=$(awk '/Deleting/ { print $4 }' $TMP_FILE | sed 's/,//')
	evalSub $DELETED '[{File1' "File replacement"
	if [ $? -ne 0 ]
	then STATUS=$FAIL
	fi 

	writeOutput "Zipf-based economic model" $STATUS
	
	if [ $STATUS != $PASS ]
	then incrementFailCounter
	fi
	rm $TMP_FILE
fi

exit "$Num_Run$Num_Fail"
