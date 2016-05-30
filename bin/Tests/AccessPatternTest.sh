#!/bin/bash

# Functional tests for the 3 access pattern generators used in OptorSim.
# 1 job is submitted and the files it uses are analysed.

# Preload useful functions
SIMULATION_BASE="$(dirname $0)"/../..
SIMULATION_BASE="$(cd $SIMULATION_BASE;pwd)"
. $SIMULATION_BASE/bin/Tests/functions

##  This function extracts a list of jobFiles for job1_1 and returns them
##  as a series of space-seperated numbers in std-out.  Takes a single
##  argument: the output file.
function extractFileNumbers {

  #  First sed pulls out everything between "job1_1=[" and "]"
  #  Second sed removes all "File"s and ","s, which leaves us with a
  #     list of numbers.
  extractAttribute CE1@Site0 jobFiles $1 | \
     sed -e 's/.*job1_1=\[\([^]]*\)\].*/\1/' | \
     sed -e 's/File//g;s/,//g'
}

# --------------------------- SEQUENTIAL ------------------------------------------------
STATUS=PASS
echo "Testing Access Pattern Generators...Sequential"
TMP_FILE=/tmp/optorsim-$$
runOptorSim accesstest_seq.conf $TMP_FILE
if [ $? -eq 0 ]
then
	# Strip out from the log file the list of files used by the job and test them.
	FILES=$(extractFileNumbers $TMP_FILE)
	evaluate "$FILES" "1 2 3 4 5 6 7 8 9 10" "Sequential access pattern"
	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi
fi
# ----------------------------------- GAUSSIAN -------------------------------------------

DIFF_FILE=/tmp/diffs-$$
echo "Testing Access Pattern Generators...Gaussian"
runOptorSim accesstest_gauss.conf $TMP_FILE
if [ $? -eq 0 ]
then
	## Store a list of differences in file numbers between successive file requests
	extractFileNumbers $TMP_FILE  | \
	   awk '{for(i=1; i < NF; i++)print $(i+1)-$i;}' > $DIFF_FILE

	# Find the mean step size and its standard deviation.
	MEAN=$(awk 'BEGIN{s=0;}{s+=$1;}END{print s/NR;}' $DIFF_FILE)
	STDEV=$(awk "BEGIN{s=0;}{s+=((\$1-($MEAN))^2);}END{dev=sqrt(s/(NR-1)); print dev;}" $DIFF_FILE)
	rm $DIFF_FILE
	
	# Mean step size should be zero.
	# Standard deviation should be 0.5*(no of files) (=50 for fileset of 100), but
	# modified slightly by 'wrap-around' of indices to give ~40.

	TEST_FLAG=`echo "if($MEAN < -1 || $MEAN > 1 || $STDEV < 30 || $STDEV > 50) print 1 else print 0;"| bc`
	evaluate $TEST_FLAG 0 "Gaussian access pattern"

	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi
fi
# -------------------------------- ZIPF ----------------------------------------

echo "Testing Access Pattern Generators...Zipf"
runOptorSim accesstest_zipf.conf $TMP_FILE
if [ $? -eq 0 ]
then
	# For each file index, loop through the list and find how many times it occurs.
	# Then sort in decreasing order to give rankings, which are written to a file.
	RANK_FILE=/tmp/rankings-$$
	echo $(extractFileNumbers $TMP_FILE | awk '{for(i=1;i<=NF;i++)print $i;}' | \
	   sort | uniq -c | awk '{print $1}' | sort -rn) > $RANK_FILE

	# The Zipf parameter, alpha, is then found by performing a maximum likelihood fit
	# (min chi^2) on the equation ln(P) = -(alpha)ln(n) + beta, where P is the probability
	# of getting the file with rank n.

	NUM_FILES=750
	ALPHA=$(awk "BEGIN{ i=1; S=0; Sx=0; Sy=0; Sxx=0; Sxy=0; }{
			while (i<=NF) {
                           log_i=log(i);
                           log_j=log((\$i)/$NUM_FILES);
			   S+=1;
			   Sx+=log_i;
			   Sy+=log_j;
			   Sxx+=log_i*log_i;
			   Sxy+=log_i*log_j;
             	           i=i+1;
			}	
		}END {  
		DELTA=S*Sxx-Sx*Sx+0.01;
		alpha=( (-Sxx*Sy + Sx*Sxy)/DELTA);
                print alpha;
        }" $RANK_FILE)
 
	# Ideally alpha is about 0.85 but actual result fluctuates. Pass anything 
	# between 0.5 and 1, which is reasonable according to literature.

	ALPHA_FLAG=`echo "if($ALPHA < 0.5 || $ALPHA > 1) print 1 else print 0;" | bc`
#	echo $ALPHA
	evaluate $ALPHA_FLAG 0 "Zipf-like access pattern"
	
	if [ $? -ne 0 ]
		then STATUS=FAIL
		incrementFailCounter
	fi
	# clean up
	rm $TMP_FILE
	rm $RANK_FILE
fi

exit "$Num_Run$Num_Fail"
