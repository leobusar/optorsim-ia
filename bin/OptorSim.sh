#!/bin/bash

# Parse options:
#   --jmp  use jmp profiling
#   --     stop all further cmd-line processing
while [ $# -gt 0 ]; do 
  case "$1" in
    # use jmp profiling.
    --jmp)
      JMP="-Xrunjmp:"
      shift
      ;;

    # skip the "--" and stop processing cmd-line options.
    --)
      shift
      break
      ;;

    # An unknown option, stop processing and pass everything to OptorSim
    *)
      break;
  esac
done  

# Make sure we have the simulation base set to the correct place.
SIMULATION_BASE="$(dirname $0)"/..
SIMULATION_BASE="$(cd $SIMULATION_BASE;pwd)"

OPTORSIM_CLASSPATH=$SIMULATION_BASE/lib/optorsim.jar:$SIMULATION_BASE/external-lib/jcommon-0.9.5.jar:$SIMULATION_BASE/external-lib/jfreechart-0.9.20.jar

java $JMP -classpath $OPTORSIM_CLASSPATH org.edg.data.replication.optorsim.OptorSimMain $*

