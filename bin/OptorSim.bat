@echo off

rem must be run from optorsim directory

java -classpath lib\optorsim.jar;external-lib\jcommon-0.9.5.jar;external-lib\jfreechart-0.9.20.jar org.edg.data.replication.optorsim.OptorSimMain %*


