#! /usr/bin/env bash
startdir="/data/"
files=`find "${startdir}uploaded/ctt" -type f \( -name 'CTT--*.gz' \)`
for VARIABLE in $files
do
   echo $VARIABLE
   afterdot=${VARIABLE#*CTT--}
   mv $VARIABLE "${startdir}rotated/CTT-$1-$afterdot"
done
