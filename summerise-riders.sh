#!/bin/bash
#
# Give me a summary of riders for a summary file & checksums
#
#
#
if [  -z "$1" ] ; then
	echo "Please pass me the WMCCL base input file"
	exit 5
fi

inputfile=$1

if [ ! -f $inputfile ] ; then
	echo Cannot fine $inputfile!
	exit 5
fi

Women=`egrep "^Senior/Masters Female|^Junior Female" $inputfile | wc -l`
U6=`egrep "^Under 6" $inputfile | wc -l`
U8=`egrep "^Under 8" $inputfile | wc -l`
U10=`egrep "^Under 10" $inputfile | wc -l`
U12=`egrep "^Under 12" $inputfile | wc -l`
Youth=`egrep "^Youth" $inputfile | wc -l`
Senior=`egrep "^Junior Open|Senior Open|Masters 40-49" $inputfile | wc -l`
Vet50=`egrep "^Masters 50+" $inputfile | wc -l`
Total=`cat $inputfile | wc -l `
Total=$((Total-1)) # Remove header!


echo Race Summary - Categories unverified
echo ============
echo
echo U6 riders  : $U6
echo U8 riders  : $U8
echo U10 riders : $U10
echo U12 riders : $U12
echo Youth riders : $Youth
echo

echo Senior riders : $Senior
echo Vet50 riders  : $Vet50
echo Women riders  : $Women
echo
echo Total riders  : $Total

exit 0
