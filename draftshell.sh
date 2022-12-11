#!/bin/sh
declare -i recordcount #declare a variable, i to specify that it is an integer

recordcount=$(ls  . | wc -l )
echo $recordcount