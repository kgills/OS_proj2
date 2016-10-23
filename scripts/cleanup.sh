#!/bin/bash

# Change this to your netid
netid=khg140030

#
# Root directory of your project
PROJDIR=$HOME/Workspace/OS_proj2

#
# This assumes your config file is named "config.txt"
# and is located in your project directory
#
CONFIG=$PROJDIR/config_files/config0.txt

n=1

cat $CONFIG | sed -e "s/#.*//" | sed -e "/^\s*$/d" | grep "\s*[0-9]\+\s*\w\+.*" |
(
    # read i
    # echo $i
    while read line 
    do
        host=$( echo $line | awk '{ print $2 }' )

        echo $host
        ssh $netid@$host killall -u $netid &
        sleep 1

        n=$(( n + 1 ))
    done
   
)


echo "Cleanup complete"
