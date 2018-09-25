#!/usr/bin/env bash
if [ "$#" -ne 1 ] || { [ $1 != "CountFileHits" ] && [ $1 != "CountIpHits" ] && [ $1 != "MaxHitFile" ]; }; then
    echo "Please provide a argument"
    echo "$0 [CountFileHits | CountIpHits | MaxHitFile]"
    echo "CountFileHits for question 1"
    echo "CountIpHits for question 2"
    echo "MaxHitFile for question 3"
    exit 1;
fi

if [ -d "./output" ]; then
        rm -rf output
fi
hadoop fs -test -d /p1/output
if [ $? == 0 ]; then
        hadoop fs -rm -r -f /p1/output
fi
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* $1.java
jar cf lg.jar $1*.class
hadoop jar lg.jar $1 /p1/input /p1/output
hadoop fs -get /p1/output

case $1 in
"CountFileHits")
    echo " Answer for question 1: `grep \"/assets/js/the-associates.js\" output/part*`"
    ;;
"CountIpHits")
    echo "Answer for question 2: `grep \"10.99.99.186\" output/part*`"
    ;;
"MaxHitFile")
    echo "run \"cat output/part*\""
    ;;
esac
