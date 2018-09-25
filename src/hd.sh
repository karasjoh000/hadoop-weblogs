#!/usr/bin/env bash
if [ -d "./output" ]; then
        `rm -rf output`
fi
`hadoop fs -test -d /p1/output`
if [ $? == 0 ]; then
        `hadoop fs -rm -r -f /p1/output`
fi
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* $1.java
jar cf lg.jar $1*.class
hadoop jar lg.jar $1 /p1/input /p1/output
hadoop fs -get /p1/output
less ./output/part*
