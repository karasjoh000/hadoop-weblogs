1. put access_log file into this directory

2. create a input directory in the hdfs:
hadoop fs -mkdir /p1
hadoop fs -mkdir /p1/input

3. put the access_log into input dir:
hadoop fs -put access_log /p1/input

4. now run either:
    "./hd.sh CountFileHits" for question 1,
    "./hd.sh CountIpHits" for question 2,
 or "./hd.sh MaxhitFile" for question 3


 CountFileHits.java is for question 1
 CountIpHits.java is for question 2
 MaxHitFile is for question 3

 answers.txt contains the answers to the 3 questions.


