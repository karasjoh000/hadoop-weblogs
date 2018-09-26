1. Put this directory into cloudera VM. 

2. put access_log file into this directory

3. create a input directory in the hdfs:
hadoop fs -mkdir /p1
hadoop fs -mkdir /p1/input

4. put the access_log into input dir:
hadoop fs -put access_log /p1/input

5. now run either:
    "./hd.sh CountFileHits" for question 1,
    "./hd.sh CountIpHits" for question 2,
 or "./hd.sh MaxhitFile" for question 3

 the output directory will be put into this folder as "output" after the job is done

 CountFileHits.java is for question 1
 CountIpHits.java is for question 2
 MaxHitFile is for question 3

 answers.txt as well as Homework2JohnKarasev.pdf contains the answers to the 3 questions.


