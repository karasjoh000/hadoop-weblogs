# Web Log Stats using Hadoop. 
--------
### Directions: 
1. Put `src` dir into cloudera VM.  
2. put access_log file into `src` directory  
2. create a input directory in the hdfs:
`hadoop fs -mkdir /p1`
`hadoop fs -mkdir /p1/input`  
4. put the access_log into input dir:
`hadoop fs -put access_log /p1/input`  
5. now run either:
    `./hd.sh CountFileHits` for question 1,
    `./hd.sh CountIpHits` for question 2,
 or `./hd.sh MaxhitFile` for question 3

 the output directory will be put into this folder as "output" after the job is done

 `CountFileHits.java` is for question 1  
 `CountIpHits.java` is for question 2  
 `MaxHitFile` is for question 3  

 see assignment details for the description of each calculation. 